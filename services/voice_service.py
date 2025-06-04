import os
import tempfile
import requests
import subprocess
import logging
from google.cloud import speech_v1
from google.cloud.speech_v1 import types
from services.whatsapp_service import send_whatsapp_message

logger = logging.getLogger("voice_note_service")
logging.basicConfig(level=logging.INFO)

class VoiceNoteService:
    def __init__(self, access_token=None):
        self.access_token = access_token or os.getenv("META_ACCESS_TOKEN")
        if not self.access_token:
            raise ValueError("META_ACCESS_TOKEN is required for VoiceNoteService")
        self.speech_client = speech_v1.SpeechClient()

    def download_voice_note(self, media_url):
        logger.info(f"[VoiceNoteService] Downloading media_url={media_url}")
        headers = {'Authorization': f'Bearer {self.access_token}'}
        media_id = media_url.split('/')[-1]
        meta_url = f"https://graph.facebook.com/v19.0/{media_id}"
        response = requests.get(meta_url, headers=headers)
        logger.info(f"[VoiceNoteService] Meta API response: status={response.status_code}, body={response.text}")
        response.raise_for_status()
        media_data = response.json()
        if 'url' not in media_data:
            logger.error(f"[VoiceNoteService] No 'url' in response: {media_data}")
            raise ValueError(f"Media URL not found in response: {media_data}")
        actual_url = media_data['url']
        logger.info(f"[VoiceNoteService] Got media URL: {actual_url}")
        media_response = requests.get(actual_url, headers=headers, stream=True)
        logger.info(f"[VoiceNoteService] Media download response: status={media_response.status_code}")
        media_response.raise_for_status()
        content_type = media_response.headers.get('content-type', 'audio/ogg')
        extension = 'ogg' if 'ogg' in content_type else content_type.split('/')[-1]
        fd, temp_path = tempfile.mkstemp(suffix=f'.{extension}')
        size = 0
        with os.fdopen(fd, 'wb') as temp_file:
            for chunk in media_response.iter_content(chunk_size=8192):
                if chunk:
                    size += len(chunk)
                    temp_file.write(chunk)
        logger.info(f"[VoiceNoteService] Voice note downloaded: path={temp_path}, type={content_type}, size={size}")
        return temp_path, content_type

    def reencode_to_ogg_opus(self, input_path):
        fd, output_path = tempfile.mkstemp(suffix='.ogg')
        os.close(fd)
        cmd = [
            'ffmpeg', '-y', '-i', input_path,
            '-ac', '1', '-ar', '16000', '-c:a', 'libopus', output_path
        ]
        logger.info(f"[VoiceNoteService] Re-encoding: cmd={' '.join(cmd)}")
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            logger.info(f"[VoiceNoteService] Re-encoding success: output={output_path}")
        except subprocess.CalledProcessError as e:
            logger.error(f"[VoiceNoteService] Re-encoding failed: {e.stderr.decode()}")
            raise
        return output_path

    def transcribe_audio(self, file_path, language_code='en-US'):
        logger.info(f"[VoiceNoteService] Transcription start: file={file_path}")
        encoding = speech_v1.RecognitionConfig.AudioEncoding.OGG_OPUS
        with open(file_path, 'rb') as audio_file:
            content = audio_file.read()
        audio = types.RecognitionAudio(content=content)
        config = types.RecognitionConfig(
            encoding=encoding,
            sample_rate_hertz=16000,
            language_code=language_code,
            enable_automatic_punctuation=True,
            model='default',
            use_enhanced=True,
            audio_channel_count=1,
            enable_word_confidence=True,
            max_alternatives=3
        )
        logger.info(f"[VoiceNoteService] Google STT config: encoding={encoding}, lang={language_code}, model=default, sample_rate=16000, channels=1")
        try:
            response = self.speech_client.recognize(config=config, audio=audio)
            logger.info(f"[VoiceNoteService] Google STT response: results={len(response.results) if response.results else 0}")
            if not response.results or not response.results[0].alternatives:
                logger.warning(f"[VoiceNoteService] Transcription empty: file={file_path}")
                return None, None
            alt = response.results[0].alternatives[0]
            transcript = alt.transcript.strip()
            confidence = alt.confidence
            logger.info(f"[VoiceNoteService] Transcription: transcript='{transcript}', confidence={confidence}")
            return transcript, confidence
        except Exception as e:
            logger.error(f"[VoiceNoteService] Transcription error: {e}")
            return None, None

    def handle_voice_note(self, media_url, to_number, language_code='en-US'):
        temp_path, content_type = self.download_voice_note(media_url)
        reencoded_path = None
        try:
            transcript, confidence = self.transcribe_audio(temp_path, language_code)
            if not transcript:
                logger.info("[VoiceNoteService] Transcript fallback: Trying re-encode and re-transcribe")
                reencoded_path = self.reencode_to_ogg_opus(temp_path)
                transcript, confidence = self.transcribe_audio(reencoded_path, language_code)
            if transcript:
                logger.info(f"[VoiceNoteService] Transcript success: {transcript}")
                # conf_percent = f"{round(confidence * 100)}%" if confidence is not None else "N/A"
                # message = f'I heard you say "{transcript}"\n\n(Confidence score {conf_percent})'
                # send_whatsapp_message(to_number, message)
                return transcript, confidence
            else:
                logger.warning("[VoiceNoteService] Transcript failed: No transcript after all attempts")
                send_whatsapp_message(to_number, "Sorry, I couldn't understand the voice note.")
                return None, None
        except Exception as e:
            logger.error(f"[VoiceNoteService] Voice note transcription error: {e}")
            send_whatsapp_message(to_number, f"Sorry, an error occurred transcribing the voice note: {e}")
            return None, None
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
                logger.info(f"[VoiceNoteService] Cleanup: Removed {temp_path}")
            if reencoded_path and os.path.exists(reencoded_path):
                os.remove(reencoded_path)
                logger.info(f"[VoiceNoteService] Cleanup: Removed {reencoded_path}")
