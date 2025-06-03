from google.cloud import speech_v1
from google.cloud.speech_v1 import types
import os
import requests
import tempfile
from typing import Dict, Optional, Tuple
import logging
from services.logger_config import app_logger, log_operation
import wave

META_API_URL = "https://graph.facebook.com/v19.0/"  # Correct base URL for Meta API

class VoiceService:
    def __init__(self):
        self.client = speech_v1.SpeechClient()
        self.supported_formats = {
            'audio/ogg': speech_v1.RecognitionConfig.AudioEncoding.OGG_OPUS,
            'audio/opus': speech_v1.RecognitionConfig.AudioEncoding.OGG_OPUS,
            'audio/ogg; codecs=opus': speech_v1.RecognitionConfig.AudioEncoding.OGG_OPUS,
            'audio/mpeg': speech_v1.RecognitionConfig.AudioEncoding.MP3,
            'audio/wav': speech_v1.RecognitionConfig.AudioEncoding.LINEAR16,
            'audio/x-wav': speech_v1.RecognitionConfig.AudioEncoding.LINEAR16
        }
        
    def download_voice_note(self, media_url: str, access_token: str) -> Tuple[str, str]:
        """
        Download voice note from WhatsApp servers.
        Returns tuple of (file_path, mime_type)
        """
        try:
            headers = {
                'Authorization': f'Bearer {access_token}'
            }
            
            # Log detailed request information
            log_operation(app_logger, "Fetching media URL", f"Requesting: {media_url}")
            app_logger.debug(f"Request headers: {headers}")
            
            # Adjust metadata_url and media_id as per requirements
            media_id = media_url.split('/')[-1]
            response = requests.get(f"https://graph.facebook.com/v19.0/{media_id}", headers=headers)
                        
            if response.status_code == 400:
                error_details = response.json() if response.headers.get('Content-Type') == 'application/json' else response.text
                error_msg = f"400 Bad Request: {error_details}"
                log_operation(app_logger, "Media URL error", error_msg)
                app_logger.error(f"Failed to fetch media URL: {media_url}")
                app_logger.error(f"Response details: {error_details}")
                
                # Check for encoding issues in media_id
                if "\\/" in str(media_url):
                    app_logger.error("Detected encoding issue in media URL. Ensure media_id is correctly formatted.")
                
                raise ValueError(f"Failed to fetch media URL. Error: {error_details}")
            
            response.raise_for_status()
            
            # Extract the actual media URL from the response
            media_data = response.json()
            log_operation(app_logger, "Media URL response", f"Response: {media_data}")
            
            if 'url' not in media_data:
                error_msg = f"Media URL not found in response: {media_data}"
                log_operation(app_logger, "Media URL error", error_msg)
                app_logger.error(error_msg)
                raise ValueError(error_msg)
                
            actual_url = media_data['url']
            log_operation(app_logger, "Got media URL", f"URL: {actual_url}")
            
            # Download the actual media file
            log_operation(app_logger, "Downloading media", f"From URL: {actual_url}")
            media_response = requests.get(actual_url, headers=headers, stream=True)
            media_response.raise_for_status()
            
            # Get content type and verify it's supported
            content_type = media_response.headers.get('content-type', 'audio/ogg')
            # Normalize mime type for WhatsApp's OGG/OPUS format
            if content_type == 'audio/ogg; codecs=opus':
                mime_type = 'audio/ogg'
            else:
                mime_type = content_type

            log_operation(app_logger, "Content type", 
                         f"Raw: {content_type}, Normalized: {mime_type}")
                
            if mime_type not in self.supported_formats:
                error_msg = f"Unsupported mime type: {mime_type} (raw: {content_type})"
                log_operation(app_logger, "Mime type error", error_msg)
                app_logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Create temporary file with appropriate extension
            extension = 'ogg' if mime_type == 'audio/ogg' else mime_type.split('/')[-1]
            
            # Create temp file and stream the content
            fd, temp_path = tempfile.mkstemp(suffix=f'.{extension}')
            content_size = 0
            
            with os.fdopen(fd, 'wb') as temp_file:
                for chunk in media_response.iter_content(chunk_size=8192):
                    if chunk:
                        content_size += len(chunk)
                        temp_file.write(chunk)
            
            if content_size == 0:
                error_msg = "Downloaded file is empty"
                log_operation(app_logger, "Download error", error_msg)
                app_logger.error(error_msg)
                raise ValueError(error_msg)
                
            log_operation(app_logger, "Voice note downloaded", 
                         f"Path: {temp_path}, Type: {mime_type}, Size: {content_size} bytes")
            
            return temp_path, mime_type
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Network error downloading voice note: {str(e)}"
            log_operation(app_logger, "Download network error", error_msg)
            app_logger.error(error_msg)
            raise ValueError(error_msg)
        except ValueError as e:
            raise
        except Exception as e:
            error_msg = f"Unexpected error downloading voice note: {str(e)}"
            log_operation(app_logger, "Download error", error_msg)
            app_logger.error(error_msg)
            raise ValueError(error_msg)

    def _get_wav_info(self, file_path: str) -> Dict:
        """Get WAV file information."""
        with wave.open(file_path, 'rb') as wav_file:
            return {
                'channels': wav_file.getnchannels(),
                'sample_width': wav_file.getsampwidth(),
                'frame_rate': wav_file.getframerate(),
                'frames': wav_file.getnframes(),
                'duration': wav_file.getnframes() / wav_file.getframerate()
            }
    
    def transcribe_audio(self, file_path: str, mime_type: str, 
                        language_code: str = 'en-US', max_retries: int = 2) -> Dict[str, str]:
        """
        Transcribe audio file using Google Cloud Speech-to-Text.
        Includes retry logic for failed transcriptions.
        """
        last_error = None
        
        for attempt in range(max_retries + 1):
            try:
                # Verify file exists and has content
                if not os.path.exists(file_path):
                    raise ValueError(f"Audio file not found: {file_path}")
                    
                file_size = os.path.getsize(file_path)
                if file_size == 0:
                    raise ValueError("Audio file is empty")
                    
                log_operation(app_logger, "Starting transcription", 
                             f"Attempt {attempt + 1}/{max_retries + 1}, "
                             f"File: {file_path}, Size: {file_size} bytes, Type: {mime_type}")
                
                # Read the audio file
                with open(file_path, 'rb') as audio_file:
                    content = audio_file.read()
                
                # Get encoding type based on mime_type
                encoding = self.supported_formats.get(mime_type)
                if not encoding:
                    raise ValueError(f"Unsupported audio format: {mime_type}")
                
                # Configure audio and recognition settings
                audio = types.RecognitionAudio(content=content)
                
                # Adjust settings based on retry attempt
                if attempt == 0:
                    # First attempt: Optimized for voice messages
                    config = types.RecognitionConfig(
                        encoding=encoding,
                        sample_rate_hertz=16000,  # WhatsApp voice messages are typically 16kHz
                        language_code=language_code,
                        enable_automatic_punctuation=True,
                        model='phone_call',  # Optimized for phone audio
                        use_enhanced=True,  # Use enhanced model
                        audio_channel_count=1,  # Mono audio
                        enable_word_confidence=True,  # Get per-word confidence
                        max_alternatives=1
                    )
                else:
                    # Retry attempt: Use more aggressive settings
                    config = types.RecognitionConfig(
                        encoding=encoding,
                        sample_rate_hertz=16000,
                        language_code=language_code,
                        enable_automatic_punctuation=True,
                        model='default',  # Try default model instead
                        use_enhanced=True,
                        audio_channel_count=1,
                        enable_word_confidence=True,
                        max_alternatives=3,  # Try to get more alternatives
                        speech_contexts=[{  # Add common speech contexts
                            'phrases': ['hello', 'hi', 'hey', 'yes', 'no', 'thanks', 'please', 
                                      'okay', 'ok', 'thank you', 'bye', 'goodbye'],
                            'boost': 20.0
                        }]
                    )
                
                log_operation(app_logger, "Sending to Google Speech-to-Text", 
                             f"Attempt {attempt + 1}, Config: encoding={encoding}, lang={language_code}, "
                             f"model={config.model}")
                
                # Perform the transcription
                try:
                    response = self.client.recognize(config=config, audio=audio)
                    log_operation(app_logger, "Got transcription response", 
                                 f"Results: {len(response.results) if response.results else 0}")
                    
                    # Process results
                    if not response.results:
                        last_error = "No transcription results returned"
                        continue  # Try next attempt
                        
                    result = response.results[0]
                    if not result.alternatives:
                        last_error = "No transcription alternatives returned"
                        continue  # Try next attempt
                    
                    # Try each alternative until we find non-empty transcript
                    for alternative in result.alternatives:
                        transcript = alternative.transcript.strip()
                        confidence = alternative.confidence
                        
                        if transcript:
                            confidence_display = f"{confidence * 100:.2f}%" if confidence is not None else "N/A"
                            log_operation(app_logger, "Voice note transcribed", 
                                         f"Text: '{transcript}', Confidence: {confidence_display}")
                            
                            if os.path.exists(file_path):
                                os.remove(file_path)
                            
                            return {
                                "status": "success",
                                "message": f"Transcription successful with {confidence_display} confidence.",
                                "transcript": transcript,
                                "confidence": confidence,
                                "confidence_percent": confidence_display
                            }
                    
                    last_error = "Empty transcript returned"
                    continue  # Try next attempt
                
                except Exception as e:
                    last_error = f"Google API error: {str(e)}"
                    log_operation(app_logger, "Google API error", last_error)
                    app_logger.error(last_error)
                    continue  # Try next attempt
                
            except Exception as e:
                last_error = f"Transcription error: {str(e)}"
                log_operation(app_logger, "Transcription error", 
                             f"File: {file_path}, Error: {last_error}")
                app_logger.error(last_error)
                break  # Stop retrying on non-API errors
        
        # If we get here, all attempts failed
        return {
            "status": "error",
            "error": last_error or "Failed to transcribe after all attempts"
        }

    def detect_language(self, audio_file: str, mime_type: str) -> Optional[str]:
        """
        Detect spoken language in audio file.
        Returns language code or None if detection fails.
        """
        try:
            # Read the audio file
            with open(audio_file, 'rb') as audio:
                content = audio.read()
            
            # Get encoding type based on mime_type
            encoding = self.supported_formats.get(mime_type)
            if not encoding:
                raise ValueError(f"Unsupported audio format: {mime_type}")
            
            # Get sample rate for WAV files
            sample_rate_hertz = 16000
            if mime_type in ['audio/wav', 'audio/x-wav']:
                wav_info = self._get_wav_info(audio_file)
                sample_rate_hertz = wav_info['frame_rate']
            
            # Configure audio and recognition settings
            audio = types.RecognitionAudio(content=content)
            config = types.RecognitionConfig(
                encoding=encoding,
                sample_rate_hertz=sample_rate_hertz,
                enable_language_identification=True,
                model='phone_call'
            )
            
            # Perform the recognition
            response = self.client.recognize(config=config, audio=audio)
            
            # Get detected language
            if response.results and response.results[0].language_code:
                return response.results[0].language_code
            
            return None
            
        except Exception as e:
            log_operation(app_logger, "Language detection error", 
                         f"File: {audio_file}, Error: {str(e)}")
            app_logger.error(str(e))
            return None

# Create global instance
voice_service = VoiceService()