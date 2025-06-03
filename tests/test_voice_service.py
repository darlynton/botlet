import unittest
import os
from services.voice_service import voice_service
import wave
import numpy as np
import tempfile

class TestVoiceService(unittest.TestCase):
    def create_test_wav(self, duration_seconds=1, frequency=440):
        """Create a test WAV file with a simple sine wave."""
        # Audio parameters
        sample_rate = 16000  # 16kHz
        num_samples = int(duration_seconds * sample_rate)
        
        # Generate time array
        t = np.linspace(0, duration_seconds, num_samples, False)
        
        # Generate audio signal (combination of frequencies for more realistic sound)
        audio_data = (
            np.sin(2 * np.pi * 440 * t) * 0.4 +  # A4 note
            np.sin(2 * np.pi * 880 * t) * 0.3 +  # A5 note
            np.sin(2 * np.pi * 1760 * t) * 0.2   # A6 note
        )
        
        # Apply fade in/out to avoid clicks
        fade_duration = int(0.1 * sample_rate)  # 100ms fade
        fade_in = np.linspace(0, 1, fade_duration)
        fade_out = np.linspace(1, 0, fade_duration)
        audio_data[:fade_duration] *= fade_in
        audio_data[-fade_duration:] *= fade_out
        
        # Normalize to 16-bit range and convert to int16
        audio_data = np.int16(audio_data * 32767)
        
        # Create temporary WAV file
        fd, temp_path = tempfile.mkstemp(suffix='.wav')
        os.close(fd)
        
        with wave.open(temp_path, 'wb') as wav_file:
            wav_file.setnchannels(1)  # Mono
            wav_file.setsampwidth(2)  # 2 bytes per sample (16 bits)
            wav_file.setframerate(sample_rate)
            wav_file.writeframes(audio_data.tobytes())
        
        return temp_path

    def test_speech_to_text_setup(self):
        """Test that Google Cloud Speech-to-Text is properly configured."""
        try:
            # Create a test WAV file (2 seconds duration)
            test_file = self.create_test_wav(duration_seconds=2)
            
            # Verify the WAV file exists and has content
            self.assertTrue(os.path.exists(test_file))
            file_size = os.path.getsize(test_file)
            self.assertGreater(file_size, 0, "Test WAV file is empty")
            
            # Try to transcribe it
            result = voice_service.transcribe_audio(
                test_file,
                'audio/wav',
                'en-US'
            )
            
            # Print detailed result for debugging
            print("\nTranscription Result:")
            for key, value in result.items():
                print(f"{key}: {value}")
            
            # Check if we got a response
            self.assertEqual(result["status"], "success", 
                           f"Transcription failed: {result.get('error', 'Unknown error')}")
            
            print("\nSpeech-to-Text test completed successfully!")
            print("Your Google Cloud setup is working correctly.")
            
        except Exception as e:
            print("\nError testing Speech-to-Text setup:")
            print(f"Error: {str(e)}")
            print("\nPlease check your Google Cloud credentials and setup.")
            raise
        finally:
            # Clean up test file if it exists
            if 'test_file' in locals() and os.path.exists(test_file):
                try:
                    os.remove(test_file)
                except Exception as e:
                    print(f"\nWarning: Could not clean up test file: {e}")

if __name__ == '__main__':
    unittest.main() 