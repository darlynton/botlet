import os
import requests
from dotenv import load_dotenv
from typing import Dict, Any, Optional, List
import logging
from services.whatsapp_sender import WhatsAppSender

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
MAX_MESSAGE_LENGTH = 4000  # Setting slightly below 4096 for safety

class WhatsAppService:
    def __init__(self):
        self.sender = WhatsAppSender()
        self.api_url = self.sender.api_url
        self.headers = self.sender.headers

    def send_message(self, to: str, message: str) -> Dict[str, Any]:
        """Send a message through WhatsApp with proper error handling."""
        return self.sender.send_message(to, message)

    def refresh_token(self) -> bool:
        """Refresh the Meta API token."""
        return self.sender.refresh_token()

# Standalone functions for backward compatibility
def send_whatsapp_message(to: str, message: str, retry_count: int = 3) -> dict:
    """Backward compatible function for sending WhatsApp messages."""
    service = WhatsAppService()
    return service.send_message(to, message)

def split_message(message: str, max_length: int = MAX_MESSAGE_LENGTH) -> List[str]:
    """Split a long message into smaller chunks that fit within WhatsApp's limits."""
    if len(message) <= max_length:
        return [message]
    
    # Try to split on sentence endings
    chunks = []
    current_chunk = ""
    
    # Split into sentences (roughly)
    sentences = message.replace('\n', '. ').split('. ')
    
    for sentence in sentences:
        if len(current_chunk) + len(sentence) + 2 <= max_length:  # +2 for ". "
            current_chunk += sentence + ". "
        else:
            if current_chunk:
                chunks.append(current_chunk.strip())
            current_chunk = sentence + ". "
    
    if current_chunk:
        chunks.append(current_chunk.strip())
    
    # If any chunk is still too long, split it on words
    final_chunks = []
    for chunk in chunks:
        if len(chunk) > max_length:
            words = chunk.split()
            current_chunk = ""
            for word in words:
                if len(current_chunk) + len(word) + 1 <= max_length:
                    current_chunk += word + " "
                else:
                    if current_chunk:
                        final_chunks.append(current_chunk.strip())
                    current_chunk = word + " "
            if current_chunk:
                final_chunks.append(current_chunk.strip())
        else:
            final_chunks.append(chunk)
    
    return final_chunks