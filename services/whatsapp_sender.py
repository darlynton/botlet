import logging
import requests
from typing import Optional, Dict, Any
import os
from dotenv import load_dotenv
import traceback
import json
import time

# Load environment variables
load_dotenv()

# Configure logging
from services.logger_config import whatsapp_logger as logger

META_API_URL = "https://graph.facebook.com/v19.0/"
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")

class TokenError(Exception):
    """Exception raised for token-related errors."""
    pass

class WhatsAppSender:
    _instance = None
    _initialized = False
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize the WhatsApp sender with API configuration."""
        if not WhatsAppSender._initialized:
            self._token = os.getenv("META_ACCESS_TOKEN")
            self.phone_number_id = os.getenv("PHONE_NUMBER_ID")
            
            if not self.phone_number_id or not self._token:
                raise ValueError("PHONE_NUMBER_ID and META_ACCESS_TOKEN must be configured")
            
            # Construct the API URL using the phone number ID
            self.api_url = f"{META_API_URL}{self.phone_number_id}/messages"
            
            # Update headers
            self._update_headers()
            
            # Initialize retry settings
        self.max_retries = 3
        self.retry_delay = 1  # Start with 1 second delay
        
        logger.info("WhatsApp sender initialized with API URL: %s", self.api_url)
        WhatsAppSender._initialized = True

    def _update_headers(self):
        """Update the headers with the current token."""
        self.headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json"
        }

    def refresh_token(self):
        """Refresh the Meta API token. Should be called by your token management system."""
        new_token = os.getenv("META_ACCESS_TOKEN")  # Re-read from environment
        if new_token and new_token != self._token:
            self._token = new_token
            self._update_headers()
            logger.info("Meta API token refreshed successfully")
            return True
        return False

    def _check_auth_error(self, response) -> bool:
        """Check if the error is due to authentication/token issues."""
        if response.status_code in (401, 403):
            error_data = response.json()
            error_msg = error_data.get('error', {}).get('message', '').lower()
            return any(term in error_msg for term in ['token', 'auth', 'unauthorized'])
        return False

    def _handle_auth_error(self, response):
        """Handle authentication errors by attempting to refresh the token."""
        if self._check_auth_error(response):
            logger.warning("Authentication error detected. Attempting to refresh token.")
            if self.refresh_token():
                return True
            logger.error("Token refresh failed")
            return False

    def send_message(self, user_id: str, message: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Send a message to a WhatsApp user."""
        logger.info(f"Preparing to send WhatsApp message to {user_id}")
        logger.debug(f"Message content: {message[:100]}...")  # Log first 100 chars
        
        try:
            # Validate user_id format
            if not self.is_valid_whatsapp_id(user_id):
                logger.error(f"Invalid WhatsApp ID format: {user_id}")
                return {
                    "success": False,
                    "error": "Invalid WhatsApp ID format",
                    "status_code": 400
                }
            
            # Prepare the message payload
            payload = {
                "messaging_product": "whatsapp",
                "recipient_type": "individual",
                "to": user_id,
                "type": "text",
                "text": {"body": message}
            }
            
            if metadata:
                payload["metadata"] = metadata
            
            # Send with retries
            for attempt in range(self.max_retries):
                try:
                    logger.info("=" * 80)
                    logger.info("SENDING WHATSAPP MESSAGE")
                    logger.info(f"To: {user_id}")
                    logger.info(f"Message: {message[:100]}...")
                    logger.info(f"API URL: {self.api_url}")
                    
                    response = requests.post(
                        self.api_url,
                        headers=self.headers,
                        json=payload,
                        timeout=10  # 10 second timeout
                    )
                    
                    logger.info(f"WhatsApp API response status: {response.status_code}")
                    logger.debug(f"Response content: {response.text[:500]}...")  # Log first 500 chars
                    
                    # If successful, return message ID
                    if response.status_code == 200:
                        response_data = response.json()
                        message_id = response_data.get("messages", [{}])[0].get("id")
                        logger.info(f"Message sent successfully. Message ID: {message_id}")
                        return {
                            "success": True,
                            "message_id": message_id,
                            "response": response_data
                        }
                    
                    # Handle auth errors with token refresh
                    if self._handle_auth_error(response):
                        continue  # Retry with new token
                    
                    # Log error details
                    logger.error(f"Error sending message (attempt {attempt + 1}/{self.max_retries})")
                    logger.error(f"Status code: {response.status_code}")
                    logger.error(f"Response: {response.text}")
                    
                    if attempt < self.max_retries - 1:
                        retry_delay = self.retry_delay * (2 ** attempt)  # Exponential backoff
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    
                except requests.exceptions.RequestException as e:
                    logger.error(f"Request failed (attempt {attempt + 1}/{self.max_retries}): {str(e)}")
                    if attempt < self.max_retries - 1:
                        retry_delay = self.retry_delay * (2 ** attempt)
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
            
            # If we get here, all retries failed
            error_msg = f"Failed to send message after {self.max_retries} attempts"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "status_code": response.status_code if 'response' in locals() else None
            }
            
        except Exception as e:
            error_msg = f"Unexpected error sending message: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "error": error_msg,
                "status_code": None
            }

    def is_valid_whatsapp_id(self, user_id: str) -> bool:
        """Validate WhatsApp ID format."""
        # Basic validation: should be numbers, at least 10 digits
        stripped_id = ''.join(filter(str.isdigit, user_id))
        return len(stripped_id) >= 10

    def _send_single_message(self, to: str, message: str) -> Dict[str, Any]:
        """Send a single message chunk with logging."""
        try:
            payload = {
                "messaging_product": "whatsapp",
                "to": to,
                "type": "text",
                "text": {"body": message}
            }
            
            logger.debug(f"Sending request to WhatsApp API: {self.api_url}")
            logger.debug(f"Request payload: {payload}")
            
            response = requests.post(
                self.api_url,
                headers=self.headers,
                json=payload
            )
            
            logger.info(f"WhatsApp API response status: {response.status_code}")
            logger.debug(f"Response content: {response.text[:500]}...")  # Log first 500 chars
            
            if response.status_code == 200:
                logger.info("Message sent successfully")
                return {'success': True, 'response': response.json()}
            else:
                error_msg = f"WhatsApp API error: {response.status_code} - {response.text}"
                logger.error(error_msg)
                
                if response.status_code == 401:
                    logger.info("Attempting to refresh token due to 401 error")
                    if self.refresh_token():
                        logger.info("Token refreshed, retrying message send")
                        return self._send_single_message(to, message)
                
                return {'success': False, 'error': error_msg}
                
        except requests.exceptions.RequestException as e:
            error_msg = f"Network error sending WhatsApp message: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            return {'success': False, 'error': error_msg}