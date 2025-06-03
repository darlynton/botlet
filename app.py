import os
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import services.ai_engine
import services.db_services
from vertexai.generative_models import Content, Part
from services.reminder_service import reminder_service
from services.whatsapp_service import WhatsAppService
from services.message_queue import MessageQueue
import atexit
from collections import OrderedDict
import time
import threading
from services.rate_limiter import rate_limiter
import logging
from datetime import datetime
import json
import traceback
import requests
import sys

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def initialize_services():
    """Initialize all required services with proper error handling"""
    try:
        # Initialize logging first
        logger.info("Setting up logging...")
        os.makedirs('logs', exist_ok=True)
        
        # Initialize database and its directory
        logger.info("Initializing database...")
        db_dir = os.path.dirname(os.path.abspath("bot_data.db"))
        os.makedirs(db_dir, exist_ok=True)
        services.db_services.init_db()
        
        # Initialize connection pool
        logger.info("Initializing connection pool...")
        services.db_services.ConnectionPool.initialize()
        
        # Initialize WhatsApp service
        logger.info("Initializing WhatsApp service...")
        whatsapp_service = services.whatsapp_service.WhatsAppService()
        
        # Initialize message queue and lock for normal operation
        logger.info("Initializing message queue...")
        queue = MessageQueue()
        lock_path = os.path.join(db_dir, ".bot_data_queue.lock")
        if os.path.exists(lock_path):
            try:
                os.unlink(lock_path)
                logger.info("Cleaned up stale lock file")
            except OSError as e:
                logger.warning(f"Could not remove stale lock: {e}")
        queue._initialized = False  # Force reinitialization
        queue.__init__()
        logger.info("Message queue initialized successfully")
        
        # Start reminder service last since it depends on other services
        logger.info("Starting reminder service...")
        reminder_service.start()
        return True
    except Exception as e:
        logger.error("Failed to initialize services")
        logger.error(f"Error: {str(e)}")
        logger.error(traceback.format_exc())
        return False

app = Flask(__name__)

# Initialize all services before starting the app
if not initialize_services():
    logger.error("Service initialization failed. Exiting...")
    sys.exit(1)

# Dictionary to store conversation history for each user (in-memory cache)
# Key: WhatsApp user ID (sender_id)
# Value: List of vertexai.generative_models.Content objects
# Note: Primary history persistence is in db_services.
conversation_histories = {} 

# Message ID tracking with LRU-style cache
# Keep track of processed message IDs for 1 hour to prevent duplicates
MAX_CACHE_SIZE = 1000
MESSAGE_EXPIRY = 3600  # 1 hour in seconds
processed_messages = OrderedDict()

def clean_old_messages():
    """Remove message IDs older than MESSAGE_EXPIRY seconds"""
    current_time = time.time()
    # Convert to list to avoid runtime modification issues
    for msg_id, timestamp in list(processed_messages.items()):
        if current_time - timestamp > MESSAGE_EXPIRY:
            processed_messages.pop(msg_id, None)
        else:
            # Since OrderedDict is time-ordered, once we hit a message that's not expired,
            # all subsequent messages are also not expired
            break

def cleanup_old_data():
    """Periodic cleanup of old data"""
    try:
        # Clean up old messages from memory
        clean_old_messages()
        
        # Clean up old sessions (older than 30 days)
        result = services.db_services.clear_old_sessions(days_old=30) # Call from db_services
        if result["status"] == "success":
            logger.info(f"Session cleanup: {result['message']}") # Changed print to logger.info
        else:
            logger.error(f"Session cleanup failed: {result['message']}") # Changed print to logger.error
            
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}") # Changed print to logger.error

# Schedule periodic cleanup every 24 hours
def schedule_cleanup():
    """Schedule the cleanup task"""
    def run_cleanup():
        while True:
            cleanup_old_data()
            time.sleep(86400)  # 24 hours
    
    cleanup_thread = threading.Thread(target=run_cleanup, daemon=True)
    cleanup_thread.start()

# Start the cleanup scheduler
schedule_cleanup()

# Register cleanup on app shutdown
atexit.register(cleanup_old_data)

def is_message_processed(msg_id: str) -> bool:
    """Check if a message ID has been processed and update its timestamp if it has"""
    current_time = time.time()
    
    # Clean old messages periodically
    if len(processed_messages) > MAX_CACHE_SIZE:
        clean_old_messages()
    
    # If message was processed recently, update its timestamp and return True
    if msg_id in processed_messages:
        processed_messages.move_to_end(msg_id)
        processed_messages[msg_id] = current_time
        return True
    
    # New message, add it to cache
    processed_messages[msg_id] = current_time
    if len(processed_messages) > MAX_CACHE_SIZE:
        # Remove oldest message if cache is full
        processed_messages.popitem(last=False)
    return False

# Constants
META_API_URL = "https://graph.facebook.com/v19.0/"
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
VERIFY_TOKEN = os.getenv("WEBHOOK_VERIFY_TOKEN", "default_verify_token_for_dev")  # Get from env or use default for dev

# Management endpoints for authorized numbers
@app.route('/api/authorized-numbers', methods=['GET', 'POST', 'DELETE'])
def manage_authorized_numbers():
    # Check admin token
    admin_token = request.headers.get('X-Admin-Token')
    if admin_token != os.getenv('ADMIN_TOKEN'):
        return jsonify({"error": "Unauthorized"}), 401

    if request.method == 'GET':
        numbers = services.db_services.list_authorized_numbers() # Call from db_services
        return jsonify({"authorized_numbers": numbers})

    elif request.method == 'POST':
        data = request.get_json()
        if not data or not data.get('phone_number') or not data.get('name'):
            return jsonify({"error": "Missing required fields"}), 400

        result = services.db_services.add_authorized_number( # Call from db_services
            phone_number=data['phone_number'],
            name=data['name'],
            added_by=data.get('added_by', 'admin')
        )
        
        if result['status'] == 'success':
            return jsonify(result), 201
        else:
            return jsonify(result), 400

    elif request.method == 'DELETE':
        data = request.get_json()
        if not data or not data.get('phone_number'):
            return jsonify({"error": "Missing phone number"}), 400

        result = services.db_services.remove_authorized_number(data['phone_number']) # Call from db_services
        
        if result['status'] == 'success':
            return jsonify(result), 200
        else:
            return jsonify(result), 404

@app.route('/webhook', methods=['GET', 'POST'])
def webhook():
    """Handle incoming webhook requests from WhatsApp"""
    logger.info("----------------------------------------")
    logger.info(f"Webhook called with method: {request.method}")
    logger.info(f"Request headers: {dict(request.headers)}")
    logger.info(f"Query parameters: {dict(request.args)}")
    
    VERIFY_TOKEN = os.getenv('WHATSAPP_VERIFY_TOKEN') or os.getenv('VERIFY_TOKEN') or 'testtoken'
    if request.method == 'GET':
        # Handle verification challenge 
        mode = request.args.get('hub.mode')
        token = request.args.get('hub.verify_token')
        challenge = request.args.get('hub.challenge')
        logger.info(f"Webhook verification attempt - Mode: {mode}, Token: {token}, Challenge: {challenge}")
        logger.info(f"Expected verify token: {VERIFY_TOKEN}")

        if mode and token:
            if mode == 'subscribe' and token == VERIFY_TOKEN:
                logger.info("Webhook verification successful!")
                return challenge, 200
            logger.warning("Webhook verification failed - token mismatch")
            return 'Forbidden', 403
        logger.warning("Webhook verification failed - missing mode or token")
        return 'Bad Request', 400

    # Process POST requests (actual webhook messages)
    start_time = time.time()
    timeout = 25  # Total timeout budget in seconds
    try:
        # Log raw request data for debugging
        raw_data = request.get_data()
        logger.info(f"Raw request data: {raw_data.decode('utf-8')}")
        
        # Parse JSON payload
        try:
            data = request.get_json()
            logger.info(f"Parsed webhook data: {json.dumps(data, indent=2)}")
        except Exception as e:
            logger.error(f"Failed to parse webhook JSON: {str(e)}")
            return jsonify({'error': 'Invalid JSON payload'}), 400

        if not data:
            logger.error("Empty webhook payload received")
            return jsonify({'error': 'Empty payload'}), 400

        logger.info(f"Webhook data received: {json.dumps(data)}")

        # Basic validation
        if not isinstance(data, dict) or 'entry' not in data:
            logger.error("Invalid webhook data structure - missing 'entry'")
            return jsonify({'error': 'Invalid data structure'}), 400

        try:
            # Extract message data
            entry = data['entry'][0]
            changes = entry['changes'][0]
            value = changes.get('value', {})

            # Handle status updates
            if 'statuses' in value:
                status_data = value['statuses'][0]
                logger.info(f"Status update received: {status_data.get('status')}")
                return jsonify({'status': 'processed'}), 200

            # Handle messages
            messages = value.get('messages', [])
            if not messages:
                logger.info("No messages in webhook data")
                return jsonify({'status': 'no messages'}), 200

            # Get message details
            message = messages[0]
            message_id = message.get('id')
            message_type = message.get('type', 'unknown')
            from_number = message.get('from')

            # Validate required fields
            if not all([message_id, message_type, from_number]):
                logger.error(f"Missing required message fields: id={message_id}, type={message_type}, from={from_number}")
                return jsonify({'error': 'Invalid message format'}), 400

            logger.info(f"Processing message from {from_number} (ID: {message_id}, type: {message_type})")
            
            # Check rate limit
            user_id = from_number  # Assuming user_id is the sender's phone number
            allowed, reason, unblock_time = rate_limiter.check_rate_limit(user_id)
            if not allowed:
                logger.warning(f"Rate limit exceeded for user {user_id}. Message ID: {message_id}. Reason: {reason}")
                return jsonify({'error': reason or 'Rate limit exceeded, please try again later.'}), 429
            
            # Mark message as processed (for deduplication)
            if is_message_processed(message_id):
                logger.info(f"Duplicate message detected and ignored (ID: {message_id})")
                return jsonify({'status': 'duplicate'}), 200

            # Restore message queue usage for actual processing
            message_queue = MessageQueue()
            if message_queue.is_duplicate_webhook(message_id):
                logger.info(f"Duplicate message detected: {message_id}")
                return jsonify({'status': 'duplicate'}), 200

            # Check authorization
            if not services.db_services.is_number_authorized(from_number):
                logger.warning(f"Unauthorized number: {from_number}")
                try:
                    message_queue.enqueue_message(
                        user_id=from_number,
                        message="Sorry, this number is not authorized to use this service.",
                        metadata={"type": "unauthorized_response"}
                    )
                except Exception as e:
                    logger.error(f"Failed to enqueue unauthorized response: {e}")
                return jsonify({'error': 'Unauthorized number'}), 403

            # Process message based on type
            if message_type == 'text':
                text = message['text']['body']
                try:
                    message_queue.enqueue_message(
                        user_id=from_number,
                        message=text,
                        metadata={
                            "message_id": message_id,
                            "type": "text"
                        }
                    )
                except Exception as e:
                    logger.error(f"Failed to enqueue text message: {e}")
            elif message_type in ['image', 'document', 'audio', 'video']:
                media_id = message.get(message_type, {}).get('id')
                if not media_id:
                    logger.error(f"Missing media ID for {message_type} message")
                    return jsonify({'error': f'Invalid {message_type} message'}), 400
                try:
                    message_queue.enqueue_message(
                        user_id=from_number,
                        message=f"{message_type}:{media_id}",
                        metadata={
                            "message_id": message_id,
                            "type": message_type,
                            "media_id": media_id
                        }
                    )
                except Exception as e:
                    logger.error(f"Failed to enqueue media message: {e}")
            else:
                logger.warning(f"Unsupported message type: {message_type}")
                try:
                    message_queue.enqueue_message(
                        user_id=from_number,
                        message=f"Sorry, {message_type} messages are not supported yet.",
                        metadata={
                            "message_id": message_id,
                            "type": "unsupported_response"
                        }
                    )
                except Exception as e:
                    logger.error(f"Failed to enqueue unsupported message type: {e}")
            logger.info(f"Successfully queued message from {from_number} (ID: {message_id})")
            return jsonify({'status': 'queued'}), 200
        except (KeyError, IndexError) as e:
            logger.error(f"Error extracting webhook data: {str(e)}", exc_info=True)
            return jsonify({'error': 'Invalid webhook format'}), 400
    except Exception as e:
        logger.error(f"Unhandled webhook error: {str(e)}", exc_info=True)
        logger.error(traceback.format_exc())
        # Always return 200 for webhook POST, even on error
        return jsonify({'status': 'error', 'detail': str(e)}), 200

# Add endpoint to check user's rate limit status
@app.route('/api/rate-limit-status/<user_id>', methods=['GET'])
def get_rate_limit_status(user_id):
    # Check admin token
    admin_token = request.headers.get('X-Admin-Token')
    if admin_token != os.getenv('ADMIN_TOKEN'):
        return jsonify({"error": "Unauthorized"}), 401
        
    stats = rate_limiter.get_user_stats(user_id)
    return jsonify(stats)

if __name__ == '__main__':
    # Ensure GOOGLE_APPLICATION_CREDENTIALS is set for local testing
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        service_account_path = os.path.join(os.path.dirname(__file__), "service_account_key.json")
        if os.path.exists(service_account_path):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path
            logger.info(f"Set GOOGLE_APPLICATION_CREDENTIALS to {service_account_path}")
        else:
            logger.warning("WARNING: GOOGLE_APPLICATION_CREDENTIALS not set and service_account_key.json not found in current directory.")
            logger.warning("AI Engine might not authenticate correctly.")
            
app.run(debug=True, host="0.0.0.0", port=7001)