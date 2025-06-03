import logging
import time
from logging.handlers import RotatingFileHandler
import os
from datetime import datetime

# Create logs directory if it doesn't exist
LOGS_DIR = "logs"
if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

# Configure root logger
logging.basicConfig(level=logging.INFO)

# Create formatters
standard_formatter = logging.Formatter(
    '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Create file handlers
app_handler = RotatingFileHandler(
    os.path.join(LOGS_DIR, 'app.log'),
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5
)
app_handler.setFormatter(standard_formatter)
app_handler.setLevel(logging.INFO)

queue_handler = RotatingFileHandler(
    os.path.join(LOGS_DIR, 'queue.log'),
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5
)
queue_handler.setFormatter(standard_formatter)
queue_handler.setLevel(logging.INFO)

whatsapp_handler = RotatingFileHandler(
    os.path.join(LOGS_DIR, 'whatsapp.log'),
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5
)
whatsapp_handler.setFormatter(standard_formatter)
whatsapp_handler.setLevel(logging.INFO)

message_queue_handler = RotatingFileHandler(
    os.path.join(LOGS_DIR, 'message_queue.log'),
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5
)
message_queue_handler.setFormatter(standard_formatter)
message_queue_handler.setLevel(logging.INFO)

# Create loggers
app_logger = logging.getLogger('app')
app_logger.addHandler(app_handler)
app_logger.setLevel(logging.INFO)

queue_logger = logging.getLogger('queue')
queue_logger.addHandler(queue_handler)
queue_logger.setLevel(logging.INFO)

whatsapp_logger = logging.getLogger('whatsapp')
whatsapp_logger.addHandler(whatsapp_handler)
whatsapp_logger.setLevel(logging.INFO)
whatsapp_logger.propagate = False

message_queue_logger = logging.getLogger('services.message_queue')
message_queue_logger.addHandler(message_queue_handler)
message_queue_logger.setLevel(logging.INFO)
message_queue_logger.propagate = False

# Throttled logging decorator
def throttled_log(interval):
    """Decorator to throttle logging to once per interval seconds."""
    def decorator(func):
        last_log = {}
        def wrapper(*args, **kwargs):
            current_time = time.time()
            if args[0] not in last_log or current_time - last_log[args[0]] >= interval:
                last_log[args[0]] = current_time
                return func(*args, **kwargs)
        return wrapper
    return decorator

@throttled_log(5)  # Only log once every 5 seconds
def log_operation(logger, operation: str, details: str = None):
    """Log an operation with optional details."""
    if details:
        logger.info(f"Operation: {operation} | Details: {details}")
    else:
        logger.info(f"Operation: {operation}")

# Setup main loggers
ai_logger = logging.getLogger('ai')
ai_logger.setLevel(logging.INFO)

reminder_logger = logging.getLogger('reminder')
reminder_logger.setLevel(logging.INFO)

# Export the loggers
__all__ = ['app_logger', 'whatsapp_logger', 'ai_logger', 'queue_logger', 
           'reminder_logger', 'log_operation']