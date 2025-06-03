import os
import threading
import time
import json
import logging
import sqlite3
import traceback
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from enum import Enum
import hashlib
from services.file_lock import FileLock
from services.whatsapp_service import WhatsAppService
from services.logger_config import queue_logger, log_operation
from services.db_services import ConnectionPool

logger = logging.getLogger(__name__)

class MessageStatus(Enum):
    PENDING = 'pending'
    IN_PROGRESS = 'in_progress'
    COMPLETED = 'completed'
    FAILED = 'failed'
    CANCELLED = 'cancelled'

class MessageQueue:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(MessageQueue, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self, db_path: str = "bot_data.db", max_retries: int = 3):
        if self._initialized:
            return
        with self._lock:
            try:
                logger.info("[MessageQueue] __init__ starting initialization")
                self.db_path = db_path
                self.max_retries = max_retries
                self.whatsapp = WhatsAppService()
                
                # Initialize instance variables
                self._pending_messages = {}
                self._processed_messages = set()
                self._webhook_messages = {}  # Changed to dict to store timestamps
                self._message_lock = threading.Lock()
                
                # Rate limiting settings
                self.GLOBAL_RATE_LIMIT = 25  # messages per second
                self.USER_RATE_LIMIT = 2     # messages per second per user
                self.RATE_WINDOW = 5         # seconds to track rate limiting
                self.DUPLICATE_WINDOW = 60   # seconds to track duplicates
                self._message_timestamps = []
                self._user_message_counts = {}
                
                # Retry intervals (exponential backoff)
                self.retry_intervals = [30, 60, 300, 900]  # 30s, 1m, 5m, 15m
                
                # Use /tmp for lock file to avoid path issues
                lock_path = "/tmp/botlet_queue.lock"
                self._processor_lock = FileLock(lock_path)
                
                # Ensure database tables exist
                self._ensure_tables()
                
                logger.info("[MessageQueue] Initialization complete, starting queue processor...")
                # Start the queue processor
                self._start_queue_processor()
                logger.info("[MessageQueue] Queue processor start requested")
                self._initialized = True
                
            except Exception as e:
                logger.error(f"Error initializing MessageQueue: {str(e)}")
                logger.error(traceback.format_exc())
                raise

    def _start_queue_processor(self):
        """Starts the queue processor in a background thread with improved lock handling."""
        logger.info("[MessageQueue] _start_queue_processor called")
        try:
            # Try to acquire the lock with a reasonable timeout
            logger.info("[MessageQueue] Attempting to acquire queue processor lock...")
            if self._processor_lock.acquire(blocking=True, force=True, max_age=300, timeout=10):
                logger.info("[MessageQueue] Acquired queue processor lock")
                try:
                    thread = threading.Thread(target=self._process_queue, daemon=True)
                    thread.start()
                    logger.info("[MessageQueue] Queue processor thread started")
                except Exception as e:
                    logger.error(f"Failed to start queue processor thread: {str(e)}")
                    self._processor_lock.release()
                    raise
            else:
                logger.error("[MessageQueue] Could not acquire queue processor lock")
                raise RuntimeError("Failed to acquire queue processor lock")
        except Exception as e:
            logger.error(f"Error in queue processor startup: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def _ensure_tables(self):
        """Ensure required database tables exist"""
        try:
            with ConnectionPool.get_connection() as conn:
                cursor = conn.cursor()
                
                # Create message queue table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS message_queue (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id TEXT NOT NULL,
                        message TEXT NOT NULL,
                        status TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        retry_count INTEGER DEFAULT 0,
                        last_retry TIMESTAMP,
                        next_retry TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        metadata TEXT,
                        message_hash TEXT UNIQUE,
                        error_message TEXT,
                        UNIQUE(message_hash)
                    )
                """)
                
                # Create webhook messages table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS webhook_messages (
                        webhook_message_id TEXT PRIMARY KEY,
                        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        message_data TEXT  -- Store any additional metadata
                    )
                """)
                
                # Add indices for message queue
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_status_retry 
                    ON message_queue(status, next_retry)
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_message_hash 
                    ON message_queue(message_hash)
                """)
                
                # Add index for webhook messages
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_webhook_processed 
                    ON webhook_messages(processed_at)
                """)
                
                conn.commit()
                logger.info("Database tables and indices initialized successfully")
                
        except Exception as e:
            logger.error(f"Error ensuring tables: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def _generate_message_hash(self, user_id: str, message: str, metadata: Dict[str, Any]) -> str:
        """Generate a unique hash for a message to detect duplicates"""
        # Include webhook_message_id in the hash if available
        webhook_id = metadata.get('webhook_message_id', '')
        message_data = f"{user_id}:{message}:{webhook_id}"
        return hashlib.md5(message_data.encode()).hexdigest()

    def is_duplicate_webhook(self, webhook_message_id: str) -> bool:
        """Check if a webhook message has already been processed."""
        if not webhook_message_id:
            logger.warning("No webhook_message_id provided for duplicate check")
            return False
            
        with self._message_lock:
            current_time = time.time()
            logger.info(f"Checking for duplicate webhook message: {webhook_message_id}")
            
            # Clean up old webhook messages
            self._webhook_messages = {
                msg_id: timestamp 
                for msg_id, timestamp in self._webhook_messages.items()
                if current_time - timestamp < self.DUPLICATE_WINDOW
            }
            
            # Check in-memory cache
            if webhook_message_id in self._webhook_messages:
                logger.info(f"Duplicate webhook message found in memory cache: {webhook_message_id}")
                return True
            
            # Check database
            try:
                with ConnectionPool.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        SELECT processed_at FROM webhook_messages 
                        WHERE webhook_message_id = ?
                    ''', (webhook_message_id,))
                    result = cursor.fetchone()
                    
                    if result:
                        processed_at = result[0]
                        logger.info(f"Duplicate webhook message found in database: {webhook_message_id}, processed at {processed_at}")
                        # Add to in-memory cache
                        self._webhook_messages[webhook_message_id] = current_time
                        return True
                        
                    logger.info(f"New webhook message received: {webhook_message_id}")
                    return False
            except Exception as e:
                logger.error(f"Error checking for duplicate webhook: {str(e)}")
                logger.error(traceback.format_exc())
                return False

    def _cleanup_webhook_messages(self):
        """Clean up old webhook message records."""
        try:
            with ConnectionPool.get_connection() as conn:
                cursor = conn.cursor()
                # Delete records older than DUPLICATE_WINDOW
                cursor.execute('''
                    DELETE FROM webhook_messages 
                    WHERE processed_at < datetime('now', ?)
                ''', (f'-{self.DUPLICATE_WINDOW} seconds',))
                conn.commit()
                
                if cursor.rowcount > 0:
                    logger.info(f"Cleaned up {cursor.rowcount} old webhook message records")
                    
        except Exception as e:
            logger.error(f"Error cleaning up webhook messages: {e}")

    def queue_webhook_message(self, from_number: str, message_id: str, content: str, message_type: str) -> bool:
        """Queue a message received from webhook."""
        try:
            # Check for duplicates first
            if self.is_duplicate_webhook(message_id):
                logger.info(f"Duplicate webhook message {message_id} - skipping")
                return False
                
            # Queue the message
            result = self.enqueue_message(
                user_id=from_number,
                message=content,
                metadata={
                    "message_id": message_id,
                    "type": message_type,
                    "source": "webhook"
                }
            )
            
            return result > 0
            
        except Exception as e:
            logger.error(f"Error queueing webhook message: {e}")
            return False

    def _try_immediate_delivery(self, user_id: str, message: str) -> bool:
        """Attempts to deliver the message immediately with better error handling."""
        try:
            logger.info("=" * 80)
            logger.info("ATTEMPTING IMMEDIATE DELIVERY")
            logger.info(f"User ID: {user_id}")
            logger.info(f"Message: {message[:100]}...")
            logger.info(f"API URL: {self.whatsapp.api_url}")
            logger.info(f"Headers: {json.dumps(self.whatsapp.headers, indent=2)}")
            
            # Check rate limiting
            with self._message_lock:
                if self._is_rate_limited(user_id):
                    logger.info(f"Rate limiting message to {user_id}, will retry later")
                    logger.info("=" * 80)
                    return False
            
            logger.info("Calling WhatsApp sender...")
            result = self.whatsapp.send_message(user_id, message)
            logger.info(f"WhatsApp API Response: {json.dumps(result, indent=2)}")
            
            if result.get('success'):
                logger.info(f"Successfully delivered message to {user_id} (ID: {result.get('message_id')})")
                
                # Track successful send for rate limiting
                with self._message_lock:
                    self._track_message_sent(user_id)
                
                logger.info("=" * 80)
                return True
                
            # Handle token errors specially
            if result.get('requires_token_refresh'):
                logger.error("WhatsApp API token has expired - stopping message processing")
                # Mark all pending messages as failed due to token
                with ConnectionPool.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        UPDATE message_queue 
                        SET status = 'failed',
                            error_message = 'Token expired - requires refresh'
                        WHERE status = 'pending'
                    """)
                    conn.commit()
                
                # Clear pending messages
                with self._lock:
                    self._pending_messages.clear()
                return False
            
            # Handle rate limiting errors
            if result.get('status_code') == 429:
                logger.warning("Rate limit hit from WhatsApp API, will retry with backoff")
                return False
            
            # Handle other errors
            error = result.get('error', 'Unknown error')
            status_code = result.get('status_code')
            logger.error(f"Failed to send message: {error} (Status: {status_code})")
            logger.error(f"Full response: {json.dumps(result, indent=2)}")
            logger.info("=" * 80)
            return False
            
        except Exception as e:
            logger.error(f"Error during message delivery: {str(e)}")
            logger.error(traceback.format_exc())
            logger.info("=" * 80)
            return False

    def _should_log(self):
        """Determine if we should log based on the throttling interval."""
        current_time = time.time()
        # Use shorter interval if we have pending messages
        interval = 5 if self._pending_messages else self._log_interval
        if current_time - self._last_log_time >= interval:
            self._last_log_time = current_time
            return True
        return False

    def _process_queue(self):
        """Process messages in the queue with enhanced logging and heartbeat"""
        logger.info("[QueueProcessor] Entered _process_queue main loop")
        heartbeat_counter = 0
        while True:
            try:
                if not self._processor_lock.is_locked():
                    logger.error("Lost queue processor lock, attempting to reacquire")
                    if not self._processor_lock.acquire(blocking=True, timeout=10):
                        logger.error("Failed to reacquire queue processor lock")
                        break
                with ConnectionPool.get_connection() as conn:
                    cursor = conn.cursor()
                    # Get pending messages ready for processing
                    cursor.execute("""
                        SELECT id, user_id, message, retry_count, metadata 
                        FROM message_queue 
                        WHERE status = ? AND next_retry <= datetime('now')
                        ORDER BY created_at ASC LIMIT 10
                    """, (MessageStatus.PENDING.value,))
                    pending_messages = cursor.fetchall()
                    if pending_messages:
                        logger.info(f"[QueueProcessor] Found {len(pending_messages)} messages to process")
                    else:
                        if heartbeat_counter % 10 == 0:
                            logger.info("[QueueProcessor] Heartbeat: no pending messages")
                        heartbeat_counter += 1
                    for msg_id, user_id, message, retry_count, metadata_str in pending_messages:
                        logger.info(f"[QueueProcessor] About to process message {msg_id} for user {user_id}")
                        try:
                            metadata = json.loads(metadata_str) if metadata_str else {}
                            logger.info(f"[QueueProcessor] Calling _process_message for msg_id={msg_id}")
                            # Update status to in progress
                            try:
                                cursor.execute("""
                                    UPDATE message_queue 
                                    SET status = ?, updated_at = datetime('now')
                                    WHERE id = ?
                                """, (MessageStatus.IN_PROGRESS.value, msg_id))
                                conn.commit()
                            except Exception as sql_update_exc:
                                logger.error(f"[QueueProcessor] SQL error updating status to in_progress for msg_id={msg_id}: {sql_update_exc}")
                                logger.error(traceback.format_exc())
                                continue
                            # Process the message
                            success = self._process_message(msg_id, user_id, message, metadata)
                            logger.info(f"[QueueProcessor] _process_message returned: {success}")
                            if success:
                                logger.info(f"[QueueProcessor] Successfully processed message {msg_id}")
                                try:
                                    cursor.execute("""
                                        UPDATE message_queue 
                                        SET status = ?, updated_at = datetime('now')
                                        WHERE id = ?
                                    """, (MessageStatus.COMPLETED.value, msg_id))
                                    conn.commit()
                                except Exception as sql_update_exc:
                                    logger.error(f"[QueueProcessor] SQL error updating status to completed for msg_id={msg_id}: {sql_update_exc}")
                                    logger.error(traceback.format_exc())
                            else:
                                next_retry = self._calculate_next_retry(retry_count)
                                if next_retry:
                                    logger.warning(f"[QueueProcessor] Message {msg_id} failed, scheduling retry #{retry_count + 1} at {next_retry}")
                                    try:
                                        cursor.execute("""
                                            UPDATE message_queue 
                                            SET status = ?, retry_count = retry_count + 1,
                                                next_retry = ?, updated_at = datetime('now')
                                            WHERE id = ?
                                        """, (MessageStatus.PENDING.value, next_retry, msg_id))
                                        conn.commit()
                                    except Exception as sql_update_exc:
                                        logger.error(f"[QueueProcessor] SQL error updating status to pending for retry for msg_id={msg_id}: {sql_update_exc}")
                                        logger.error(traceback.format_exc())
                                else:
                                    logger.error(f"[QueueProcessor] Message {msg_id} failed permanently after {retry_count} retries")
                                    try:
                                        cursor.execute("""
                                            UPDATE message_queue 
                                            SET status = ?, updated_at = datetime('now'),
                                                error_message = ?
                                            WHERE id = ?
                                        """, (MessageStatus.FAILED.value, 
                                             "Maximum retry attempts exceeded", msg_id))
                                        conn.commit()
                                    except Exception as sql_update_exc:
                                        logger.error(f"[QueueProcessor] SQL error updating status to failed for msg_id={msg_id}: {sql_update_exc}")
                                        logger.error(traceback.format_exc())
                        except Exception as e:
                            logger.error(f"[QueueProcessor] Error processing message {msg_id}: {str(e)}")
                            logger.error(traceback.format_exc())
                            # Update message status to reflect the error
                            try:
                                cursor.execute("""
                                    UPDATE message_queue 
                                    SET status = ?, error_message = ?,
                                        updated_at = datetime('now')
                                    WHERE id = ?
                                """, (MessageStatus.FAILED.value, str(e), msg_id))
                                conn.commit()
                            except Exception as sql_update_exc:
                                logger.error(f"[QueueProcessor] SQL error updating status to failed for msg_id={msg_id}: {sql_update_exc}")
                                logger.error(traceback.format_exc())
                
                # --- Backlog diagnostics: warn if many old pending messages ---
                try:
                    with ConnectionPool.get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute('''
                            SELECT COUNT(*), MIN(created_at)
                            FROM message_queue
                            WHERE status = ? AND created_at < datetime('now', '-5 minutes')
                        ''', (MessageStatus.PENDING.value,))
                        old_count, oldest = cursor.fetchone()
                        if old_count and old_count > 5:
                            logger.warning(f"[QueueProcessor] WARNING: {old_count} pending messages older than 5 minutes! Oldest: {oldest}")
                except Exception as diag_exc:
                    logger.error(f"[QueueProcessor] Error during backlog diagnostics: {diag_exc}")
                
                # Sleep briefly between processing batches
                time.sleep(1)
            except Exception as e:
                logger.error(f"[QueueProcessor] Error in queue processor: {str(e)}")
                logger.error(traceback.format_exc())
                time.sleep(5)  # Wait longer on error

    def _calculate_next_retry(self, retry_count):
        """Calculate the next retry time using exponential backoff, capped at 15 minutes."""
        intervals = [30, 60, 300, 900]  # seconds: 30s, 1m, 5m, 15m
        interval = intervals[min(retry_count, len(intervals) - 1)]
        return (datetime.utcnow() + timedelta(seconds=interval)).strftime('%Y-%m-%d %H:%M:%S')

    def _process_message(self, msg_id, user_id, message, metadata):
        """Process a message: send WhatsApp reply and log all steps/results."""
        logger.info(f"[MessageQueue] _process_message called for msg_id={msg_id}, user_id={user_id}")
        try:
            from services.ai_engine import generate_response
            # --- Improved conversation history: fetch both user and bot messages in order ---
            with ConnectionPool.get_connection() as conn:
                cursor = conn.cursor()
                # Fetch last 12 messages (user and bot) for this user, ordered by created_at
                cursor.execute('''
                    SELECT message, status, created_at
                    FROM message_queue
                    WHERE user_id = ? AND status IN (?, ?, ?)
                    ORDER BY created_at DESC
                    LIMIT 12
                ''', (user_id, MessageStatus.COMPLETED.value, MessageStatus.IN_PROGRESS.value, MessageStatus.PENDING.value))
                rows = cursor.fetchall()
            # Build conversation history: alternate user/bot, most recent last
            conversation_history = []
            for row in reversed(rows):
                msg_text, status, _ = row
                if status == MessageStatus.PENDING.value:
                    # User message (pending means just received, not yet replied)
                    conversation_history.append({"role": "user", "content": msg_text})
                else:
                    # Bot reply (completed/in_progress means bot sent a reply)
                    conversation_history.append({"role": "assistant", "content": msg_text})
            # Add current user message as the latest
            conversation_history.append({"role": "user", "content": message})
            # Only keep the last 6 exchanges (12 turns)
            conversation_history = conversation_history[-12:]
            ai_result = generate_response(user_id, conversation_history)
            if ai_result.get("status") == "success":
                response_text = ai_result.get("message", "Sorry, I couldn't generate a response.")
            else:
                response_text = ai_result.get("message", "Sorry, I couldn't generate a response.")
            ws = WhatsAppService()
            logger.info(f"[MessageQueue] Preparing to send WhatsApp reply to {user_id}: {response_text}")
            send_result = ws.send_message(user_id, response_text)
            logger.info(f"[MessageQueue] WhatsApp send_message result: {send_result}")
            if send_result and send_result.get('success'):
                logger.info(f"[MessageQueue] Successfully sent WhatsApp reply to {user_id}: {response_text}")
                return True
            else:
                logger.error(f"[MessageQueue] Failed to send WhatsApp reply to {user_id}: {send_result}")
                return False
        except Exception as e:
            logger.error(f"[MessageQueue] Exception in _process_message: {e}")
            logger.error(traceback.format_exc())
            return False

    def get_message_status(self, message_id: int) -> Dict[str, Any]:
        """Get the current status of a message."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT status, retry_count, error_message, created_at, last_attempt
                FROM message_queue
                WHERE id = ?
            ''', (message_id,))
            result = cursor.fetchone()
            
            if result:
                status, retry_count, error_message, created_at, last_attempt = result
                return {
                    "message_id": message_id,
                    "status": status,
                    "retry_count": retry_count,
                    "error_message": error_message,
                    "created_at": created_at,
                    "last_attempt": last_attempt
                }
            return {"error": "Message not found"}

    def cancel_message(self, message_id: int) -> bool:
        """Cancel a pending message."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE message_queue
                    SET status = ?
                    WHERE id = ? AND status = ?
                ''', (MessageStatus.CANCELLED.value, message_id, MessageStatus.PENDING.value))
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error cancelling message {message_id}: {str(e)}")
            return False

    def _should_check_database(self):
        """Determine if we should check the database for new messages."""
        current_time = time.time()
        if current_time - self._last_db_check >= self._db_check_interval:
            self._last_db_check = current_time
            return True
        return False

    def _is_duplicate_message(self, message_hash: str) -> bool:
        """Check if a message is a duplicate within the duplicate window."""
        current_time = time.time()
        with self._message_lock:
            if message_hash in self._processed_messages:
                last_time = self._processed_messages[message_hash]
                if current_time - last_time < self.DUPLICATE_WINDOW:
                    logger.warning(f"Duplicate message detected (hash: {message_hash})")
                    return True

            # Check database for duplicates
            try:
                with ConnectionPool.get_connection() as conn:
                    cursor = conn.cursor()
                    # Look for any recent message with the same hash
                    cursor.execute("""
                        SELECT COUNT(*) FROM message_queue 
                        WHERE message_hash = ? 
                        AND created_at > datetime('now', ?)
                    """, (message_hash, f'-{self.DUPLICATE_WINDOW} seconds'))
                    
                    count = cursor.fetchone()[0]
                    if count > 0:
                        logger.warning(f"Duplicate message found in database (hash: {message_hash})")
                        return True
            except Exception as e:
                logger.error(f"Error checking for duplicates in DB: {str(e)}")
                logger.error(traceback.format_exc())
                # If DB check fails, fall back to memory-only check
                pass

            # Not a duplicate - add to tracking
            self._processed_messages[message_hash] = current_time
            return False

    def __del__(self):
        """Clean up resources when the instance is destroyed."""
        if hasattr(self, '_processor_lock') and self._processor_lock:
            self._processor_lock.release()

    def cleanup(self):
        """Explicitly clean up resources."""
        if hasattr(self, '_processor_lock') and self._processor_lock:
            self._processor_lock.release()

    def get_queue_status(self) -> Dict[str, Any]:
        """Get current status of the message queue."""
        try:
            with ConnectionPool.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get counts by status
                cursor.execute("""
                    SELECT status, COUNT(*) 
                    FROM message_queue 
                    GROUP BY status
                """)
                status_counts = dict(cursor.fetchall())
                
                # Get oldest pending message
                cursor.execute("""
                    SELECT MIN(created_at), COUNT(*) 
                    FROM message_queue 
                    WHERE status = ?
                """, (MessageStatus.PENDING.value,))
                oldest_pending, pending_count = cursor.fetchone()
                
                # Get recent failures
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM message_queue 
                    WHERE status = ? 
                    AND last_attempt > datetime('now', '-1 hour')
                """, (MessageStatus.FAILED.value,))
                recent_failures = cursor.fetchone()[0]
                
                return {
                    "status_counts": status_counts,
                    "oldest_pending": oldest_pending,
                    "pending_count": pending_count,
                    "recent_failures": recent_failures,
                    "queue_processor_running": self._processor_lock is not None and self._processor_lock.lock_handle is not None
                }
                
        except Exception as e:
            logger.error(f"Error getting queue status: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                "error": str(e)
            }

    def enqueue_message(self, user_id: str, message: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Add a message to the queue with enhanced logging and duplicate detection"""
        try:
            metadata = metadata or {}
            message_hash = self._generate_message_hash(user_id, message, metadata)
            
            logger.info(f"Attempting to enqueue message for user {user_id}")
            logger.debug(f"Message content: {message[:100]}...")  # Log first 100 chars
            logger.debug(f"Message metadata: {metadata}")
            
            webhook_message_id = metadata.get('webhook_message_id')
            if webhook_message_id and self.is_duplicate_webhook(webhook_message_id):
                logger.info(f"Skipping duplicate webhook message: {webhook_message_id}")
                return False
                
            with ConnectionPool.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute("""
                        INSERT INTO message_queue 
                        (user_id, message, status, metadata, message_hash, next_retry)
                        VALUES (?, ?, ?, ?, ?, datetime('now'))
                    """, (user_id, message, MessageStatus.PENDING.value, 
                         json.dumps(metadata), message_hash))
                    
                    if webhook_message_id:
                        cursor.execute("""
                            INSERT INTO webhook_messages 
                            (webhook_message_id, message_data)
                            VALUES (?, ?)
                        """, (webhook_message_id, json.dumps({
                            'user_id': user_id,
                            'message_hash': message_hash,
                            'metadata': metadata
                        })))
                    
                    conn.commit()
                    logger.info(f"Successfully enqueued message with hash: {message_hash}")
                    return True
                    
                except sqlite3.IntegrityError as e:
                    if "UNIQUE constraint failed" in str(e):
                        logger.info(f"Duplicate message detected with hash: {message_hash}")
                        return False
                    raise
                    
        except Exception as e:
            logger.error(f"Error enqueuing message: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def _process_message(self, msg_id, user_id, message, metadata):
        """Process a message: send WhatsApp reply and log all steps/results."""
        logger.info(f"[MessageQueue] _process_message called for msg_id={msg_id}, user_id={user_id}")
        try:
            from services.ai_engine import generate_response
            # --- Improved conversation history: fetch both user and bot messages in order ---
            with ConnectionPool.get_connection() as conn:
                cursor = conn.cursor()
                # Fetch last 12 messages (user and bot) for this user, ordered by created_at
                cursor.execute('''
                    SELECT message, status, created_at
                    FROM message_queue
                    WHERE user_id = ? AND status IN (?, ?, ?)
                    ORDER BY created_at DESC
                    LIMIT 12
                ''', (user_id, MessageStatus.COMPLETED.value, MessageStatus.IN_PROGRESS.value, MessageStatus.PENDING.value))
                rows = cursor.fetchall()
            # Build conversation history: alternate user/bot, most recent last
            conversation_history = []
            for row in reversed(rows):
                msg_text, status, _ = row
                if status == MessageStatus.PENDING.value:
                    # User message (pending means just received, not yet replied)
                    conversation_history.append({"role": "user", "content": msg_text})
                else:
                    # Bot reply (completed/in_progress means bot sent a reply)
                    conversation_history.append({"role": "assistant", "content": msg_text})
            # Add current user message as the latest
            conversation_history.append({"role": "user", "content": message})
            # Only keep the last 6 exchanges (12 turns)
            conversation_history = conversation_history[-12:]
            ai_result = generate_response(user_id, conversation_history)
            if ai_result.get("status") == "success":
                response_text = ai_result.get("message", "Sorry, I couldn't generate a response.")
            else:
                response_text = ai_result.get("message", "Sorry, I couldn't generate a response.")
            ws = WhatsAppService()
            logger.info(f"[MessageQueue] Preparing to send WhatsApp reply to {user_id}: {response_text}")
            send_result = ws.send_message(user_id, response_text)
            logger.info(f"[MessageQueue] WhatsApp send_message result: {send_result}")
            if send_result and send_result.get('success'):
                logger.info(f"[MessageQueue] Successfully sent WhatsApp reply to {user_id}: {response_text}")
                return True
            else:
                logger.error(f"[MessageQueue] Failed to send WhatsApp reply to {user_id}: {send_result}")
                return False
        except Exception as e:
            logger.error(f"[MessageQueue] Exception in _process_message: {e}")
            logger.error(traceback.format_exc())
            return False

    def get_message_status(self, message_id: int) -> Dict[str, Any]:
        """Get the current status of a message."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT status, retry_count, error_message, created_at, last_attempt
                FROM message_queue
                WHERE id = ?
            ''', (message_id,))
            result = cursor.fetchone()
            
            if result:
                status, retry_count, error_message, created_at, last_attempt = result
                return {
                    "message_id": message_id,
                    "status": status,
                    "retry_count": retry_count,
                    "error_message": error_message,
                    "created_at": created_at,
                    "last_attempt": last_attempt
                }
            return {"error": "Message not found"}

    def cancel_message(self, message_id: int) -> bool:
        """Cancel a pending message."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE message_queue
                    SET status = ?
                    WHERE id = ? AND status = ?
                ''', (MessageStatus.CANCELLED.value, message_id, MessageStatus.PENDING.value))
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error cancelling message {message_id}: {str(e)}")
            return False

    def _should_check_database(self):
        """Determine if we should check the database for new messages."""
        current_time = time.time()
        if current_time - self._last_db_check >= self._db_check_interval:
            self._last_db_check = current_time
            return True
        return False

    def _is_duplicate_message(self, message_hash: str) -> bool:
        """Check if a message is a duplicate within the duplicate window."""
        current_time = time.time()
        with self._message_lock:
            if message_hash in self._processed_messages:
                last_time = self._processed_messages[message_hash]
                if current_time - last_time < self.DUPLICATE_WINDOW:
                    logger.warning(f"Duplicate message detected (hash: {message_hash})")
                    return True

            # Check database for duplicates
            try:
                with ConnectionPool.get_connection() as conn:
                    cursor = conn.cursor()
                    # Look for any recent message with the same hash
                    cursor.execute("""
                        SELECT COUNT(*) FROM message_queue 
                        WHERE message_hash = ? 
                        AND created_at > datetime('now', ?)
                    """, (message_hash, f'-{self.DUPLICATE_WINDOW} seconds'))
                    
                    count = cursor.fetchone()[0]
                    if count > 0:
                        logger.warning(f"Duplicate message found in database (hash: {message_hash})")
                        return True
            except Exception as e:
                logger.error(f"Error checking for duplicates in DB: {str(e)}")
                logger.error(traceback.format_exc())
                # If DB check fails, fall back to memory-only check
                pass

            # Not a duplicate - add to tracking
            self._processed_messages[message_hash] = current_time
            return False

    def __del__(self):
        """Clean up resources when the instance is destroyed."""
        if hasattr(self, '_processor_lock') and self._processor_lock:
            self._processor_lock.release()

    def cleanup(self):
        """Explicitly clean up resources."""
        if hasattr(self, '_processor_lock') and self._processor_lock:
            self._processor_lock.release()

    def get_queue_status(self) -> Dict[str, Any]:
        """Get current status of the message queue."""
        try:
            with ConnectionPool.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get counts by status
                cursor.execute("""
                    SELECT status, COUNT(*) 
                    FROM message_queue 
                    GROUP BY status
                """)
                status_counts = dict(cursor.fetchall())
                
                # Get oldest pending message
                cursor.execute("""
                    SELECT MIN(created_at), COUNT(*) 
                    FROM message_queue 
                    WHERE status = ?
                """, (MessageStatus.PENDING.value,))
                oldest_pending, pending_count = cursor.fetchone()
                
                # Get recent failures
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM message_queue 
                    WHERE status = ? 
                    AND last_attempt > datetime('now', '-1 hour')
                """, (MessageStatus.FAILED.value,))
                recent_failures = cursor.fetchone()[0]
                
                return {
                    "status_counts": status_counts,
                    "oldest_pending": oldest_pending,
                    "pending_count": pending_count,
                    "recent_failures": recent_failures,
                    "queue_processor_running": self._processor_lock is not None and self._processor_lock.lock_handle is not None
                }
                
        except Exception as e:
            logger.error(f"Error getting queue status: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                "error": str(e)
            }

    def enqueue_message(self, user_id: str, message: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Add a message to the queue with enhanced logging and duplicate detection"""
        try:
            metadata = metadata or {}
            message_hash = self._generate_message_hash(user_id, message, metadata)
            
            logger.info(f"Attempting to enqueue message for user {user_id}")
            logger.debug(f"Message content: {message[:100]}...")  # Log first 100 chars
            logger.debug(f"Message metadata: {metadata}")
            
            webhook_message_id = metadata.get('webhook_message_id')
            if webhook_message_id and self.is_duplicate_webhook(webhook_message_id):
                logger.info(f"Skipping duplicate webhook message: {webhook_message_id}")
                return False
                
            with ConnectionPool.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute("""
                        INSERT INTO message_queue 
                        (user_id, message, status, metadata, message_hash, next_retry)
                        VALUES (?, ?, ?, ?, ?, datetime('now'))
                    """, (user_id, message, MessageStatus.PENDING.value, 
                         json.dumps(metadata), message_hash))
                    
                    if webhook_message_id:
                        cursor.execute("""
                            INSERT INTO webhook_messages 
                            (webhook_message_id, message_data)
                            VALUES (?, ?)
                        """, (webhook_message_id, json.dumps({
                            'user_id': user_id,
                            'message_hash': message_hash,
                            'metadata': metadata
                        })))
                    
                    conn.commit()
                    logger.info(f"Successfully enqueued message with hash: {message_hash}")
                    return True
                    
                except sqlite3.IntegrityError as e:
                    if "UNIQUE constraint failed" in str(e):
                        logger.info(f"Duplicate message detected with hash: {message_hash}")
                        return False
                    raise
                    
        except Exception as e:
            logger.error(f"Error enqueuing message: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def _process_message(self, msg_id, user_id, message, metadata):
        """Process a message: send WhatsApp reply and log all steps/results."""
        logger.info(f"[MessageQueue] _process_message called for msg_id={msg_id}, user_id={user_id}")
        try:
            from services.ai_engine import generate_response
            # --- Improved conversation history: fetch both user and bot messages in order ---
            with ConnectionPool.get_connection() as conn:
                cursor = conn.cursor()
                # Fetch last 12 messages (user and bot) for this user, ordered by created_at
                cursor.execute('''
                    SELECT message, status, created_at
                    FROM message_queue
                    WHERE user_id = ? AND status IN (?, ?, ?)
                    ORDER BY created_at DESC
                    LIMIT 12
                ''', (user_id, MessageStatus.COMPLETED.value, MessageStatus.IN_PROGRESS.value, MessageStatus.PENDING.value))
                rows = cursor.fetchall()
            # Build conversation history: alternate user/bot, most recent last
            conversation_history = []
            for row in reversed(rows):
                msg_text, status, _ = row
                if status == MessageStatus.PENDING.value:
                    # User message (pending means just received, not yet replied)
                    conversation_history.append({"role": "user", "content": msg_text})
                else:
                    # Bot reply (completed/in_progress means bot sent a reply)
                    conversation_history.append({"role": "assistant", "content": msg_text})
            # Add current user message as the latest
            conversation_history.append({"role": "user", "content": message})
            # Only keep the last 6 exchanges (12 turns)
            conversation_history = conversation_history[-12:]
            ai_result = generate_response(user_id, conversation_history)
            if ai_result.get("status") == "success":
                response_text = ai_result.get("message", "Sorry, I couldn't generate a response.")
            else:
                response_text = ai_result.get("message", "Sorry, I couldn't generate a response.")
            ws = WhatsAppService()
            logger.info(f"[MessageQueue] Preparing to send WhatsApp reply to {user_id}: {response_text}")
            send_result = ws.send_message(user_id, response_text)
            logger.info(f"[MessageQueue] WhatsApp send_message result: {send_result}")
            if send_result and send_result.get('success'):
                logger.info(f"[MessageQueue] Successfully sent WhatsApp reply to {user_id}: {response_text}")
                return True
            else:
                logger.error(f"[MessageQueue] Failed to send WhatsApp reply to {user_id}: {send_result}")
                return False
        except Exception as e:
            logger.error(f"[MessageQueue] Exception in _process_message: {e}")
            logger.error(traceback.format_exc())
            return False