import sqlite3
import time
import threading
from datetime import datetime, timedelta
import pytz
from zoneinfo import ZoneInfo
import json
import logging
from typing import Optional, Dict, Any, List
import traceback
from collections import OrderedDict
from services.base_models import (
    ConnectionPool, 
    get_user_timezone, 
    reminder_notifier,
    DATABASE_NAME
)

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Connection pool
class ConnectionPool:
    _pool = []
    _max_connections = 5
    _lock = threading.Lock()
    _last_log_time = 0
    _log_interval = 60  # Increase log interval to 60 seconds
    _initialized = False
    _active_connections = set()  # Track active connections
    
    @classmethod
    def initialize(cls):
        """Initialize the connection pool with some connections."""
        if not cls._initialized:
            with cls._lock:
                if not cls._initialized:  # Double-check under lock
                    try:
                        for _ in range(2):  # Start with 2 connections
                            conn = sqlite3.connect(DATABASE_NAME, check_same_thread=False)
                            conn.row_factory = sqlite3.Row
                            cls._pool.append(conn)
                        cls._initialized = True
                        logger.info("Connection pool initialized successfully")
                    except Exception as e:
                        logger.error(f"Failed to initialize connection pool: {e}")
                        logger.error(traceback.format_exc())
                        raise

    @classmethod
    def get_connection(cls):
        """Get a connection from the pool or create a new one if needed."""
        if not cls._initialized:
            cls.initialize()

        with cls._lock:
            if len(cls._pool) > 0:
                conn = cls._pool.pop()
            else:
                # Create new connection if pool is empty and we haven't hit max
                if len(cls._active_connections) < cls._max_connections:
                    conn = sqlite3.connect(DATABASE_NAME, check_same_thread=False)
                    conn.row_factory = sqlite3.Row
                else:
                    raise Exception("Connection pool exhausted")
            
            cls._active_connections.add(conn)
            return ConnectionContextManager(cls, conn)

    @classmethod
    def return_connection(cls, conn):
        """Return a connection to the pool."""
        with cls._lock:
            if conn in cls._active_connections:
                cls._active_connections.remove(conn)
                if len(cls._pool) < cls._max_connections:
                    cls._pool.append(conn)
                else:
                    conn.close()

class ConnectionContextManager:
    def __init__(self, pool, connection):
        self.pool = pool
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.return_connection(self.connection)

def init_db():
    """Initialize the database with required tables"""
    logger.info("Initializing database...")
    try:
        # Create a direct connection for initialization
        conn = sqlite3.connect(DATABASE_NAME, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Create user_memory table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_memory (
                user_id TEXT,
                key TEXT,
                value TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, key)
            )
        """)

        # Create user_timezones table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_timezones (
                user_id TEXT PRIMARY KEY,
                timezone TEXT NOT NULL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create user_sessions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_sessions (
                user_id TEXT PRIMARY KEY,
                session_data TEXT,
                last_interaction TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create conversation_history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS conversation_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                role TEXT,
                content TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create authorized_numbers table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS authorized_numbers (
                phone_number TEXT PRIMARY KEY,
                name TEXT,
                added_by TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create reminders table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reminders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                message TEXT,
                reminder_time TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'pending'
            )
        """)

        conn.commit()
        logger.info("Database initialized successfully")
        
        # Close the initialization connection
        conn.close()
        
        # Now initialize the connection pool
        ConnectionPool.initialize()
        
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        logger.error(traceback.format_exc())
        raise

# --- Utility Functions ---
def get_user_timezone(user_id: str) -> str:
    """Retrieves a user's stored timezone with improved error handling."""
    with ConnectionPool.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT timezone 
                FROM user_timezones 
                WHERE user_id = ? 
                AND last_updated > datetime('now', '-30 days')
            """, (user_id,))
            result = cursor.fetchone()
            if result:
                # Validate the stored timezone is still valid
                try:
                    pytz.timezone(result[0])
                    return result[0]
                except pytz.exceptions.UnknownTimeZoneError:
                    logger.error(f"Invalid timezone stored for user {user_id}: {result[0]}")
                    # Fall through to default
            
            default_tz = 'Europe/London'
            logger.info(f"No valid timezone found for user {user_id}. Using default: {default_tz}")
            return default_tz
            
        except Exception as e:
            logger.error(f"Error retrieving timezone for {user_id}: {e}")
            logger.error(traceback.format_exc())
            return 'Europe/London'  # Fallback on error

def set_user_timezone(user_id: str, timezone: str) -> dict:
    """Stores a user's timezone with validation and normalization."""
    with ConnectionPool.get_connection() as conn:
        cursor = conn.cursor()
        try:
            # Clean and normalize timezone string
            timezone = timezone.strip()
            
            # Handle common abbreviations
            timezone_map = {
                'GMT': 'Europe/London',
                'EST': 'America/New_York',
                'PST': 'America/Los_Angeles',
                'BST': 'Europe/London',
                # Add more common mappings as needed
            }
            
            if timezone.upper() in timezone_map:
                timezone = timezone_map[timezone.upper()]
            
            # Validate timezone string using pytz
            tz = pytz.timezone(timezone)
            normalized_name = tz.zone  # Get the standardized name
            
            # Store with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    cursor.execute("""
                        INSERT OR REPLACE INTO user_timezones 
                        (user_id, timezone, last_updated) 
                        VALUES (?, ?, datetime('now'))
                    """, (user_id, normalized_name))
                    conn.commit()
                    
                    # Verify storage
                    cursor.execute("SELECT timezone FROM user_timezones WHERE user_id = ?", (user_id,))
                    stored_tz = cursor.fetchone()
                    if stored_tz and stored_tz[0] == normalized_name:
                        logger.info(f"Successfully set timezone to '{normalized_name}' for user {user_id}")
                        return {"status": "success", "message": f"Timezone set to {normalized_name}"}
                    else:
                        raise Exception("Verification failed: stored timezone does not match input")
                        
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    continue
                    
        except pytz.exceptions.UnknownTimeZoneError:
            logger.warning(f"Invalid timezone specified: '{timezone}' for user {user_id}")
            return {
                "status": "error", 
                "message": f"Invalid timezone specified: {timezone}. Please use a valid timezone name (e.g., 'Europe/London', 'America/New_York')"
            }
        except Exception as e:
            logger.error(f"Error setting timezone for {user_id}: {e}")
            logger.error(traceback.format_exc())
            return {"status": "error", "message": f"Failed to set timezone: {str(e)}"}

def convert_to_utc(dt_str: str, user_id: str) -> datetime:
    """Converts a datetime string in user's timezone to UTC datetime object."""
    user_tz_str = get_user_timezone(user_id)
    user_tz = pytz.timezone(user_tz_str)
    
    # Parse the datetime string without timezone info initially
    dt_naive = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
    
    # Localize the naive datetime to the user's timezone
    dt_local = user_tz.localize(dt_naive)
    
    # Convert to UTC
    dt_utc = dt_local.astimezone(pytz.UTC)
    return dt_utc

def convert_to_user_timezone(utc_dt: datetime, user_id: str) -> datetime:
    """Converts a UTC datetime object to the user's local timezone datetime object."""
    user_tz_str = get_user_timezone(user_id)
    user_tz = pytz.timezone(user_tz_str)
    return utc_dt.astimezone(user_tz)

# --- Authorized Numbers Management ---
def is_number_authorized(phone_number: str) -> bool:
    """Checks if a phone number is in the authorized list."""
    with ConnectionPool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM authorized_numbers WHERE phone_number = ?", (phone_number,))
        return cursor.fetchone() is not None

def add_authorized_number(phone_number: str, name: str, added_by: str = "admin") -> dict:
    """Adds a phone number to the authorized list."""
    with ConnectionPool.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("INSERT INTO authorized_numbers (phone_number, name, added_by) VALUES (?, ?, ?)",
                           (phone_number, name, added_by))
            conn.commit()
            return {"status": "success", "message": f"Number {phone_number} added for {name}."}
        except sqlite3.IntegrityError:
            return {"status": "error", "message": f"Number {phone_number} is already authorized."}
        except Exception as e:
            logger.error(f"Error adding authorized number {phone_number}: {e}")
            logger.error(traceback.format_exc())
            return {"status": "error", "message": f"Failed to add number: {str(e)}"}

def remove_authorized_number(phone_number: str) -> dict:
    """Removes a phone number from the authorized list."""
    with ConnectionPool.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM authorized_numbers WHERE phone_number = ?", (phone_number,))
            conn.commit()
            if cursor.rowcount > 0:
                return {"status": "success", "message": f"Number {phone_number} removed."}
            else:
                return {"status": "error", "message": f"Number {phone_number} not found."}
        except Exception as e:
            logger.error(f"Error removing authorized number {phone_number}: {e}")
            logger.error(traceback.format_exc())
            return {"status": "error", "message": f"Failed to remove number: {str(e)}"}

def list_authorized_numbers() -> list:
    """Lists all authorized phone numbers."""
    with ConnectionPool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT phone_number, name, added_by, added_on FROM authorized_numbers")
        return [{"phone_number": row[0], "name": row[1], "added_by": row[2], "added_on": row[3]} for row in cursor.fetchall()]

# --- User Session Management ---
def track_user_interaction(user_id: str):
    """Updates the last interaction timestamp for a user session."""
    with ConnectionPool.get_connection() as conn:
        cursor = conn.cursor()
        current_time = datetime.now().isoformat()
        try:
            cursor.execute("INSERT OR REPLACE INTO user_sessions (user_id, last_interaction) VALUES (?, ?)",
                           (user_id, current_time))
            conn.commit()
            logger.debug(f"Tracked interaction for user {user_id}")
        except Exception as e:
            logger.error(f"Error tracking user interaction for {user_id}: {e}")
            logger.error(traceback.format_exc())

def retrieve_session(user_id: str) -> dict:
    """Retrieves session data for a user."""
    with ConnectionPool.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT session_data, last_interaction FROM user_sessions WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
            if result:
                session_data = json.loads(result[0]) if result[0] else {}
                last_interaction = result[1]
                logger.debug(f"Retrieved session for {user_id}. Last interaction: {last_interaction}")
                return {"session_data": session_data, "last_interaction": last_interaction}
            logger.debug(f"No session found for user {user_id}")
            return {"session_data": {}, "last_interaction": None}
        except Exception as e:
            logger.error(f"Error retrieving session for {user_id}: {e}")
            logger.error(traceback.format_exc())
            return {"session_data": {}, "last_interaction": None}

def store_session(user_id: str, session_data: dict):
    """Stores session data for a user."""
    with ConnectionPool.get_connection() as conn:
        cursor = conn.cursor()
        current_time = datetime.now().isoformat()
        try:
            session_json = json.dumps(session_data)
            cursor.execute("INSERT OR REPLACE INTO user_sessions (user_id, last_interaction, session_data) VALUES (?, ?, ?)",
                           (user_id, current_time, session_json))
            conn.commit()
            logger.debug(f"Stored session for user {user_id}.")
        except Exception as e:
            logger.error(f"Error storing session for {user_id}: {e}")
            logger.error(traceback.format_exc())

def clear_old_sessions(days_old: int = 30) -> dict:
    """Clears sessions (and potentially associated history) older than a specified number of days."""
    with ConnectionPool.get_connection() as conn:
        cursor = conn.cursor()
        cutoff_date = datetime.now() - timedelta(days=days_old)
        cutoff_iso = cutoff_date.isoformat()
        
        try:
            # Get user_ids of sessions to delete
            cursor.execute("SELECT user_id FROM user_sessions WHERE last_interaction < ?", (cutoff_iso,))
            user_ids_to_delete = [row[0] for row in cursor.fetchall()]

            if not user_ids_to_delete:
                logger.info("No old sessions to clear.")
                return {"status": "success", "message": "No old sessions to clear."}

            # Delete conversation history for these users
            placeholders = ','.join('?' for _ in user_ids_to_delete)
            cursor.execute(f"DELETE FROM conversation_history WHERE user_id IN ({placeholders})", user_ids_to_delete)
            deleted_history_count = cursor.rowcount
            logger.info(f"Deleted {deleted_history_count} old conversation history entries.")

            # Delete user memory for these users
            # cursor.execute(f"DELETE FROM user_memory WHERE user_id IN ({placeholders})", user_ids_to_delete)
            # deleted_memory_count = cursor.rowcount
            # logger.info(f"Deleted {deleted_memory_count} old user memory entries.")

            # Delete reminders for these users
            cursor.execute(f"DELETE FROM reminders WHERE user_id IN ({placeholders})", user_ids_to_delete)
            deleted_reminders_count = cursor.rowcount
            logger.info(f"Deleted {deleted_reminders_count} old reminder entries.")
            
            # Delete timezones for these users
            # cursor.execute(f"DELETE FROM user_timezones WHERE user_id IN ({placeholders})", user_ids_to_delete)
            # deleted_timezones_count = cursor.rowcount
            # logger.info(f"Deleted {deleted_timezones_count} old user timezone entries.")

            # Finally, delete the sessions themselves
            cursor.execute("DELETE FROM user_sessions WHERE last_interaction < ?", (cutoff_iso,))
            deleted_sessions_count = cursor.rowcount
            conn.commit()
            
            logger.info(f"Cleared {deleted_sessions_count} sessions older than {days_old} days.")
            return {"status": "success", "message": f"Cleared {deleted_sessions_count} sessions and associated data older than {days_old} days."}
        except Exception as e:
            logger.error(f"Error clearing old sessions: {e}")
            logger.error(traceback.format_exc())
            return {"status": "error", "message": f"Failed to clear old sessions: {str(e)}"}

# --- Conversation History Management ---
def store_conversation_message(user_id: str, role: str, content: str):
    """Store a message in the conversation history."""
    logger.info(f"Attempting to store message for {user_id} (role: {role})...")
    try:
        with ConnectionPool.get_connection() as conn: # Fixed: Connection2Pool -> ConnectionPool
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO conversation_history (user_id, role, content)
                VALUES (?, ?, ?)
            ''', (user_id, role, content))
            conn.commit()
            logger.info(f"Successfully stored message for {user_id} (role: {role}): {content[:50]}...")
    except Exception as e:
        logger.error(f"Error storing conversation message for {user_id}: {e}")
        logger.error(traceback.format_exc())
        # Re-raise the exception after logging if app.py expects it, or handle more gracefully
        # In this context, it's better to re-raise if it's a critical DB issue
        raise

def retrieve_conversation_history(user_id: str, limit: int = 10) -> list:
    """Retrieve recent conversation history for a user."""
    logger.info(f"Attempting to retrieve conversation history for {user_id} (limit: {limit})...")
    try:
        with ConnectionPool.get_connection() as conn: # Fixed: Connection2Pool -> ConnectionPool
            cursor = conn.cursor()
            cursor.execute('''
                SELECT role, content
                FROM conversation_history
                WHERE user_id = ?
                ORDER BY timestamp DESC
                LIMIT ?
            ''', (user_id, limit))
            
            # Convert to list of dicts and reverse to get chronological order
            history = [{"role": role, "content": content} for role, content in cursor.fetchall()]
            formatted_history = list(reversed(history))
            logger.info(f"Retrieved {len(formatted_history)} history items for {user_id}.")
            return formatted_history
    except Exception as e:
        logger.error(f"Error retrieving conversation history for {user_id}: {e}")
        logger.error(traceback.format_exc())
        return []

# --- User Memory Management ---
_memory_cache = {}
_memory_cache_ttl = 300  # 5 minutes

def store_user_memory(user_id: str, key: str, value: str) -> dict:
    """Stores a piece of factual information about the user."""
    max_retries = 3
    for attempt in range(max_retries):
        with ConnectionPool.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # First check if value is different from what's already stored
                cursor.execute("SELECT value FROM user_memory WHERE user_id = ? AND key = ?", (user_id, key))
                existing = cursor.fetchone()
                
                if existing and existing[0] == value:
                    logger.info(f"Value for {key} unchanged for user {user_id}")
                    return {"status": "success", "message": f"Value for '{key}' already set."}
                
                # Store the new value
                cursor.execute("""
                    INSERT OR REPLACE INTO user_memory (user_id, key, value, last_updated)
                    VALUES (?, ?, ?, datetime('now'))
                """, (user_id, key, value))
                conn.commit()
                
                # Verify the storage
                cursor.execute("SELECT value FROM user_memory WHERE user_id = ? AND key = ?", (user_id, key))
                stored_value = cursor.fetchone()
                if stored_value and stored_value[0] == value:
                    logger.info(f"Successfully stored memory for user {user_id}: {key}={value}")
                    return {"status": "success", "message": f"Remembered '{key}'."}
                else:
                    raise Exception("Verification failed: stored value does not match input")
                    
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed storing user memory for {user_id} (key: {key}): {e}")
                logger.error(traceback.format_exc())
                if attempt == max_retries - 1:
                    return {"status": "error", "message": f"Failed to store memory after {max_retries} attempts: {str(e)}"}
                continue
    return {"status": "error", "message": "Failed to store memory: maximum retries exceeded"}

def retrieve_user_memory(user_id: str, key: str) -> dict:
    """Retrieves a piece of information previously stored about the user."""
    # Check cache first
    cache_key = f"{user_id}:{key}"
    if cache_key in _memory_cache:
        cached_value, cache_time = _memory_cache[cache_key]
        if (datetime.now() - cache_time).total_seconds() < _memory_cache_ttl:
            logger.info(f"Retrieved {key} from cache for user {user_id}")
            return {"status": "success", "value": cached_value, "message": f"Retrieved '{key}'.", "cached": True}
    
    max_retries = 3
    for attempt in range(max_retries):
        with ConnectionPool.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    SELECT value, last_updated 
                    FROM user_memory 
                    WHERE user_id = ? AND key = ?
                """, (user_id, key))
                result = cursor.fetchone()
                
                if result:
                    value, last_updated = result
                    # Update cache
                    _memory_cache[cache_key] = (value, datetime.now())
                    logger.info(f"Retrieved memory for user {user_id}: {key}={value}")
                    return {"status": "success", "value": value, "message": f"Retrieved '{key}'.", "cached": False}
                else:
                    logger.info(f"No memory found for key '{key}' for user {user_id}")
                    return {"status": "not_found", "message": f"No memory found for '{key}'."}
                    
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed retrieving user memory for {user_id} (key: {key}): {e}")
                logger.error(traceback.format_exc())
                if attempt == max_retries - 1:
                    return {"status": "error", "message": f"Failed to retrieve memory after {max_retries} attempts: {str(e)}"}
                continue
    
    return {"status": "error", "message": "Failed to retrieve memory: maximum retries exceeded"}

def delete_user_memory(user_id: str, key: str) -> dict:
    """Deletes a specific piece of information about the user."""
    with ConnectionPool.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM user_memory WHERE user_id = ? AND key = ?", (user_id, key))
            conn.commit()
            if cursor.rowcount > 0:
                return {"status": "success", "message": f"Forgot '{key}'."}
            else:
                return {"status": "not_found", "message": f"No memory found for '{key}' to delete."}
        except Exception as e:
            logger.error(f"Error deleting user memory for {user_id} (key: {key}): {e}")
            logger.error(traceback.format_exc())
            return {"status": "error", "message": f"Failed to delete memory: {str(e)}"}

# --- Reminder Management ---
# In services/db_services.py
def ensure_reminders_table():
    """Ensures the reminders table exists."""
    with ConnectionPool.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS reminders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    reminder_text TEXT NOT NULL,              
                    scheduled_time TEXT NOT NULL,        
                    original_timezone_str TEXT NOT NULL,
                    reminder_type TEXT DEFAULT 'reminder', 
                    is_sent INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()
            logger.info("Reminders table ensured.")
        except Exception as e:
            logger.error(f"Error ensuring reminders table: {e}")
            logger.error(traceback.format_exc())

def add_reminder(user_id: str, reminder_text: str, scheduled_time: str, original_timezone_str: str, reminder_type: str = 'reminder') -> dict:
    """Adds a new reminder to the database."""
    with ConnectionPool.get_connection() as conn:
        cursor = conn.cursor()
        try:
            # scheduled_time should already be in user's timezone if from set_reminder tool
            # Store as string, but ensure it's parsable
            datetime.strptime(scheduled_time, "%Y-%m-%d %H:%M") 
            
            cursor.execute(
                "INSERT INTO reminders (user_id, reminder_text, scheduled_time, original_timezone_str, reminder_type, is_sent) VALUES (?, ?, ?, ?, ?, ?)",
                (user_id, reminder_text, scheduled_time, original_timezone_str, reminder_type, 0)  # Ensure is_sent is 0
            )
            conn.commit()
            logger.info(f"Reminder added for {user_id} at {scheduled_time}: {reminder_text}")
            return {"status": "success", "message": f"Reminder set for {scheduled_time} (your local time)."}
        except ValueError:
            logger.error(f"Invalid scheduled_time format for reminder: {scheduled_time}")
            return {"status": "error", "message": "Invalid time format for reminder. Please use YYYY-MM-DD HH:MM."}
        except Exception as e:
            logger.error(f"Error adding reminder for {user_id}: {e}")
            logger.error(traceback.format_exc())
            return {"status": "error", "message": f"Failed to set reminder: {str(e)}"}

def get_pending_reminders() -> list:
    """Retrieves all pending reminders that are due or overdue."""
    with ConnectionPool.get_connection() as conn:
        cursor = conn.cursor()
        current_time_utc = datetime.now(pytz.UTC)  # Current UTC time for comparison
        
        reminders_to_send = []
        
        try:
            # Select all unsent reminders
            cursor.execute("SELECT id, user_id, reminder_text, scheduled_time, reminder_type, original_timezone_str, is_sent FROM reminders WHERE is_sent = 0")
            for row in cursor.fetchall():
                reminder_id, user_id, reminder_text, scheduled_time_str, reminder_type, original_timezone_str, is_sent = row
                
                try:
                    # Determine the timezone to parse the scheduled_time_str
                    user_tz_str = original_timezone_str if original_timezone_str else "UTC"  # Use stored timezone or default
                    user_tz = pytz.timezone(user_tz_str)
                    
                    # Parse the scheduled time string using the expected format (without seconds)
                    scheduled_time_naive = datetime.strptime(scheduled_time_str, "%Y-%m-%d %H:%M")
                    
                    # Localize the naive datetime object to the user's timezone
                    scheduled_time_local = user_tz.localize(scheduled_time_naive)
                    
                    # Convert this local time to UTC for comparison with current_time_utc
                    scheduled_time_utc = scheduled_time_local.astimezone(pytz.UTC)
                    
                    # If the scheduled UTC time is less than or equal to the current UTC time, it's due
                    if scheduled_time_utc <= current_time_utc:
                        reminders_to_send.append({
                            "id": reminder_id,
                            "user_id": user_id,
                            "reminder_text": reminder_text,
                            "scheduled_time_utc": scheduled_time_utc,
                            "reminder_type": reminder_type,
                            "is_sent": is_sent
                        })
                except ValueError as ve:  # Catch specific parsing errors
                    logger.error(f"SKIPPING: Datetime format mismatch for reminder ID {reminder_id} ('{scheduled_time_str}'). Error: {ve}")
                    continue  # Skip this reminder and move to the next one
                except Exception as e:  # Catch other general errors during processing
                    logger.error(f"Error processing reminder ID {reminder_id} for user {user_id}: {e}")
                    logger.error(traceback.format_exc())
                    mark_reminder_sent(reminder_id)  # Mark as sent to avoid repeated errors
                    continue  # Skip this reminder and move to the next one
                    
            logger.debug(f"Found {len(reminders_to_send)} pending reminders.")
            return reminders_to_send
        except Exception as e:
            logger.error(f"Error getting pending reminders: {e}")
            logger.error(traceback.format_exc())
            return []
        
def mark_reminder_sent(reminder_id: int) -> bool:
    """Marks a reminder as sent."""
    try:
        logger.info(f"mark_reminder_sent called with reminder_id={reminder_id} (type: {type(reminder_id)})")
        reminder_id = int(reminder_id)
    except Exception as type_e:
        logger.error(f"reminder_id type error: {type_e} (value: {reminder_id})")
        return False
    with ConnectionPool.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE reminders SET is_sent = 1 WHERE id = ? AND is_sent = 0", (reminder_id,))
            conn.commit()
            logger.info(f"UPDATE reminders SET is_sent = 1 WHERE id = {reminder_id} AND is_sent = 0: rowcount={cursor.rowcount}")
            if cursor.rowcount > 0:
                logger.info(f"Reminder ID {reminder_id} marked as sent.")
                return True
            else:
                logger.warning(f"Reminder ID {reminder_id} was already marked as sent or not found.")
                return False
        except Exception as e:
            logger.error(f"Error marking reminder {reminder_id} as sent: {e}")
            logger.error(traceback.format_exc())
            return False

def get_user_reminders(user_id: str) -> list:
    """Retrieves all active (not yet sent) reminders for a specific user."""
    with ConnectionPool.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT id, reminder_text, scheduled_time FROM reminders WHERE user_id = ? AND is_sent = 0 ORDER BY scheduled_time ASC",
                (user_id,)
            )
            reminders_raw = cursor.fetchall()
            
            user_tz_str = get_user_timezone(user_id)
            user_tz = pytz.timezone(user_tz_str)
            
            formatted_reminders = []
            for r_id, r_text, r_scheduled_time_str in reminders_raw:
                try:
                    formatted_reminders.append({
                        "id": r_id,
                        "text": r_text,
                        "scheduled_time": r_scheduled_time_str # Display as originally set
                    })
                except Exception as e:
                    logger.error(f"Error formatting reminder {r_id} for display: {e}")
                    logger.error(traceback.format_exc())
                    formatted_reminders.append({
                        "id": r_id,
                        "text": r_text,
                        "scheduled_time": "Error parsing time",
                        "error": str(e)
                    })

            logger.info(f"Retrieved {len(formatted_reminders)} active reminders for user {user_id}.")
            return formatted_reminders
        except Exception as e:
            logger.error(f"Error getting user reminders for {user_id}: {e}")
            logger.error(traceback.format_exc())
            return []

def cancel_all_reminders(user_id: str) -> dict:
    """Cancels all active (not yet sent) reminders for a user."""
    with ConnectionPool.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE reminders SET is_sent = 1 WHERE user_id = ? AND is_sent = 0", (user_id,))
            conn.commit()
            rows_affected = cursor.rowcount
            logger.info(f"Cancelled {rows_affected} reminders for user {user_id}.")
            if rows_affected > 0:
                return {"status": "success", "message": f"All {rows_affected} active reminders cancelled."}
            else:
                return {"status": "not_found", "message": "No active reminders found to cancel."}
        except Exception as e:
            logger.error(f"Error cancelling all reminders for {user_id}: {e}")
            logger.error(traceback.format_exc())
            return {"status": "error", "message": f"Failed to cancel all reminders: {str(e)}"}

def cancel_reminder(user_id: str, reminder_id: int) -> dict:
    """Cancels a specific reminder by its ID for a user."""
    with ConnectionPool.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE reminders SET is_sent = 1 WHERE id = ? AND user_id = ? AND is_sent = 0", (reminder_id, user_id))
            conn.commit()
            if cursor.rowcount > 0:
                logger.info(f"Cancelled reminder ID {reminder_id} for user {user_id}.")
                return {"status": "success", "message": f"Reminder {reminder_id} cancelled."}
            else:
                logger.warning(f"Reminder ID {reminder_id} not found or already cancelled for user {user_id}.")
                return {"status": "not_found", "message": f"Reminder {reminder_id} not found or already cancelled."}
        except Exception as e:
            logger.error(f"Error cancelling reminder {reminder_id} for {user_id}: {e}")
            logger.error(traceback.format_exc())
            return {"status": "error", "message": f"Failed to cancel reminder {reminder_id}: {str(e)}"}

# --- Message Processing Cache ---
MAX_CACHE_SIZE = 1000
processed_messages = OrderedDict()

def clean_old_messages():
    """Remove old messages from the cache."""
    current_time = time.time()
    for msg_id in list(processed_messages.keys()):
        if current_time - processed_messages[msg_id] > 3600:  # 1 hour expiration
            processed_messages.pop(msg_id)

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

def ensure_database_schema():
    """Ensures all required tables exist with the correct schema."""
    with ConnectionPool.get_connection() as conn:
        cursor = conn.cursor()
        try:
            # User memory table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_memory (
                    user_id TEXT,
                    key TEXT,
                    value TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, key)
                )
            """)
            
            # Add last_updated column if it doesn't exist
            cursor.execute("PRAGMA table_info(user_memory)")
            columns = [row[1] for row in cursor.fetchall()]
            if 'last_updated' not in columns:
                cursor.execute("ALTER TABLE user_memory ADD COLUMN last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            
            # User timezones table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_timezones (
                    user_id TEXT PRIMARY KEY,
                    timezone TEXT NOT NULL,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Add last_updated column if it doesn't exist
            cursor.execute("PRAGMA table_info(user_timezones)")
            columns = [row[1] for row in cursor.fetchall()]
            if 'last_updated' not in columns:
                cursor.execute("ALTER TABLE user_timezones ADD COLUMN last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            
            conn.commit()
            logger.info("Database schema checked and updated successfully")
            
        except Exception as e:
            logger.error(f"Error ensuring database schema: {e}")
            logger.error(traceback.format_exc())
            raise