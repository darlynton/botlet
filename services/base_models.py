"""Base models and shared functionality."""
import logging
import sqlite3
import os
import threading
from datetime import datetime
import pytz
from typing import Optional
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Database configuration
DATABASE_NAME = 'bot_data.db'

class ConnectionPool:
    """Manages a pool of database connections."""
    _pool = []
    _max_connections = 5
    _lock = threading.Lock()
    _last_log_time = 0
    _log_interval = 60
    _initialized = False
    _active_connections = set()

    @classmethod
    def initialize(cls):
        """Initialize the connection pool."""
        if not cls._initialized:
            with cls._lock:
                if not cls._initialized:
                    for _ in range(cls._max_connections):
                        conn = sqlite3.connect(DATABASE_NAME)
                        cls._pool.append(conn)
                    cls._initialized = True

    @classmethod
    def get_connection(cls):
        """Get a connection from the pool."""
        if not cls._initialized:
            cls.initialize()
        
        with cls._lock:
            if cls._pool:
                conn = cls._pool.pop()
                cls._active_connections.add(conn)
                return ConnectionContextManager(cls, conn)
            else:
                conn = sqlite3.connect(DATABASE_NAME)
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
    """Context manager for database connections."""
    def __init__(self, pool, connection):
        self.pool = pool
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.return_connection(self.connection)

class ReminderNotifier:
    """A simple event notifier for reminders."""
    def __init__(self):
        self.listeners = []

    def add_listener(self, listener):
        """Adds a listener to be notified."""
        self.listeners.append(listener)

    def notify(self):
        """Notifies all registered listeners."""
        for listener in self.listeners:
            if hasattr(listener, 'notify_new_reminder'):
                listener.notify_new_reminder()

def get_user_timezone(user_id: str) -> str:
    """Retrieves a user's stored timezone."""
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
                try:
                    pytz.timezone(result[0])
                    return result[0]
                except pytz.exceptions.UnknownTimeZoneError:
                    logger.error(f"Invalid timezone stored for user {user_id}: {result[0]}")
            
            default_tz = 'Europe/London'
            logger.info(f"No valid timezone found for user {user_id}. Using default: {default_tz}")
            return default_tz
        except Exception as e:
            logger.error(f"Error retrieving timezone for {user_id}: {e}")
            return 'Europe/London'  # Fallback on error

# Global reminder notifier instance
reminder_notifier = ReminderNotifier()
