"""Shared utilities for reminder functionality."""
import sqlite3
import logging
from datetime import datetime, timedelta
import pytz
from typing import Optional, List, Dict, Any
from services.base_models import ConnectionPool, get_user_timezone, reminder_notifier

logger = logging.getLogger(__name__)

def ensure_reminders_table():
    """Ensure the reminders table exists in the database."""
    try:
        with ConnectionPool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS reminders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    reminder_text TEXT NOT NULL,
                    scheduled_time TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'pending',
                    attempts INTEGER DEFAULT 0,
                    last_attempt TIMESTAMP
                )
            """)
            conn.commit()
            logger.info("Reminders table initialized")
    except Exception as e:
        logger.error(f"Error ensuring reminders table: {str(e)}")
        raise

def create_reminder(user_id: str, reminder_text: str, scheduled_time: datetime) -> Dict[str, Any]:
    """Create a new reminder in the database."""
    try:
        with ConnectionPool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO reminders 
                (user_id, reminder_text, scheduled_time) 
                VALUES (?, ?, ?)
            """, (user_id, reminder_text, scheduled_time.isoformat()))
            
            reminder_id = cursor.lastrowid
            conn.commit()
            
            reminder_notifier.notify()
            
            return {
                "success": True,
                "reminder_id": reminder_id,
                "reminder_text": reminder_text,
                "scheduled_time": scheduled_time.isoformat()
            }
    except Exception as e:
        logger.error(f"Error creating reminder: {str(e)}")
        return {"success": False, "error": str(e)}

def add_reminder(user_id: str, reminder_text: str, minutes_from_now: int) -> Dict[str, Any]:
    """Add a new reminder for a user, scheduled minutes from now."""
    try:
        # Get user's timezone
        user_tz_str = get_user_timezone(user_id)
        user_tz = pytz.timezone(user_tz_str) if user_tz_str else pytz.UTC
        
        # Calculate scheduled time in user's timezone
        now_in_user_tz = datetime.now(user_tz)
        scheduled_time = now_in_user_tz + timedelta(minutes=minutes_from_now)
        
        return create_reminder(user_id, reminder_text, scheduled_time)
        
    except Exception as e:
        logger.error(f"Error adding reminder: {str(e)}")
        return {"success": False, "error": str(e)}

def get_user_reminders(user_id: str) -> List[Dict[str, Any]]:
    """Get all pending reminders for a user."""
    try:
        with ConnectionPool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, reminder_text, scheduled_time, status 
                FROM reminders 
                WHERE user_id = ? 
                AND status = 'pending'
                ORDER BY scheduled_time ASC
            """, (user_id,))
            
            reminders = []
            for row in cursor.fetchall():
                reminder_id, text, scheduled_time, status = row
                reminders.append({
                    "reminder_id": reminder_id,
                    "text": text,
                    "scheduled_time": scheduled_time,
                    "status": status
                })
            
            return reminders
    except Exception as e:
        logger.error(f"Error getting reminders: {str(e)}")
        return []

def cancel_reminder(reminder_id: int) -> Dict[str, Any]:
    """Cancel a specific reminder by ID."""
    try:
        with ConnectionPool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE reminders 
                SET status = 'cancelled' 
                WHERE id = ? AND status = 'pending'
            """, (reminder_id,))
            conn.commit()
            
            if cursor.rowcount > 0:
                return {"success": True, "message": f"Reminder {reminder_id} cancelled"}
            return {"success": False, "error": "Reminder not found or already cancelled"}
    except Exception as e:
        logger.error(f"Error cancelling reminder: {str(e)}")
        return {"success": False, "error": str(e)}

def cancel_all_reminders(user_id: str) -> Dict[str, Any]:
    """Cancel all pending reminders for a user."""
    try:
        with ConnectionPool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE reminders 
                SET status = 'cancelled' 
                WHERE user_id = ? AND status = 'pending'
            """, (user_id,))
            conn.commit()
            
            count = cursor.rowcount
            return {
                "success": True,
                "message": f"Cancelled {count} reminder{'s' if count != 1 else ''}"
            }
    except Exception as e:
        logger.error(f"Error cancelling all reminders: {str(e)}")
        return {"success": False, "error": str(e)}