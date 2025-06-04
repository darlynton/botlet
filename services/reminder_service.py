import threading
import time
from datetime import datetime, timedelta
import pytz
import sqlite3
import logging
from services.db_services import (
    get_pending_reminders, mark_reminder_sent, 
    get_user_timezone, convert_to_user_timezone,
    ConnectionPool, add_reminder
)
from services.whatsapp_service import send_whatsapp_message
from services.base_models import ReminderNotifier, reminder_notifier
from services.logger_config import whatsapp_logger

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReminderService:
    def __init__(self):
        self.stop_event = threading.Event()
        self.thread = None
        self.has_pending_reminders = threading.Event()
        self._last_log_time = 0
        self._log_interval = 60  # Log only every 60 seconds when idle
        reminder_notifier.add_listener(self)  # Register as a listener

    def start(self):
        """Starts the reminder checking service."""
        if not self.thread or not self.thread.is_alive():
            self.stop_event.clear()
            self.thread = threading.Thread(target=self._check_reminders)
            self.thread.daemon = True
            self.thread.start()

    def stop(self):
        """Stops the reminder checking service."""
        if self.thread and self.thread.is_alive():
            print("Stopping reminder service...")
            self.stop_event.set()
            try:
                # Wake up the thread if it's sleeping
                self.has_pending_reminders.set()
                # Wait for the thread to finish with a timeout
                self.thread.join(timeout=2.0)
                if self.thread.is_alive():
                    print("Warning: Reminder service thread did not stop gracefully")
            except Exception as e:
                print(f"Error stopping reminder service: {e}")

    def notify_new_reminder(self):
        """Notify the service that a new reminder has been added."""
        self.has_pending_reminders.set()

    def _check_for_pending_reminders(self) -> bool:
        """Check if there are any pending (unsent AND due) reminders in the database."""
        try:
            with ConnectionPool.get_connection() as conn:
                cursor = conn.cursor()
                now_utc = datetime.now(pytz.utc) # Get current time in UTC
                
                # The SQL query should be a clean string, with parameters passed separately
                cursor.execute('''
                    SELECT COUNT(*) FROM reminders
                    WHERE is_sent = 0 AND scheduled_time <= ?
                ''', (now_utc.isoformat(),)) # Pass the UTC time as an ISO formatted string parameter
                
                count = cursor.fetchone()[0]
                return count > 0
        except Exception as e:
            print(f"Error checking for pending reminders: {str(e)}")
            import traceback
            traceback.print_exc()
            return False # Return False on error

    def _check_reminders(self):
        """Background task to check and handle reminders."""
        while not self.stop_event.is_set():
            try:
                # Check if there are any pending reminders
                if not self._check_for_pending_reminders():
                    if self._should_log():
                        print("No pending reminders. Service going to sleep...")
                    # Clear the flag and wait for new reminders
                    self.has_pending_reminders.clear()
                    # Wait until either a new reminder is added or stop is called
                    self.has_pending_reminders.wait(timeout=60)  # Check every minute anyway
                    continue

                # Get all pending reminders that are due
                due_reminders = get_pending_reminders()
                current_time = datetime.now(pytz.UTC)
                
                if self._should_log():
                    print(f"Checking reminders at {current_time} UTC")
                
                for reminder in due_reminders:
                    if self.stop_event.is_set():
                        break
                        
                    # Ensure the reminder is not already marked as sent
                    if reminder.get("is_sent", 0) == 1:
                        if self._should_log():
                            print(f"Skipping already sent reminder ID {reminder['id']}")
                        continue

                    # Get user's timezone for formatting the message
                    user_tz = get_user_timezone(reminder["user_id"])
                    local_time = current_time.astimezone(pytz.timezone(user_tz))
                    
                    if self._should_log():
                        print(f"Processing reminder {reminder['id']} for user {reminder['user_id']}")
                        print(f"Reminder scheduled for: {reminder['scheduled_time']} in {user_tz}")
                        print(f"Current local time: {local_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                    
                    # Format the reminder message with local time
                    if reminder["reminder_type"] == "reminder":
                        message = f"🔔 *Reminder ({local_time.strftime('%I:%M %p %Z')})*: {reminder['reminder_text']}"
                    else:  # scheduled_message
                        message = reminder['reminder_text']

                    if self._should_log():
                        print(f"Sending message: {message}")

                    send_result = None
                    try:
                        # Log before sending
                        whatsapp_logger.info(f"[ReminderService] Attempting to send WhatsApp reminder to {reminder['user_id']} (reminder ID: {reminder['id']})")
                        # Send the reminder via WhatsApp
                        send_result = send_whatsapp_message(reminder["user_id"], message)
                        if send_result and send_result.get("status") == "success":
                            whatsapp_logger.info(f"Reminder sent successfully. ID: {reminder['id']}")
                        else:
                            whatsapp_logger.warning(f"Failed to send reminder {reminder['id']}. Result: {send_result}")
                    except Exception as e:
                        whatsapp_logger.error(f"Exception while sending reminder {reminder['id']}: {e}")
                    finally:
                        whatsapp_logger.info(f"Marking reminder as sent (regardless of send result). ID: {reminder['id']}")
                        mark_result = mark_reminder_sent(reminder["id"])
                        whatsapp_logger.info(f"Tried to mark reminder {reminder['id']} as sent, result: {mark_result}")
                        if mark_result:
                            if self._should_log():
                                print(f"Marked reminder {reminder['id']} as complete")
                        else:
                            logger.warning(f"Failed to mark reminder {reminder['id']} as sent. It may be sent again!")
            except Exception as e:
                print(f"Error in reminder service: {str(e)}")
                import traceback
                traceback.print_exc()
            
            # Wait for a shorter interval when actively checking reminders
            self.has_pending_reminders.wait(timeout=30)

    def _get_last_user_interaction(self, user_id: str) -> datetime:
        """
        Get the timestamp of the user's last interaction.
        This should be implemented to track when users last sent messages.
        """
        # TODO: Implement this to track actual user interactions
        # For now, we'll assume all reminders are outside the window
        return None

    def _should_log(self):
        """Determine if we should log based on the throttling interval."""
        current_time = time.time()
        if current_time - self._last_log_time >= self._log_interval:
            self._last_log_time = current_time
            return True
        return False

# DEPRECATED: Do not use this function. Use set_reminder from ai_engine.py for all reminder creation.
# def set_reminder(reminder_text: str, minutes_from_now: int = None, specific_time_str: str = None, user_id: str = None) -> dict:
#     """Sets a reminder for the user."""
#     if not user_id:
#         return {
#             "status": "error",
#             "message": "User ID is required to set a reminder but was not provided."
#         }
#     logger.info(f"Setting reminder for user {user_id}: '{reminder_text}'")

#     # Get user's timezone
#     user_tz_str = get_user_timezone(user_id)
#     if not user_tz_str:
#         user_tz_str = "UTC"  # Default to UTC if not set

#     user_tz = pytz.timezone(user_tz_str)
#     now_in_user_tz = datetime.now(user_tz)

#     # Determine the scheduled time
#     scheduled_local_time = None
#     if minutes_from_now is not None:
#         scheduled_local_time = now_in_user_tz + timedelta(minutes=minutes_from_now)
#     elif specific_time_str:
#         try:
#             # Attempt to parse specific_time_str as HH:MM AM/PM or 24-hour format
#             try:
#                 time_obj = datetime.strptime(specific_time_str, "%I:%M %p").time()
#             except ValueError:
#                 time_obj = datetime.strptime(specific_time_str, "%H:%M").time()
            
#             scheduled_local_time = now_in_user_tz.replace(
#                 hour=time_obj.hour,
#                 minute=time_obj.minute,
#                 second=0, microsecond=0
#             )
#             if scheduled_local_time <= now_in_user_tz:
#                 scheduled_local_time += timedelta(days=1)
#         except ValueError:
#             logger.warning(f"Could not parse specific_time_str '{specific_time_str}' with either HH:MM AM/PM or 24-hour format.")
#             return {"status": "error", "message": "Unsupported time format. Please use 'HH:MM AM/PM' or 'HH:MM' (e.g., '7:30 PM' or '19:30')."}
#     else:
#         return {
#             "status": "error",
#             "message": (
#                 "I didn't catch when you'd like to be reminded. "
#                 "Just let me know what you'd like the reminder to say and when (e.g., \"in 10 minutes\", \"tomorrow at 3 PM\", or \"on December 25th at 9 AM\")."
#             )
#         }

#     if scheduled_local_time is None:
#         return {"status": "error", "message": "Could not determine reminder time after parsing."}

#     # Convert the determined local time to UTC for storage
#     if scheduled_local_time.tzinfo is None:
#         scheduled_local_time = user_tz.localize(scheduled_local_time)

#     scheduled_utc_time = scheduled_local_time.astimezone(pytz.UTC)

#     # Add reminder to the database
#     result = add_reminder(
#         user_id=user_id,
#         reminder_text=reminder_text,
#         scheduled_time=scheduled_local_time.strftime("%Y-%m-%d %H:%M"),
#         original_timezone_str=user_tz_str
#     )

#     if result.get("status") == "success":
#         reminder_notifier.notify()
#         display_time_str = scheduled_local_time.strftime("%I:%M %p")
#         display_date_str = scheduled_local_time.strftime("%A, %B %d, %Y")
#         today_in_user_tz = now_in_user_tz.date()
#         scheduled_date_in_user_tz = scheduled_local_time.date()

#         day_description = "today" if scheduled_date_in_user_tz == today_in_user_tz else (
#             "tomorrow" if scheduled_date_in_user_tz == today_in_user_tz + timedelta(days=1) else f"on {display_date_str}"
#         )

#         return {
#             "status": "success",
#             "message": f"Okay, I've set a reminder for you!\n\n*Reminder:* {reminder_text}\n*Time:* {day_description} at {display_time_str} ({user_tz_str})"
#         }
#     else:
#         return {"status": "error", "message": f"Failed to set reminder: {result.get('message', 'Unknown error')}"}

# Create a global instance of the reminder service
reminder_service = ReminderService()