import os
import logging
import json
import traceback
import sqlite3
import hashlib
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv
from typing import Optional, Dict, Any, List

import vertexai
from vertexai.generative_models import (
    GenerativeModel, 
    GenerationConfig,
    Part,
    Content,
    Tool,
    FunctionDeclaration,
)
from vertexai.language_models import ChatModel, InputOutputTextPair
from google.cloud import aiplatform
from services.message_queue import MessageQueue
from services.db_services import (
    store_user_memory, retrieve_user_memory, delete_user_memory,
    get_user_timezone, set_user_timezone,
    store_session, retrieve_conversation_history
)
from services.reminder_utils import (
    add_reminder, get_user_reminders, cancel_reminder,
    cancel_all_reminders, ensure_reminders_table
)
from services.base_models import reminder_notifier

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file at the very beginning
load_dotenv()

# --- Configuration ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
REGION = "us-central1"
MODEL_NAME = "gemini-2.5-flash-preview-05-20"
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY").strip() if os.getenv("OPENWEATHER_API_KEY") else None
SEARCH_API_KEY = os.getenv("Search_API_KEY").strip() if os.getenv("Search_API_KEY") else None
GOOGLE_CSE_ID = os.getenv("GOOGLE_CSE_ID").strip() if os.getenv("GOOGLE_CSE_ID") else None

DB_NAME = "bot_data.db"

# Import bot instructions
from config.bot_instructions import get_system_instructions

# Cache system instructions
SYSTEM_INSTRUCTIONS = get_system_instructions()

# Initialize Vertex AI and model
try:
    vertexai.init(project=PROJECT_ID, location=REGION)
    logger.info("Initializing Vertex AI model...")
    
    generation_config = {
        "temperature": 0.7,
        "top_p": 0.95,
        "top_k": 40,
        "candidate_count": 1,
        "max_output_tokens": 2048,
        "stop_sequences": [],
    }
    
    from vertexai.generative_models._generative_models import HarmCategory, HarmBlockThreshold
    
    safety_settings = {
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
    }
    
    model = GenerativeModel(
        MODEL_NAME,
        generation_config=generation_config,
        safety_settings=safety_settings,
    )
    logger.info("Successfully initialized Vertex AI model")
except Exception as e:
    logger.error(f"Failed to initialize Vertex AI model: {str(e)}")
    logger.error(traceback.format_exc())
    raise

# Initialize database tables
ensure_reminders_table()

def get_current_weather(location: str) -> dict:
    """Gets the current weather conditions for a specified location."""
    if not OPENWEATHER_API_KEY:
        return {"error": f"Weather API key is not set"}

    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": location,
        "appid": OPENWEATHER_API_KEY,
        "units": "metric"
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        if data.get("cod") not in [200, "200"]:
            return {"error": f"OpenWeatherMap API error: {data.get('message', 'Unknown error')}"}

        return {
            "location": data.get("name"),
            "temperature_celsius": data['main']['temp'],
            "feels_like_celsius": data['main']['feels_like'],
            "description": data['weather'][0]['description'],
            "humidity": data['main']['humidity'],
            "wind_speed_mps": data['wind']['speed']
        }

    except Exception as e:
        return {"error": f"Weather API error: {str(e)}"}

def _extract_structured_info(search_results: dict, query: str) -> dict:
    """Extract and structure information from search results based on query type."""
    info = {
        "found": False,
        "title": None,
        "main_content": None,
        "details": [],
        "timestamp": None,
        "source": None,
        "category": None
    }
    
    # Determine the category of information we're looking for
    query_lower = query.lower()
    if any(term in query_lower for term in ["news", "latest", "update", "recent"]):
        info["category"] = "news"
    elif any(term in query_lower for term in ["score", "result", "vs", "match", "game"]):
        info["category"] = "event_result"
    elif any(term in query_lower for term in ["weather", "temperature", "forecast"]):
        info["category"] = "weather"
    elif any(term in query_lower for term in ["price", "cost", "worth", "value"]):
        info["category"] = "price"
    else:
        info["category"] = "general"

    # Look through all available sources
    for source in search_results.get("sources", []):
        if not info["found"]:
            info["found"] = True
            info["source"] = source.get("link")
            info["title"] = source.get("title")

        # Extract timestamp/date information if available
        if "date" in source.get("title", "").lower() or "date" in source.get("snippet", "").lower():
            info["timestamp"] = source.get("snippet")

    # Process highlights for main content and details
    relevant_content = []
    for highlight in search_results.get("highlights", []):
        # Skip navigation elements and very short snippets
        if len(highlight.split()) > 3 and not any(nav in highlight.lower() for nav in ["menu", "navigation", "click", "search"]):
            relevant_content.append(highlight)
    
    if relevant_content:
        info["main_content"] = relevant_content[0]  # Most relevant highlight
        info["details"].extend(relevant_content[1:])  # Additional details

    # Add information from news articles
    for article in search_results.get("news_articles", []):
        if not info["found"]:
            info["found"] = True
            info["source"] = article.get("url")
            info["title"] = article.get("title")
            info["timestamp"] = article.get("publishedAt")
        
        if article.get("description"):
            if not info["main_content"]:
                info["main_content"] = article["description"]
            else:
                info["details"].append(article["description"])

    return info

def _format_search_response(search_results: dict, query: str) -> str:
    """Format search results into a natural response based on query type."""
    
    # No results case
    if not search_results.get("highlights") and not search_results.get("news_articles"):
        return "I couldn't find any relevant information about that."
    
    # Extract structured information
    info = _extract_structured_info(search_results, query)
    if not info["found"]:
        return "I couldn't find any relevant information about that."

    response = ""
    
    # Format response based on category
    if info["category"] == "news":
        if info["title"]:
            response += f"{info['title']}\n\n"
        if info["timestamp"]:
            response += f"As of {info['timestamp']}\n"
        if info["main_content"]:
            response += f"{info['main_content']}\n"
        if info["details"]:
            response += "\nAdditional Details:\n"
            response += "\n".join(f"• {detail}" for detail in info["details"][:2])
    
    elif info["category"] == "event_result":
        if info["title"]:
            response += f"{info['title']}\n"
        if info["timestamp"]:
            response += f"Date: {info['timestamp']}\n"
        if info["main_content"]:
            response += f"\n{info['main_content']}"
        if info["details"]:
            response += "\n\nKey Details:\n"
            response += "\n".join(f"• {detail}" for detail in info["details"][:3])
    
    else:  # general information
        if info["main_content"]:
            response += info["main_content"] + "\n"
        if info["details"]:
            response += "\nAdditional Information:\n"
            response += "\n".join(f"• {detail}" for detail in info["details"][:2])
    
    # Add source attribution
    if info["source"]:
        response += f"\n\nSource: {info['source']}"
    
    return response.strip()

def search_google(query: str) -> dict:
    """Performs an enhanced search using multiple data sources with caching."""
    print(f"\nProcessing search query: {query}")
    conn = None # Initialize conn to None for finally block
    try:
        # Check cache first
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # Ensure cache table exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS search_cache (
                query_hash TEXT PRIMARY KEY,
                query TEXT,
                results TEXT,
                source TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                expiry DATETIME
            )
        ''')
        conn.commit()
        print("Cache table ensured")
        
        # Generate query hash
        query_hash = hashlib.md5(query.encode()).hexdigest()
        
        # Check cache for existing results
        cursor.execute('''
            SELECT results FROM search_cache
            WHERE query_hash = ? AND expiry > datetime('now')
        ''', (query_hash,))
        
        cached_result = cursor.fetchone()
        if cached_result:
            print("Found cached results")
            return json.loads(cached_result[0])
            
        print("No cache found, performing live search")
        
        # If not in cache, perform searches
        results = {
            "summary": "Based on the search results, here are some highlights:",
            "highlights": [],
            "sources": [],
            "news_articles": []
        }
        
        # Google Search (This IF statement should be inside the main try block)
        if SEARCH_API_KEY and GOOGLE_CSE_ID:
            print("Performing Google Search...")
            try:
                url = "https://www.googleapis.com/customsearch/v1"
                params = {
                    "key": SEARCH_API_KEY,
                    "cx": GOOGLE_CSE_ID,
                    "q": query,
                    "num": 5
                }
                response = requests.get(url, params=params, timeout=5)
                response.raise_for_status()
                search_data = response.json()

                if "items" in search_data:
                    for item in search_data["items"]:
                        snippet = item.get("snippet", "").replace("\xa0", " ").strip()
                        if snippet:
                            results["highlights"].append(snippet)
                        results["sources"].append({
                            "title": item.get("title", "").strip(),
                            "link": item.get("link", "").strip()
                        })
                    print(f"Found {len(results['highlights'])} Google search results")
                else:
                    print("No Google search results found")
            except Exception as e:
                print(f"Search error for Google Search: {str(e)}")
                print("Full error details:")
                traceback.print_exc()
            # There should be no 'else' directly after the inner try-except for the API keys
        else: # This else belongs to the 'if SEARCH_API_KEY and GOOGLE_CSE_ID:'
            print("Google Search API keys not configured")
        
        # News API Search
        NEWS_API_KEY = os.getenv("NEWS_API_KEY")
        if NEWS_API_KEY:
            print("Performing News API Search...")
            try:
                news_url = "https://newsapi.org/v2/everything"
                news_params = {
                    "q": query,
                    "apiKey": NEWS_API_KEY,
                    "sortBy": "publishedAt",
                    "language": "en",
                    "pageSize": 5
                }
                
                news_response = requests.get(news_url, params=news_params, timeout=5)
                news_response.raise_for_status()
                news_data = news_response.json()
                
                if news_data.get("status") == "ok" and "articles" in news_data:
                    for article in news_data["articles"]:
                        results["news_articles"].append({
                            "title": article["title"],
                            "description": article["description"],
                            "url": article["url"],
                            "published_at": article["publishedAt"],
                            "source": article["source"]["name"]
                        })
                    print(f"Found {len(results['news_articles'])} news articles")
                else:
                    print("No news results found")
            except Exception as e:
                print(f"News API search error: {str(e)}")
                traceback.print_exc()
        else:
            print("News API key not configured")
        
        # Format the response based on query type
        results["formatted_response"] = _format_search_response(results, query)
        
        # Store in cache with 5-minute expiry for general searches, 30 minutes for news
        expiry = datetime.now() + timedelta(minutes=30 if NEWS_API_KEY else 5)
        cursor.execute('''
            INSERT OR REPLACE INTO search_cache 
            (query_hash, query, results, source, expiry)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            query_hash,
            query,
            json.dumps({"results": results}),
            "combined",
            expiry.strftime("%Y-%m-%d %H:%M:%S")
        ))
        conn.commit()
        print("Results cached successfully")
        
        return {"results": results}

    except Exception as e: # This except block is now correctly associated with the main try
        print(f"Overall search_google error: {str(e)}")
        print("Full error details:")
        traceback.print_exc()
        return {"error": f"Overall search error: {str(e)}"}
    finally: # This finally block is now correctly associated with the main try
        if conn:
            conn.close()
            print("Database connection closed")

def get_current_datetime_info(user_id: str = None) -> dict:
    """Gets detailed information about current date, time, and timezone."""
    now_utc = datetime.now(pytz.UTC)
    print(f"Debug: Getting datetime info for user {user_id}")
    
    if user_id:
        user_tz = get_user_timezone(user_id)
        print(f"Debug: User timezone is {user_tz}")
        try:
            tz = pytz.timezone(user_tz)
            now_local = now_utc.astimezone(tz)
            print(f"Debug: Converted time is {now_local}")
            return {
                "datetime": now_local.strftime("%Y-%m-%d %H:%M"),
                "date": now_local.strftime("%Y-%m-%d"),
                "time": now_local.strftime("%I:%M %p"),  # 12-hour format with AM/PM
                "day": now_local.strftime("%A"),
                "timezone": user_tz,
                "timezone_offset": now_local.strftime("%z"),
                "timezone_name": now_local.tzname()  # Get the current timezone name (e.g., BST during summer)
            }
        except Exception as e:
            print(f"Debug: Error converting timezone: {e}")
            # Fall back to UTC if there's any error
            return {
                "datetime": now_utc.strftime("%Y-%m-%d %H:%M"),
                "date": now_utc.strftime("%Y-%m-%d"),
                "time": now_utc.strftime("%I:%M %p"),
                "day": now_utc.strftime("%A"),
                "timezone": "UTC",
                "timezone_offset": "+0000",
                "timezone_name": "UTC"
            }
    else:
        return {
            "datetime": now_utc.strftime("%Y-%m-%d %H:%M"),
            "date": now_utc.strftime("%Y-%m-%d"),
            "time": now_utc.strftime("%I:%M %p"),
            "day": now_utc.strftime("%A"),
            "timezone": "UTC",
            "timezone_offset": "+0000",
            "timezone_name": "UTC"
        }

import pytz # Make sure pytz is imported if not already


def set_reminder(user_id: str, reminder_text: str, minutes_from_now: int = None, specific_time_str: str = None) -> dict:
    """Sets a reminder for the user."""
    logger.info(f"Attempting to set reminder for user {user_id}: '{reminder_text}' for {minutes_from_now} minutes from now or at {specific_time_str}")

    # Handle initial reminder requests without time specification
    if reminder_text.lower().strip() in ["set a reminder", "set reminder", "reminder", "can you set a reminder", "can you set a reminder for me"]:
        return {
            "status": "needs_info",
            "message": "I'll help you set a reminder. Please let me know what you'd like to be reminded about and when. For example:\n- 'Remind me to call John in 30 minutes'\n- 'Set a reminder for my meeting tomorrow at 2 PM'\n- 'Remind me to check the oven at 6:30 PM'"
        }

    # Step 1: Determine the scheduled time in UTC
    user_tz_str = get_user_timezone(user_id)  # Get user's stored timezone
    if not user_tz_str:
        user_tz_str = "UTC"  # Default to UTC if not set

    user_tz = pytz.timezone(user_tz_str)
    now_in_user_tz = datetime.now(user_tz)

    scheduled_local_time = None
    if minutes_from_now is not None:
        scheduled_local_time = now_in_user_tz + timedelta(minutes=minutes_from_now)
    elif specific_time_str:
        try:
            # Attempt to parse as HH:MM AM/PM format
            time_obj = datetime.strptime(specific_time_str, "%I:%M %p").time()
            scheduled_local_time = now_in_user_tz.replace(
                hour=time_obj.hour,
                minute=time_obj.minute,
                second=0, microsecond=0
            )
            # If the specified time is in the past for today, assume next day
            if scheduled_local_time <= now_in_user_tz:
                scheduled_local_time += timedelta(days=1)
        except ValueError:
            logger.warning(f"Could not parse specific_time_str '{specific_time_str}' with HH:MM AM/PM format.")
            return {
                "status": "needs_info",
                "message": "I understand you want to set a reminder. Could you please specify when you'd like to be reminded? You can say something like:\n- 'in 30 minutes'\n- 'at 2:30 PM'\n- 'tomorrow at 9 AM'"
            }
    else:
        return {
            "status": "needs_info",
            "message": "I understand you want to set a reminder. Could you please specify when you'd like to be reminded? You can say something like:\n- 'in 30 minutes'\n- 'at 2:30 PM'\n- 'tomorrow at 9 AM'"
        }

    if scheduled_local_time is None:
        return {
            "status": "needs_info",
            "message": "I couldn't determine when to set the reminder. Could you please specify the time in one of these formats?\n- 'in X minutes'\n- 'at HH:MM AM/PM'\n- 'tomorrow at HH:MM AM/PM'"
        }

    # Convert the determined local time to UTC for storage
    if scheduled_local_time.tzinfo is None:
        scheduled_local_time = user_tz.localize(scheduled_local_time)

    scheduled_utc_time = scheduled_local_time.astimezone(pytz.UTC)

    # Step 2: Add reminder to the database
    result = add_reminder(
        user_id=user_id,
        reminder_text=reminder_text,
        scheduled_time=scheduled_local_time.strftime("%Y-%m-%d %H:%M"),  # Use local time for storage
        original_timezone_str=user_tz_str  # Pass the original timezone string
    )

    if result.get("status") == "success":
        reminder_notifier.notify()  # Notify the ReminderService immediately

        # Convert stored time back to user's timezone for display confirmation
        display_time_str = scheduled_local_time.strftime("%I:%M %p")
        display_date_str = scheduled_local_time.strftime("%A, %B %d, %Y")

        # Determine if it's "today" or "tomorrow" for a more natural response
        today_in_user_tz = now_in_user_tz.date()
        scheduled_date_in_user_tz = scheduled_local_time.date()

        day_description = ""
        if scheduled_date_in_user_tz == today_in_user_tz:
            day_description = "today"
        elif scheduled_date_in_user_tz == today_in_user_tz + timedelta(days=1):
            day_description = "tomorrow"
        else:
            day_description = f"on {display_date_str}"

        return {
            "status": "success",
            "message": f"Okay, I've set a reminder for you!\n\n*Reminder:* {reminder_text}\n*Time:* {day_description} at {display_time_str} ({user_tz_str})"
        }
    else:
        return {"status": "error", "message": f"Failed to set reminder: {result.get('message', 'Unknown error')}"}

def list_reminders(user_id: str) -> dict:
    """Lists all pending reminders for a user."""
    reminders = get_user_reminders(user_id)
    return {"reminders": reminders}

def store_timezone(user_id: str, timezone_str: str) -> dict:
    """Stores a user's timezone preference."""
    print(f"Debug: Storing timezone {timezone_str} for user {user_id}")
    # Convert common names to proper timezone strings
    timezone_mapping = {
        "BST": "Europe/London",  # British Summer Time
        "GMT": "Europe/London",  # Greenwich Mean Time
        "EST": "America/New_York",  # Eastern Time
        "EDT": "America/New_York",
        "CST": "America/Chicago",  # Central Time
        "CDT": "America/Chicago",
        "MST": "America/Denver",  # Mountain Time
        "MDT": "America/Denver",
        "PST": "America/Los_Angeles",  # Pacific Time
        "PDT": "America/Los_Angeles",
        "IST": "Asia/Kolkata",  # Indian Standard Time
        "AEST": "Australia/Sydney",  # Australian Eastern Standard Time
        "AEDT": "Australia/Sydney",  # Australian Eastern Daylight Time
        "JST": "Asia/Tokyo",  # Japan Standard Time
        "KST": "Asia/Seoul",  # Korea Standard Time
        "CST": "Asia/Shanghai",  # China Standard Time
    }
    
    # Convert common name to proper timezone if needed
    timezone = timezone_mapping.get(timezone_str.upper(), timezone_str)
    print(f"Debug: Mapped timezone is {timezone}")
    
    try:
        # Validate the timezone
        tz = pytz.timezone(timezone)
        now = datetime.now(tz)
        print(f"Debug: Validated timezone. Current time there would be {now}")
        result = set_user_timezone(user_id, timezone)
        print(f"Debug: Timezone storage result: {result}")
        return result
    except pytz.exceptions.UnknownTimeZoneError:
        print(f"Debug: Invalid timezone: {timezone_str}")
        return {"status": "error", "message": f"Invalid timezone: {timezone_str}"}

# --- TOOL DECLARATIONS ---
get_current_weather_tool_declaration = FunctionDeclaration(
    name="get_current_weather",
    description="Gets current weather conditions for a location",
    parameters={
        "type": "object",
        "properties": {
            "location": {
                "type": "string",
                "description": "The location to get weather for (e.g., 'London' or 'New York')"
            }
        },
        "required": ["location"]
    }
)

get_current_date_tool_declaration = FunctionDeclaration(
    name="get_current_datetime_info",
    description="Gets the current date, time, and timezone information.",
    parameters={
        "type": "object",
        "properties": {},
    },
)

search_google_tool_declaration = FunctionDeclaration(
    name="search_google",
    description="Performs a comprehensive web search using multiple sources including Google and news articles. Results are cached for performance and include both general web results and recent news articles.",
    parameters={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "The search query to find relevant information across web and news sources."
            }
        },
        "required": ["query"],
    },
)

store_user_memory_tool_declaration = FunctionDeclaration(
    name="store_user_memory",
    description="Stores a piece of factual or personal information about the user.",
    parameters={
        "type": "object",
        "properties": {
            "key": {
                "type": "string",
                "description": "A concise, descriptive name for the information."
            },
            "value": {
                "type": "string",
                "description": "The actual information or fact to store."
            }
        },
        "required": ["key", "value"],
    },
)

retrieve_user_memory_tool_declaration = FunctionDeclaration(
    name="retrieve_user_memory",
    description="Retrieves a piece of information previously stored about the user.",
    parameters={
        "type": "object",
        "properties": {
            "key": {
                "type": "string",
                "description": "The key of the information to retrieve."
            }
        },
        "required": ["key"],
    },
)

delete_user_memory_tool_declaration = FunctionDeclaration(
    name="delete_user_memory",
    description="Deletes a specific piece of information about the user.",
    parameters={
        "type": "object",
        "properties": {
            "key": {
                "type": "string",
                "description": "The key of the information to delete."
            }
        },
        "required": ["key"],
    },
)

set_reminder_tool_declaration = FunctionDeclaration(
    name="set_reminder",
    description="Sets a reminder for the user for a specific event. The user_id is automatically handled by the system. When the user asks to set a reminder, ask them for the reminder text and timing if not provided. Accept various time formats like 'in X minutes', 'at HH:MM AM/PM', or 'tomorrow at HH:MM'.",
    parameters={ 
        "type": "object",
        "properties": {
            "user_id": {"type": "string", "description": "The unique identifier for the user."},
            "reminder_text": {"type": "string", "description": "The text of the reminder."},
            "minutes_from_now": {"type": "integer", "description": "The number of minutes from the current time to set the reminder (must be positive). Use this if the user specifies a duration."},
            "specific_time_str": {"type": "string", "description": "The specific date and time for the reminder, e.g., '2025-12-25 09:00' or '14:30'. Use this if the user specifies an exact time or date."},
        },
        "required": ["reminder_text"],
    },
)

list_reminders_tool_declaration = FunctionDeclaration(
    name="list_reminders",
    description="Lists all pending reminders for the user.",
    parameters={
        "type": "object",
        "properties": {},
    },
)

cancel_all_reminders_tool_declaration = FunctionDeclaration(
    name="cancel_all_reminders",
    description="Cancels all pending reminders for the user.",
    parameters={
        "type": "object",
        "properties": {},
    },
)

cancel_reminder_tool_declaration = FunctionDeclaration(
    name="cancel_reminder",
    description="Cancels a specific reminder by its ID.",
    parameters={
        "type": "object",
        "properties": {
            "reminder_id": {
                "type": "integer",
                "description": "The ID of the reminder to cancel"
            }
        },
        "required": ["reminder_id"],
    },
)

store_timezone_tool_declaration = FunctionDeclaration(
    name="store_timezone",
    description="Stores a user's timezone preference, handling common abbreviations like BST, GMT, EST, etc.",
    parameters={
        "type": "object",
        "properties": {
            "timezone_str": {
                "type": "string",
                "description": "The timezone string (e.g., 'BST', 'GMT', 'Europe/London')"
            }
        },
        "required": ["timezone_str"],
    },
)

# --- COMBINED TOOLS ---
ALL_TOOLS = [Tool(
    function_declarations=[
        get_current_weather_tool_declaration,
        get_current_date_tool_declaration,
        search_google_tool_declaration,
        store_user_memory_tool_declaration,
        retrieve_user_memory_tool_declaration,
        delete_user_memory_tool_declaration, 
        set_reminder_tool_declaration,
        list_reminders_tool_declaration,
        cancel_all_reminders_tool_declaration,
        cancel_reminder_tool_declaration,
        store_timezone_tool_declaration,
    ]
)]

def _initialize_chat():
    """Initialize a new chat session with the model."""
    try:
        # System instructions are typically set in the initial prompt or via a dedicated system role
        # It's better to pass history directly to start_chat
        return model # Return the model directly, start_chat is called with history
    except Exception as e:
        logger.error(f"Failed to initialize chat: {e}")
        logger.error(traceback.format_exc())
        return None

def _store_session_async(user_id: str, session_data: dict):
    """Store session data and conversation history asynchronously."""
    try:
        # Get current conversation history
        conversation_history = retrieve_conversation_history(user_id)
        store_session(user_id, session_data, conversation_history)
    except Exception as e:
        logger.error(f"Error storing session: {e}")
        traceback.print_exc()

def generate_response(user_id: str, conversation_history: list, webhook_message_id: str = None) -> dict:
    """Generate a response using the AI model with improved error handling."""
    try:
        # Initialize chat session
        chat = model.start_chat()
        
        # Send system instructions first
        try:
            chat.send_message(SYSTEM_INSTRUCTIONS)
        except Exception as e:
            logger.error(f"Error sending system instructions: {e}")
            logger.error(traceback.format_exc())
        
        # Send previous conversation history
        for message in conversation_history[:-1]:  # Exclude the latest message
            chat.send_message(str(message))
        
        # Send the last message with appropriate tools based on content
        try:
            user_message = str(conversation_history[-1]).lower()
            tools_to_use = None

            # Determine which tools to enable based on message content
            if any(word in user_message for word in ["weather", "temperature", "forecast"]):
                tools_to_use = [Tool(function_declarations=[get_current_weather_tool_declaration])]
            elif any(word in user_message for word in ["remind", "reminder", "schedule"]):
                tools_to_use = [Tool(function_declarations=[
                    set_reminder_tool_declaration,
                    list_reminders_tool_declaration,
                    cancel_reminder_tool_declaration,
                    cancel_all_reminders_tool_declaration
                ])]
            elif any(word in user_message for word in ["timezone", "time zone"]):
                tools_to_use = [Tool(function_declarations=[store_timezone_tool_declaration])]
            elif any(word in user_message for word in ["search", "find", "look up", "tell me about"]):
                tools_to_use = [Tool(function_declarations=[search_google_tool_declaration])]
            elif any(word in user_message for word in ["remember", "memory", "store", "recall", "forget"]):
                tools_to_use = [Tool(function_declarations=[
                    store_user_memory_tool_declaration,
                    retrieve_user_memory_tool_declaration,
                    delete_user_memory_tool_declaration
                ])]

            response = chat.send_message(
                str(conversation_history[-1]),
                tools=tools_to_use if tools_to_use else None
            )
        except Exception as model_e:
            if "safety filters" in str(model_e).lower():
                return {
                    "status": "error",
                    "message": "I apologize, but I cannot process that request due to content safety restrictions."
                }
            raise  # Re-raise other model errors
            
        # Handle function calls if present
        if hasattr(response, 'candidates') and response.candidates:
            candidate = response.candidates[0]
            if hasattr(candidate, 'function_call') and candidate.function_call:
                function_call = candidate.function_call
                
                # Get the corresponding function
                available_functions = {
                    "get_current_weather": get_current_weather,
                    "search_google": search_google,
                    "store_user_memory": store_user_memory,
                    "retrieve_user_memory": retrieve_user_memory,
                    "delete_user_memory": delete_user_memory,
                    "set_reminder": add_reminder,
                    "list_reminders": get_user_reminders,
                    "cancel_all_reminders": cancel_all_reminders,
                    "cancel_reminder": cancel_reminder,
                    "store_timezone": set_user_timezone
                }
                
                func = available_functions.get(function_call.name)
                if func:
                    try:
                        # Execute the function
                        function_args = function_call.args
                        executed_tool_output = func(**function_args)
                        
                        # Send the result back to the model
                        follow_up = chat.send_message(
                            Content(
                                role="assistant",
                                parts=[Part.from_text(json.dumps(executed_tool_output))]
                            )
                        )
                        
                        # Return the final response
                        return {
                            "status": "success",
                            "message": follow_up.text,
                            "function_called": function_call.name,
                            "function_response": executed_tool_output
                        }
                    except Exception as func_e:
                        logger.error(f"Error executing function {function_call.name}: {str(func_e)}")
                        logger.error(traceback.format_exc())
                        return {
                            "status": "error",
                            "message": f"Sorry, I encountered an error while processing your request: {str(func_e)}"
                        }
                else:
                    logger.error(f"Unknown function called: {function_call.name}")
                    return {
                        "status": "error",
                        "message": "Sorry, I don't know how to handle that request."
                    }
            
        # Return normal response if no function call
        return {
            "status": "success",
            "message": response.text
        }
        
    except Exception as e:
        logger.error(f"Error generating response: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "status": "error",
            "message": "Sorry, I encountered an error while processing your request."
        }

# Initialize database tables
ensure_reminders_table()

def handle_weather_response(weather_data: dict) -> str:
    """Format weather data into a user-friendly message"""
    if not weather_data or 'error' in weather_data:
        return "Sorry, I couldn't get the weather information."
    
    try:
        temp = weather_data['main']['temp']
        desc = weather_data['weather'][0]['description']
        humidity = weather_data['main']['humidity']
        wind = weather_data['wind']['speed']
        
        return f"Current weather: {desc.capitalize()}. Temperature: {temp}°C, Humidity: {humidity}%, Wind Speed: {wind} m/s"
    except KeyError:
        return "Sorry, I couldn't process the weather information."

# Define function declarations for tools
weather_tool = Tool(
    function_declarations=[
        FunctionDeclaration(
            name="get_current_weather",
            description="Get current weather for a location",
            parameters={
                "type": "object",
                "properties": {
                    "location": {"type": "string", "description": "City name or location"},
                },
                "required": ["location"],
            },
        )
    ]
)

search_tool = Tool(
    function_declarations=[
        FunctionDeclaration(
            name="search_google",
            description="Search Google for current information",
            parameters={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query"},
                },
                "required": ["query"],
            },
        )
    ]
)

memory_tools = Tool(
    function_declarations=[
        FunctionDeclaration(
            name="store_user_memory",
            description="Store information about the user",
            parameters={
                "type": "object",
                "properties": {
                    "user_id": {"type": "string"},
                    "key": {"type": "string"},
                    "value": {"type": "string"},
                },
                "required": ["user_id", "key", "value"],
            },
        ),
        FunctionDeclaration(
            name="retrieve_user_memory",
            description="Retrieve stored information about the user",
            parameters={
                "type": "object",
                "properties": {
                    "user_id": {"type": "string"},
                    "key": {"type": "string"},
                },
                "required": ["user_id", "key"],
            },
        ),
        FunctionDeclaration(
            name="delete_user_memory",
            description="Delete stored information about the user",
            parameters={
                "type": "object",
                "properties": {
                    "user_id": {"type": "string"},
                    "key": {"type": "string"},
                },
                "required": ["user_id", "key"],
            },
        ),
    ]
)

reminder_tools = Tool(
    function_declarations=[
        FunctionDeclaration(
            name="set_reminder",
            description="Set a reminder for the user",
            parameters={
                "type": "object",
                "properties": {
                    "user_id": {"type": "string"},
                    "reminder_text": {"type": "string"},
                    "minutes_from_now": {"type": "integer", "minimum": 1},
                },
                "required": ["user_id", "reminder_text", "minutes_from_now"],
            },
        ),
        FunctionDeclaration(
            name="list_reminders",
            description="List all active reminders for the user",
            parameters={
                "type": "object",
                "properties": {
                    "user_id": {"type": "string"},
                },
                "required": ["user_id"],
            },
        ),
        FunctionDeclaration(
            name="cancel_reminder",
            description="Cancel a specific reminder",
            parameters={
                "type": "object",
                "properties": {
                    "reminder_id": {"type": "integer"},
                },
                "required": ["reminder_id"],
            },
        ),
        FunctionDeclaration(
            name="cancel_all_reminders",
            description="Cancel all reminders for the user",
            parameters={
                "type": "object",
                "properties": {
                    "user_id": {"type": "string"},
                },
                "required": ["user_id"],
            },
        ),
    ]
)

timezone_tool = Tool(
    function_declarations=[
        FunctionDeclaration(
            name="store_timezone",
            description="Store the user's timezone",
            parameters={
                "type": "object",
                "properties": {
                    "user_id": {"type": "string"},
                    "timezone": {"type": "string"},
                },
                "required": ["user_id", "timezone"],
            },
        )
    ]
)

# Combine all tools
ALL_TOOLS = [
    weather_tool,
    search_tool,
    memory_tools,
    reminder_tools,
    timezone_tool,
]

def get_ai_response(prompt: str) -> str:
    """Simple wrapper to generate a conversational response for a single-turn prompt."""
    try:
        # Use a dummy user_id for stateless calls, or extract from context if available
        user_id = "whatsapp_user"
        conversation_history = [prompt]
        result = generate_response(user_id, conversation_history)
        if result.get("status") == "success":
            return result.get("message", "Sorry, I couldn't generate a response.")
        else:
            return result.get("message", "Sorry, I couldn't generate a response.")
    except Exception as e:
        logger.error(f"get_ai_response error: {e}")
        logger.error(traceback.format_exc())
        return "Sorry, I encountered an error while generating a response."