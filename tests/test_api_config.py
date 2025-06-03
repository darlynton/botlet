"""Test script to verify API configurations."""

import os
import sys
from dotenv import load_dotenv

# Add the parent directory to Python path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def check_api_keys():
    """Check if all required API keys are configured."""
    print("\nChecking API configurations...")
    
    # Load environment variables
    load_dotenv()
    
    # Required API keys
    required_keys = {
        "Search_API_KEY": "Google Custom Search API Key",
        "GOOGLE_CSE_ID": "Google Custom Search Engine ID",
        "NEWS_API_KEY": "News API Key"
    }
    
    all_configured = True
    
    for env_var, description in required_keys.items():
        value = os.getenv(env_var)
        if not value:
            print(f"❌ {description} ({env_var}) is not configured")
            all_configured = False
        else:
            masked_value = value[:4] + "*" * (len(value) - 4)
            print(f"✅ {description} ({env_var}) is configured: {masked_value}")
    
    if all_configured:
        print("\n✅ All API keys are properly configured!")
    else:
        print("\n❌ Some API keys are missing. Please check your .env file.")
        print("\nAdd the following to your .env file:")
        for env_var in required_keys:
            if not os.getenv(env_var):
                print(f"{env_var}=your_{env_var.lower()}_here")

if __name__ == "__main__":
    check_api_keys() 