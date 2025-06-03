"""Direct test script for search functionality."""

import os
import sys
import json
from datetime import datetime

# Add the parent directory to Python path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.ai_engine import search_google

def test_specific_search():
    """Test search with a specific query."""
    query = "Manchester United vs Aston Villa score May 25 2025"
    print(f"\nTesting search with query: {query}")
    
    try:
        result = search_google(query)
        
        if "error" in result:
            print(f"\n❌ Search failed with error: {result['error']}")
            return
            
        print("\n✅ Search completed successfully!")
        
        # Print results in a formatted way
        if "results" in result:
            results = result["results"]
            
            print("\nWeb Search Highlights:")
            for i, highlight in enumerate(results.get("highlights", []), 1):
                print(f"{i}. {highlight}")
            
            print("\nSources:")
            for i, source in enumerate(results.get("sources", []), 1):
                print(f"{i}. {source['title']}")
                print(f"   Link: {source['link']}")
            
            print("\nNews Articles:")
            for i, article in enumerate(results.get("news_articles", []), 1):
                print(f"\n{i}. {article['title']}")
                print(f"   Source: {article['source']}")
                print(f"   Published: {article['published_at']}")
                print(f"   Description: {article['description']}")
        
        # Save results for debugging
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        debug_file = f"search_results_{timestamp}.json"
        with open(debug_file, "w") as f:
            json.dump(result, f, indent=2)
        print(f"\nDetailed results saved to {debug_file}")
        
    except Exception as e:
        print(f"\n❌ Test failed with exception: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_specific_search() 