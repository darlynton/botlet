"""Test script for enhanced search functionality."""

import os
import sys
import time
from datetime import datetime
import json

# Add the parent directory to Python path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.ai_engine import search_google

def test_basic_search():
    """Test basic search functionality."""
    print("\n1. Testing basic search...")
    result = search_google("latest technology news")
    
    if "error" in result:
        print("❌ Basic search failed:", result["error"])
        return False
        
    if "results" not in result:
        print("❌ Basic search failed: No results found")
        return False
        
    print("✅ Basic search successful")
    print(f"Found {len(result['results']['highlights'])} highlights")
    print(f"Found {len(result['results']['sources'])} sources")
    return True

def test_news_search():
    """Test news API integration."""
    print("\n2. Testing news search...")
    result = search_google("breaking news today")
    
    if "error" in result:
        print("❌ News search failed:", result["error"])
        return False
        
    if "results" not in result or "news_articles" not in result["results"]:
        print("❌ News search failed: No news articles found")
        return False
        
    print("✅ News search successful")
    print(f"Found {len(result['results']['news_articles'])} news articles")
    return True

def test_caching():
    """Test search result caching."""
    print("\n3. Testing caching...")
    
    # First search
    query = "test cache " + datetime.now().strftime("%Y%m%d%H%M%S")
    print("Making first request...")
    start_time = time.time()
    result1 = search_google(query)
    time1 = time.time() - start_time
    
    # Second search (should be cached)
    print("Making second request (should be cached)...")
    start_time = time.time()
    result2 = search_google(query)
    time2 = time.time() - start_time
    
    if "error" in result1 or "error" in result2:
        print("❌ Cache test failed: Search error")
        return False
        
    if json.dumps(result1) != json.dumps(result2):
        print("❌ Cache test failed: Results don't match")
        return False
        
    print("✅ Cache test successful")
    print(f"First request time: {time1:.2f}s")
    print(f"Second request time: {time2:.2f}s")
    print(f"Speed improvement: {((time1-time2)/time1*100):.1f}%")
    return True

def test_error_handling():
    """Test error handling with invalid queries."""
    print("\n4. Testing error handling...")
    
    # Test with empty query
    result = search_google("")
    if "error" in result:
        print("✅ Empty query handled correctly")
    else:
        print("❌ Empty query not handled")
        return False
    
    # Test with very long query
    result = search_google("x" * 1000)
    if "error" in result:
        print("✅ Very long query handled correctly")
    else:
        print("❌ Very long query not handled")
        return False
    
    return True

def run_all_tests():
    """Run all test cases."""
    print("Starting search functionality tests...")
    
    tests = [
        test_basic_search,
        test_news_search,
        test_caching,
        test_error_handling
    ]
    
    results = []
    for test in tests:
        results.append(test())
    
    print("\nTest Summary:")
    print(f"Total Tests: {len(tests)}")
    print(f"Passed: {sum(results)}")
    print(f"Failed: {len(results) - sum(results)}")
    
    if all(results):
        print("\n✅ All tests passed!")
    else:
        print("\n❌ Some tests failed")

if __name__ == "__main__":
    run_all_tests() 