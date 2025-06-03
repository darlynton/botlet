#!/usr/bin/env python3
from services.message_queue import MessageQueue
import json

def main():
    queue = MessageQueue()
    status = queue.get_queue_status()
    
    if 'error' in status:
        print(f"\nError getting queue status: {status['error']}")
        return
        
    print("\nMessage Queue Status:")
    print("=" * 50)
    print(f"Queue Processor Running: {status.get('queue_processor_running', 'Unknown')}")
    print("\nMessage Counts by Status:")
    for status_type, count in status.get('status_counts', {}).items():
        print(f"  {status_type}: {count}")
    print(f"\nPending Messages: {status.get('pending_count', 0)}")
    if status.get('oldest_pending'):
        print(f"Oldest Pending: {status['oldest_pending']}")
    print(f"Recent Failures: {status.get('recent_failures', 0)}")
    print("=" * 50)

if __name__ == "__main__":
    main()
