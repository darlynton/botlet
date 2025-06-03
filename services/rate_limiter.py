import time
from datetime import datetime, timedelta
import sqlite3
from typing import Dict, List, Tuple, Optional
import threading
from collections import defaultdict
import logging
from services.logger_config import app_logger, log_operation

class RateLimiter:
    def __init__(self, db_path: str = "bot_data.db"):
        self.db_path = db_path
        self._ensure_tables()
        
        # In-memory storage for request tracking
        self._request_counts = defaultdict(list)  # user_id -> [(timestamp, count)]
        self._blocked_users = {}  # user_id -> unblock_time
        self._lock = threading.Lock()
        
        # Configuration
        self.WINDOW_SIZE = 3600  # 1 hour in seconds
        self.MAX_REQUESTS = 100  # Max requests per hour
        self.BURST_WINDOW = 60   # 1 minute in seconds
        self.BURST_LIMIT = 20    # Max requests per minute
        self.BLOCK_DURATION = 3600  # Block for 1 hour
        self.SUSPICIOUS_PATTERNS = {
            'rapid_fire': {'count': 10, 'window': 30},  # 10 requests in 30 seconds
            'sustained_high': {'count': 50, 'window': 600}  # 50 requests in 10 minutes
        }

    def _ensure_tables(self):
        """Create necessary database tables if they don't exist."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Table for persistent rate limit violations
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS rate_limit_violations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    violation_type TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    details TEXT
                )
            ''')
            
            # Table for user blocks
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_blocks (
                    user_id TEXT PRIMARY KEY,
                    block_start DATETIME DEFAULT CURRENT_TIMESTAMP,
                    block_end DATETIME NOT NULL,
                    reason TEXT NOT NULL
                )
            ''')
            
            conn.commit()

    def _clean_old_requests(self, user_id: str):
        """Remove requests older than the window size."""
        current_time = time.time()
        with self._lock:
            self._request_counts[user_id] = [
                (ts, count) for ts, count in self._request_counts[user_id]
                if current_time - ts < self.WINDOW_SIZE
            ]

    def _is_blocked(self, user_id: str) -> Tuple[bool, Optional[float]]:
        """Check if a user is blocked and when they'll be unblocked."""
        current_time = time.time()
        
        # Check in-memory blocks
        if user_id in self._blocked_users:
            unblock_time = self._blocked_users[user_id]
            if current_time < unblock_time:
                return True, unblock_time
            else:
                del self._blocked_users[user_id]
        
        # Check database blocks
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT block_end 
                FROM user_blocks 
                WHERE user_id = ? AND block_end > datetime('now')
            ''', (user_id,))
            result = cursor.fetchone()
            
            if result:
                block_end = datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S')
                unblock_time = block_end.timestamp()
                return True, unblock_time
        
        return False, None

    def _check_suspicious_patterns(self, user_id: str) -> Optional[str]:
        """Check for suspicious usage patterns."""
        current_time = time.time()
        requests = self._request_counts[user_id]
        
        # Check rapid-fire requests
        recent_requests = len([r for r, _ in requests 
                             if current_time - r < self.SUSPICIOUS_PATTERNS['rapid_fire']['window']])
        if recent_requests >= self.SUSPICIOUS_PATTERNS['rapid_fire']['count']:
            return 'rapid_fire'
        
        # Check sustained high usage
        sustained_requests = len([r for r, _ in requests 
                                if current_time - r < self.SUSPICIOUS_PATTERNS['sustained_high']['window']])
        if sustained_requests >= self.SUSPICIOUS_PATTERNS['sustained_high']['count']:
            return 'sustained_high'
        
        return None

    def _block_user(self, user_id: str, reason: str):
        """Block a user for violation."""
        current_time = time.time()
        block_end = current_time + self.BLOCK_DURATION
        
        # Add to in-memory blocks
        with self._lock:
            self._blocked_users[user_id] = block_end
        
        # Add to database
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO user_blocks (user_id, block_end, reason)
                VALUES (?, datetime('now', '+1 hour'), ?)
            ''', (user_id, reason))
            
            # Log violation
            cursor.execute('''
                INSERT INTO rate_limit_violations (user_id, violation_type, details)
                VALUES (?, ?, ?)
            ''', (user_id, 'block', reason))
            
            conn.commit()
        
        log_operation(app_logger, "User blocked", 
                     f"User: {user_id}, Reason: {reason}, Duration: 1 hour",
                     level=logging.WARNING)

    def check_rate_limit(self, user_id: str) -> Tuple[bool, Optional[str], Optional[float]]:
        """
        Check if a request should be allowed.
        
        Returns:
            Tuple[bool, Optional[str], Optional[float]]:
                - Boolean indicating if request is allowed
                - Optional reason if request is denied
                - Optional timestamp when user will be unblocked
        """
        # Clean old requests first
        self._clean_old_requests(user_id)
        
        # Check if user is blocked
        is_blocked, unblock_time = self._is_blocked(user_id)
        if is_blocked:
            return False, "User is blocked", unblock_time
        
        current_time = time.time()
        
        with self._lock:
            # Add new request
            self._request_counts[user_id].append((current_time, 1))
            
            # Check hour limit
            hour_requests = sum(count for ts, count in self._request_counts[user_id]
                              if current_time - ts < self.WINDOW_SIZE)
            if hour_requests > self.MAX_REQUESTS:
                self._block_user(user_id, "Exceeded hourly limit")
                return False, "Rate limit exceeded", current_time + self.BLOCK_DURATION
            
            # Check burst limit
            burst_requests = sum(count for ts, count in self._request_counts[user_id]
                               if current_time - ts < self.BURST_WINDOW)
            if burst_requests > self.BURST_LIMIT:
                self._block_user(user_id, "Exceeded burst limit")
                return False, "Burst limit exceeded", current_time + self.BLOCK_DURATION
            
            # Check for suspicious patterns
            pattern = self._check_suspicious_patterns(user_id)
            if pattern:
                self._block_user(user_id, f"Suspicious pattern detected: {pattern}")
                return False, f"Suspicious activity detected", current_time + self.BLOCK_DURATION
        
        return True, None, None

    def get_user_stats(self, user_id: str) -> Dict:
        """Get usage statistics for a user."""
        self._clean_old_requests(user_id)
        current_time = time.time()
        
        with self._lock:
            requests = self._request_counts[user_id]
            hour_requests = sum(count for ts, count in requests
                              if current_time - ts < self.WINDOW_SIZE)
            minute_requests = sum(count for ts, count in requests
                                if current_time - ts < self.BURST_WINDOW)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Get recent violations
            cursor.execute('''
                SELECT violation_type, timestamp, details
                FROM rate_limit_violations
                WHERE user_id = ?
                ORDER BY timestamp DESC
                LIMIT 5
            ''', (user_id,))
            violations = cursor.fetchall()
            
            # Get current block status
            cursor.execute('''
                SELECT block_end, reason
                FROM user_blocks
                WHERE user_id = ? AND block_end > datetime('now')
            ''', (user_id,))
            block = cursor.fetchone()
        
        return {
            "requests_last_hour": hour_requests,
            "requests_last_minute": minute_requests,
            "recent_violations": [
                {
                    "type": v[0],
                    "timestamp": v[1],
                    "details": v[2]
                } for v in violations
            ],
            "current_block": {
                "end_time": block[0],
                "reason": block[1]
            } if block else None
        }

# Create a global instance
rate_limiter = RateLimiter() 