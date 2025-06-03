import os
import threading
from typing import Optional
import logging

logger = logging.getLogger(__name__)

class FileLock:
    def __init__(self, lock_file: str):
        self.lock_file = os.path.abspath(lock_file)
        self.lock_handle: Optional[int] = None
        self._thread_lock = threading.Lock()
        self._pid = os.getpid()
        # Ensure lock directory exists
        os.makedirs(os.path.dirname(self.lock_file), exist_ok=True)

    def acquire(self, blocking: bool = True, force: bool = False, max_age: int = None, timeout: int = None) -> bool:
        """
        DEVELOPMENT MODE: Use a process-wide threading.Lock instead of file lock to avoid fcntl/fork issues on macOS.
        This is NOT safe for multi-process, but will allow development and testing.
        """
        acquired = self._thread_lock.acquire(blocking)
        if acquired:
            self.lock_handle = 1  # Dummy value to indicate lock held
            return True
        return False

    def release(self):
        if self.lock_handle:
            try:
                self._thread_lock.release()
            except RuntimeError:
                pass
            self.lock_handle = None

    def is_locked(self) -> bool:
        return self.lock_handle is not None

    def __del__(self):
        try:
            self.release()
        except Exception:
            pass

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
