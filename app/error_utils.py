# app/error_utils.py
import os
import json
import time
import tempfile
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from flask import current_app
from enum import Enum

class ErrorCategory(Enum):
    """Error categories for better error handling and reporting"""
    NETWORK = "network"
    VALIDATION = "validation"
    DGRAPH_DAILY_LIMIT = "dgraph_daily_limit"
    DGRAPH_SCHEMA = "dgraph_schema"
    DGRAPH_AUTH = "dgraph_auth"
    DGRAPH_QUERY = "dgraph_query"
    DGRAPH_MUTATION = "dgraph_mutation"
    FILE_PROCESSING = "file_processing"
    DATABASE = "database"
    UNKNOWN = "unknown"

class ErrorHandler:
    """Enhanced error handler with categorization, daily limit tracking, and retry logic"""
    
    def __init__(self, daily_limit_gb: float = 5.0):
        self.daily_limit_gb = daily_limit_gb
        self.daily_limit_bytes = daily_limit_gb * 1024 * 1024 * 1024  # Convert to bytes
        self.error_log = []
        self.daily_usage = {}
        self._load_daily_usage()
    
    def _load_daily_usage(self):
        """Load daily usage data from file"""
        try:
            usage_file = os.path.join(tempfile.gettempdir(), 'pyp_etl_daily_usage.json')
            if os.path.exists(usage_file):
                with open(usage_file, 'r') as f:
                    self.daily_usage = json.load(f)
        except Exception as e:
            current_app.logger.warning(f"[error_handler] Failed to load daily usage: {e}")
            self.daily_usage = {}
    
    def _save_daily_usage(self):
        """Save daily usage data to file"""
        try:
            usage_file = os.path.join(tempfile.gettempdir(), 'pyp_etl_daily_usage.json')
            with open(usage_file, 'w') as f:
                json.dump(self.daily_usage, f, indent=2)
        except Exception as e:
            current_app.logger.warning(f"[error_handler] Failed to save daily usage: {e}")
    
    def _get_today_key(self) -> str:
        """Get today's date key for usage tracking"""
        return datetime.now().strftime("%Y-%m-%d")
    
    def _categorize_error(self, error: Exception, context: str = "") -> ErrorCategory:
        """Categorize error based on type and message"""
        error_msg = str(error).lower()
        error_type = type(error).__name__.lower()
        
        # Network errors
        if any(keyword in error_msg for keyword in ['connection', 'timeout', 'network', 'unreachable', 'refused']):
            return ErrorCategory.NETWORK
        
        # Dgraph daily limit errors
        if any(keyword in error_msg for keyword in ['daily limit', 'quota exceeded', 'rate limit', 'too many requests']):
            return ErrorCategory.DGRAPH_DAILY_LIMIT
        
        # Dgraph authentication errors
        if any(keyword in error_msg for keyword in ['unauthorized', 'forbidden', 'invalid token', 'authentication']):
            return ErrorCategory.DGRAPH_AUTH
        
        # Dgraph schema errors
        if any(keyword in error_msg for keyword in ['schema', 'field not found', 'type mismatch', 'invalid field']):
            return ErrorCategory.DGRAPH_SCHEMA
        
        # Dgraph query errors
        if any(keyword in error_msg for keyword in ['query', 'syntax', 'parse', 'invalid query']):
            return ErrorCategory.DGRAPH_QUERY
        
        # Dgraph mutation errors
        if any(keyword in error_msg for keyword in ['mutation', 'add', 'update', 'delete']):
            return ErrorCategory.DGRAPH_MUTATION
        
        # Validation errors
        if any(keyword in error_msg for keyword in ['validation', 'required', 'invalid', 'missing']):
            return ErrorCategory.VALIDATION
        
        # File processing errors
        if any(keyword in error_msg for keyword in ['file', 'excel', 'csv', 'parse', 'format']):
            return ErrorCategory.FILE_PROCESSING
        
        # Database errors
        if any(keyword in error_msg for keyword in ['database', 'sql', 'connection', 'transaction']):
            return ErrorCategory.DATABASE
        
        return ErrorCategory.UNKNOWN
    
    def track_data_usage(self, data_size_bytes: int, operation: str = "mutation"):
        """Track daily data usage for Dgraph daily limit monitoring"""
        today = self._get_today_key()
        
        if today not in self.daily_usage:
            self.daily_usage[today] = {
                "total_bytes": 0,
                "operations": 0,
                "mutations": 0,
                "queries": 0
            }
        
        self.daily_usage[today]["total_bytes"] += data_size_bytes
        self.daily_usage[today]["operations"] += 1
        self.daily_usage[today][operation + "s"] += 1
        
        # Check if approaching daily limit
        usage_gb = self.daily_usage[today]["total_bytes"] / (1024 * 1024 * 1024)
        if usage_gb > self.daily_limit_gb * 0.8:  # 80% of limit
            current_app.logger.warning(f"[error_handler] Approaching daily limit: {usage_gb:.2f}GB / {self.daily_limit_gb}GB")
        
        self._save_daily_usage()
    
    def check_daily_limit(self) -> Tuple[bool, float, float]:
        """Check if daily limit is exceeded"""
        today = self._get_today_key()
        
        if today not in self.daily_usage:
            return False, 0.0, self.daily_limit_gb
        
        usage_bytes = self.daily_usage[today]["total_bytes"]
        usage_gb = usage_bytes / (1024 * 1024 * 1024)
        
        return usage_gb >= self.daily_limit_gb, usage_gb, self.daily_limit_gb
    
    def handle_error(self, error: Exception, context: str = "", operation_id: str = None, 
                    retry_count: int = 0, max_retries: int = 3) -> Dict[str, Any]:
        """Handle error with categorization and retry logic"""
        category = self._categorize_error(error, context)
        
        error_info = {
            "timestamp": datetime.now().isoformat(),
            "operation_id": operation_id,
            "category": category.value,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "context": context,
            "retry_count": retry_count,
            "max_retries": max_retries,
            "should_retry": self._should_retry(error, category, retry_count, max_retries),
            "retry_delay": self._calculate_retry_delay(category, retry_count)
        }
        
        # Log error
        current_app.logger.error(f"[{category.value}] {context}: {error}")
        
        # Add to error log
        self.error_log.append(error_info)
        
        # Handle specific error types
        if category == ErrorCategory.DGRAPH_DAILY_LIMIT:
            error_info["daily_limit_exceeded"] = True
            error_info["usage_info"] = self.get_daily_usage_info()
        
        return error_info
    
    def _should_retry(self, error: Exception, category: ErrorCategory, retry_count: int, max_retries: int) -> bool:
        """Determine if error should be retried"""
        if retry_count >= max_retries:
            return False
        
        # Don't retry certain error types
        non_retryable_categories = [
            ErrorCategory.VALIDATION,
            ErrorCategory.DGRAPH_SCHEMA,
            ErrorCategory.DGRAPH_AUTH,
            ErrorCategory.DGRAPH_DAILY_LIMIT
        ]
        
        return category not in non_retryable_categories
    
    def _calculate_retry_delay(self, category: ErrorCategory, retry_count: int) -> int:
        """Calculate retry delay based on error category and retry count"""
        base_delays = {
            ErrorCategory.NETWORK: 2,
            ErrorCategory.DGRAPH_QUERY: 1,
            ErrorCategory.DGRAPH_MUTATION: 3,
            ErrorCategory.DATABASE: 2,
            ErrorCategory.UNKNOWN: 1
        }
        
        base_delay = base_delays.get(category, 1)
        return base_delay * (2 ** retry_count)  # Exponential backoff
    
    def get_daily_usage_info(self) -> Dict[str, Any]:
        """Get current daily usage information"""
        today = self._get_today_key()
        
        if today not in self.daily_usage:
            return {
                "date": today,
                "usage_gb": 0.0,
                "limit_gb": self.daily_limit_gb,
                "percentage": 0.0,
                "operations": 0,
                "mutations": 0,
                "queries": 0
            }
        
        usage_bytes = self.daily_usage[today]["total_bytes"]
        usage_gb = usage_bytes / (1024 * 1024 * 1024)
        percentage = (usage_gb / self.daily_limit_gb) * 100
        
        return {
            "date": today,
            "usage_gb": round(usage_gb, 2),
            "limit_gb": self.daily_limit_gb,
            "percentage": round(percentage, 1),
            "operations": self.daily_usage[today]["operations"],
            "mutations": self.daily_usage[today]["mutations"],
            "queries": self.daily_usage[today]["queries"]
        }
    
    def get_error_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get error summary for specified time period"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        recent_errors = [
            error for error in self.error_log
            if datetime.fromisoformat(error["timestamp"]) > cutoff_time
        ]
        
        # Categorize errors
        error_counts = {}
        for error in recent_errors:
            category = error["category"]
            error_counts[category] = error_counts.get(category, 0) + 1
        
        return {
            "time_period_hours": hours,
            "total_errors": len(recent_errors),
            "error_counts": error_counts,
            "recent_errors": recent_errors[-10:]  # Last 10 errors
        }
    
    def clear_old_errors(self, days: int = 7):
        """Clear errors older than specified days"""
        cutoff_time = datetime.now() - timedelta(days=days)
        
        self.error_log = [
            error for error in self.error_log
            if datetime.fromisoformat(error["timestamp"]) > cutoff_time
        ]
    
    def estimate_data_size(self, payload: Dict[str, Any]) -> int:
        """Estimate data size of payload in bytes"""
        try:
            return len(json.dumps(payload, default=str).encode('utf-8'))
        except Exception:
            return 0

# Global error handler instance
error_handler = ErrorHandler()
