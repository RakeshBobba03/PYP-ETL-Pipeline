# app/logging_utils.py
import os
import json
import time
import hashlib
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from flask import current_app
import tempfile
import shutil

class LoggingManager:
    """Enhanced logging manager with temp storage, token masking, and structured logging"""
    
    def __init__(self, temp_dir: str = None, retention_days: int = 7):
        self.temp_dir = temp_dir or tempfile.gettempdir()
        self.retention_days = retention_days
        self.log_dir = os.path.join(self.temp_dir, 'pyp_etl_logs')
        self._ensure_log_dir()
        self._cleanup_old_logs()
    
    def _ensure_log_dir(self):
        """Ensure log directory exists"""
        os.makedirs(self.log_dir, exist_ok=True)
    
    def _cleanup_old_logs(self):
        """Remove logs older than retention period"""
        try:
            cutoff_time = time.time() - (self.retention_days * 24 * 60 * 60)
            for filename in os.listdir(self.log_dir):
                file_path = os.path.join(self.log_dir, filename)
                if os.path.isfile(file_path) and os.path.getmtime(file_path) < cutoff_time:
                    os.remove(file_path)
                    current_app.logger.info(f"[logging] Cleaned up old log file: {filename}")
        except Exception as e:
            current_app.logger.warning(f"[logging] Error cleaning up old logs: {e}")
    
    def _mask_sensitive_data(self, data: Any) -> Any:
        """Mask sensitive data in logs (API tokens, passwords, etc.)"""
        if isinstance(data, dict):
            masked = {}
            for key, value in data.items():
                if any(sensitive in key.lower() for sensitive in ['token', 'password', 'secret', 'key', 'auth']):
                    masked[key] = "***MASKED***"
                else:
                    masked[key] = self._mask_sensitive_data(value)
            return masked
        elif isinstance(data, list):
            return [self._mask_sensitive_data(item) for item in data]
        elif isinstance(data, str):
            # Mask potential tokens (base64-like strings)
            if len(data) > 20 and data.replace('=', '').replace('+', '').replace('/', '').isalnum():
                return f"***MASKED_TOKEN_{data[:8]}***"
            return data
        else:
            return data
    
    def _generate_log_id(self, operation: str) -> str:
        """Generate unique log ID for operation"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{operation}_{timestamp}_{hashlib.md5(str(time.time()).encode()).hexdigest()[:8]}"
    
    def log_event(self, event_type: str, data: Dict[str, Any], operation_id: str = None) -> str:
        """Log structured event data"""
        if not operation_id:
            operation_id = self._generate_log_id(event_type)
        
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "operation_id": operation_id,
            "event_type": event_type,
            "data": self._mask_sensitive_data(data),
            "app_version": "1.0.0"  # You can get this from config
        }
        
        # Save to temp file
        log_file = os.path.join(self.log_dir, f"{operation_id}.json")
        try:
            with open(log_file, 'w') as f:
                json.dump(log_entry, f, indent=2)
            
            # Also log to application logger
            current_app.logger.info(f"[{event_type}] {operation_id}: {data.get('summary', 'Event logged')}")
            
            return operation_id
        except Exception as e:
            current_app.logger.error(f"[logging] Failed to save log entry: {e}")
            return None
    
    def log_mutation(self, mutation_type: str, payload: Dict[str, Any], response: Dict[str, Any], 
                    dgraph_url: str, headers: Dict[str, str], operation_id: str = None) -> str:
        """Log detailed mutation information"""
        if not operation_id:
            operation_id = self._generate_log_id(f"mutation_{mutation_type}")
        
        # Create readable mutation summary
        readable_mutation = self._create_readable_mutation(mutation_type, payload)
        
        log_data = {
            "mutation_type": mutation_type,
            "dgraph_url": dgraph_url,
            "headers": self._mask_sensitive_data(headers),
            "payload_summary": self._create_payload_summary(payload),
            "readable_mutation": readable_mutation,
            "response_status": response.get("status", "unknown"),
            "response_data": self._mask_sensitive_data(response.get("data", {})),
            "response_errors": response.get("errors", []),
            "success": "errors" not in response or len(response.get("errors", [])) == 0
        }
        
        return self.log_event("mutation", log_data, operation_id)
    
    def log_decision(self, user_action: str, item_id: int, item_name: str, 
                    canonical_choices: List[str] = None, operation_id: str = None) -> str:
        """Log user decision on review items"""
        if not operation_id:
            operation_id = self._generate_log_id("decision")
        
        log_data = {
            "user_action": user_action,
            "item_id": item_id,
            "item_name": item_name,
            "canonical_choices": canonical_choices or [],
            "timestamp": datetime.now().isoformat()
        }
        
        return self.log_event("decision", log_data, operation_id)
    
    def log_etl_stats(self, filename: str, stats: Dict[str, Any], operation_id: str = None) -> str:
        """Log ETL processing statistics"""
        if not operation_id:
            operation_id = self._generate_log_id("etl_stats")
        
        log_data = {
            "filename": filename,
            "stats": stats,
            "summary": f"Processed {stats.get('total_rows', 0)} rows, {stats.get('valid_rows', 0)} valid, {stats.get('errors', 0)} errors"
        }
        
        return self.log_event("etl_stats", log_data, operation_id)
    
    def _create_payload_summary(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Create summary of mutation payload"""
        summary = {}
        
        if "query" in payload:
            summary["query_type"] = "GraphQL"
            summary["query_length"] = len(payload["query"])
        
        if "variables" in payload:
            variables = payload["variables"]
            if "in" in variables:
                input_data = variables["in"]
                if isinstance(input_data, list):
                    summary["input_count"] = len(input_data)
                    if input_data:
                        summary["input_sample"] = str(input_data[0])[:200] + "..." if len(str(input_data[0])) > 200 else str(input_data[0])
                else:
                    summary["input_data"] = str(input_data)[:200] + "..." if len(str(input_data)) > 200 else str(input_data)
        
        return summary
    
    def _create_readable_mutation(self, mutation_type: str, payload: Dict[str, Any]) -> str:
        """Create human-readable mutation description"""
        if mutation_type == "addMember":
            variables = payload.get("variables", {})
            input_data = variables.get("in", [])
            if input_data and len(input_data) > 0:
                member = input_data[0]
                return f"Add member: {member.get('businessName', 'Unknown')} with {len(member.get('products', []))} products, {len(member.get('ingredients', []))} ingredients"
        
        elif mutation_type == "addProduct":
            variables = payload.get("variables", {})
            input_data = variables.get("in", [])
            if input_data:
                product_names = [item.get("title", "Unknown") for item in input_data]
                return f"Add products: {', '.join(product_names[:3])}{'...' if len(product_names) > 3 else ''}"
        
        elif mutation_type == "addIngredients":
            variables = payload.get("variables", {})
            input_data = variables.get("in", [])
            if input_data:
                ingredient_names = [item.get("title", "Unknown") for item in input_data]
                return f"Add ingredients: {', '.join(ingredient_names[:3])}{'...' if len(ingredient_names) > 3 else ''}"
        
        return f"Mutation: {mutation_type}"
    
    def get_logs_by_operation(self, operation_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve logs for a specific operation"""
        log_file = os.path.join(self.log_dir, f"{operation_id}.json")
        try:
            if os.path.exists(log_file):
                with open(log_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            current_app.logger.error(f"[logging] Failed to retrieve log {operation_id}: {e}")
        return None
    
    def get_recent_logs(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get recent logs within specified hours"""
        cutoff_time = time.time() - (hours * 60 * 60)
        logs = []
        
        try:
            for filename in os.listdir(self.log_dir):
                if filename.endswith('.json'):
                    file_path = os.path.join(self.log_dir, filename)
                    if os.path.getmtime(file_path) > cutoff_time:
                        with open(file_path, 'r') as f:
                            logs.append(json.load(f))
        except Exception as e:
            current_app.logger.error(f"[logging] Failed to retrieve recent logs: {e}")
        
        return sorted(logs, key=lambda x: x.get('timestamp', ''), reverse=True)

# Global logging manager instance
logging_manager = LoggingManager()
