from datetime import datetime, timezone
from typing import Any, Optional
from .type_detector import TypeDetector


class RecordNormalizer:
    def __init__(self, type_detector: Optional[TypeDetector] = None):
        self.type_detector = type_detector or TypeDetector()
        
    def normalize(self, raw_record: dict) -> dict:
        if not isinstance(raw_record, dict):
            raise ValueError("Record must be a dictionary")
        
        self._validate_required_fields(raw_record)
        
        normalized = {}
        coercion_metadata = {
            "successful_coercions": [],
            "failed_coercions": []
        }
        
        for key, value in raw_record.items():
            normalized_value, metadata = self._normalize_value(value, key)
            normalized[key] = normalized_value
            
            if metadata.get("coerced"):
                coercion_metadata["successful_coercions"].append({
                    "field": key,
                    "from_type": metadata["from_type"],
                    "to_type": metadata["to_type"]
                })
            elif metadata.get("coercion_failed"):
                coercion_metadata["failed_coercions"].append({
                    "field": key,
                    "attempted_type": metadata.get("attempted_type")
                })
        
        normalized = self._inject_timestamp(normalized)
        normalized["_coercion_metadata"] = coercion_metadata
        
        return normalized
    
    def normalize_batch(self, records: list[dict]) -> list[dict]:
        return [self.normalize(record) for record in records]
    
    def _normalize_value(self, value: Any, field_name: str = "") -> tuple[Any, dict]:
        metadata = {}
        
        if value is None:
            return None, {"type": "null"}
        
        if isinstance(value, dict):
            normalized_dict = {}
            for k, v in value.items():
                normalized_dict[k], _ = self._normalize_value(v, f"{field_name}.{k}")
            return normalized_dict, {"type": "object"}
        
        if isinstance(value, list):
            normalized_list = []
            for i, item in enumerate(value):
                normalized_item, _ = self._normalize_value(item, f"{field_name}[{i}]")
                normalized_list.append(normalized_item)
            return normalized_list, {"type": "array"}
        
        if isinstance(value, bool):
            return value, {"type": "bool"}
        
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return value, {"type": self.type_detector.detect(value)}
        
        if isinstance(value, str):
            original_value = value
            original_type = "str"
            
            coerced_value, success, detected_type = self.type_detector.coerce(value)
            
            if success and type(coerced_value) != type(original_value):
                return coerced_value, {
                    "type": detected_type,
                    "coerced": True,
                    "from_type": original_type,
                    "to_type": detected_type
                }
            
            if not success:
                return original_value, {
                    "type": "str",
                    "coercion_failed": True,
                    "attempted_type": detected_type
                }
            
            return coerced_value, {"type": detected_type}
        
        return value, {"type": "unknown"}
    
    def _inject_timestamp(self, record: dict) -> dict:
        record["sys_ingested_at"] = datetime.now(timezone.utc).isoformat()
        return record
    
    def _validate_required_fields(self, record: dict) -> None:
        if "username" not in record or record["username"] is None or record["username"] == "":
            raise ValueError("Required field 'username' is missing or empty")
