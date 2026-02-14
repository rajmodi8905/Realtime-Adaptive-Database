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
        
        flattened = {}
        coercion_metadata = {
            "successful_coercions": [],
            "failed_coercions": []
        }
        
        for key, value in raw_record.items():
            self._normalize_and_flatten(key, value, flattened, coercion_metadata)
        
        flattened = self._inject_timestamp(flattened)
        flattened["_coercion_metadata"] = coercion_metadata
        
        return flattened
    
    def normalize_batch(self, records: list[dict]) -> list[dict]:
        return [self.normalize(record) for record in records]
    
    def _normalize_and_flatten(self, key: str, value: Any, flattened: dict, coercion_metadata: dict) -> None:
        if isinstance(value, dict):
            for nested_key, nested_value in value.items():
                compound_key = f"{key}_{nested_key}"
                self._normalize_and_flatten(compound_key, nested_value, flattened, coercion_metadata)
        elif isinstance(value, list):
            normalized_list = []
            for item in value:
                if isinstance(item, dict):
                    normalized_list.append(item)
                else:
                    normalized_item, metadata = self._coerce_scalar(item)
                    normalized_list.append(normalized_item)
                    self._update_coercion_metadata(key, metadata, coercion_metadata)
            flattened[key] = normalized_list
        else:
            normalized_value, metadata = self._coerce_scalar(value)
            flattened[key] = normalized_value
            self._update_coercion_metadata(key, metadata, coercion_metadata)
    
    def _coerce_scalar(self, value: Any) -> tuple[Any, dict]:
        metadata = {}
        
        if value is None:
            return None, {"type": "null"}
        
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
    
    def _update_coercion_metadata(self, field: str, metadata: dict, coercion_metadata: dict) -> None:
        if metadata.get("coerced"):
            coercion_metadata["successful_coercions"].append({
                "field": field,
                "from_type": metadata["from_type"],
                "to_type": metadata["to_type"]
            })
        elif metadata.get("coercion_failed"):
            coercion_metadata["failed_coercions"].append({
                "field": field,
                "attempted_type": metadata.get("attempted_type")
            })
    
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
