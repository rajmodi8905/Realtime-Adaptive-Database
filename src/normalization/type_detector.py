import re
import ipaddress
from datetime import datetime
from typing import Any, Optional, Union


class TypeDetector:
    NULL_VARIANTS = {"null", "none", "nil", ""}
    BOOL_TRUE_VARIANTS = {"true", "yes"} #{"1", "t", "y"}
    BOOL_FALSE_VARIANTS = {"false", "no"} #{"0", "f", "n"}
    
    UUID_PATTERN = re.compile(
        r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
        re.IGNORECASE
    )
    
    DATETIME_FORMATS = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%m-%d-%Y",
    ]

    @classmethod
    def detect(cls, value: Any) -> str:
        if value is None:
            return "null"
        
        if isinstance(value, bool):
            return "bool"
        
        if isinstance(value, int):
            return "int"
        
        if isinstance(value, float):
            return "float"
        
        if isinstance(value, list):
            return "array"
        
        if isinstance(value, dict):
            return "object"
        
        if isinstance(value, str):
            value_stripped = value.strip()
            
            if cls._is_ip_address(value_stripped):
                return "ip"
            
            if cls._is_uuid(value_stripped):
                return "uuid"
            
            if cls._is_datetime(value_stripped):
                return "datetime"
            
            return "str"
        
        return "str"

    @classmethod
    def coerce(cls, value: Any, target_type: Optional[str] = None) -> tuple[Any, bool, str]:
        if value is None:
            return None, True, "null"
        
        if isinstance(value, str):
            value = value.strip()
            
            if value.lower() in cls.NULL_VARIANTS:
                return None, True, "null"
            
            if not target_type:
                target_type = cls.detect(value)
            
            if target_type == "bool":
                if value.lower() in cls.BOOL_TRUE_VARIANTS:
                    return True, True, "bool"
                if value.lower() in cls.BOOL_FALSE_VARIANTS:
                    return False, True, "bool"
                return value, False, "str"
            
            if target_type == "int":
                try:
                    return int(value), True, "int"
                except (ValueError, TypeError):
                    try:
                        return int(float(value)), True, "int"
                    except (ValueError, TypeError):
                        return value, False, "str"
            
            if target_type == "float":
                try:
                    return float(value), True, "float"
                except (ValueError, TypeError):
                    return value, False, "str"
            
            if target_type == "datetime":
                dt = cls._parse_datetime(value)
                if dt:
                    return dt, True, "datetime"
                return value, False, "str"
            
            if target_type in ("ip", "uuid"):
                return value, True, target_type
            
            if value.lower() in cls.BOOL_TRUE_VARIANTS:
                return True, True, "bool"
            if value.lower() in cls.BOOL_FALSE_VARIANTS:
                return False, True, "bool"
            
            try:
                int_val = int(value)
                return int_val, True, "int"
            except (ValueError, TypeError):
                pass
            
            try:
                float_val = float(value)
                return float_val, True, "float"
            except (ValueError, TypeError):
                pass
            
            return value, True, "str"
        
        detected_type = cls.detect(value)
        return value, True, detected_type

    @classmethod
    def _is_ip_address(cls, value: str) -> bool:
        try:
            ipaddress.ip_address(value)
            return True
        except ValueError:
            return False

    @classmethod
    def _is_uuid(cls, value: str) -> bool:
        return bool(cls.UUID_PATTERN.match(value))

    @classmethod
    def _is_datetime(cls, value: str) -> bool:
        return cls._parse_datetime(value) is not None

    @classmethod
    def _parse_datetime(cls, value: str) -> Optional[datetime]:
        for fmt in cls.DATETIME_FORMATS:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None
