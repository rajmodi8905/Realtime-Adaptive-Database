# ==============================================
# FieldNormalizer
# ==============================================
#
# PURPOSE:
#   Convert all field names to a single canonical form (snake_case)
#   so that naming ambiguities are resolved before analysis.
#
# WHY THIS CLASS EXISTS:
#   The data stream can send the same logical field under different
#   names across records:
#     - "userName", "UserName", "user_name", "username"
#     - "ip", "IP", "IpAddress", "ip_address"
#   If we don't normalize, the analyzer treats them as separate
#   fields and our statistics are wrong.
#
# CLASS: FieldNormalizer
# ----------------------
#   Stateless utility class. Takes a raw field name, returns canonical form.
#
#   Methods:
#   --------
#   - normalize(name: str) -> str
#       Convert a single field name to snake_case.
#       Handles camelCase, PascalCase, UPPERCASE, abbreviations.
#
#   - are_similar(name_a: str, name_b: str) -> bool
#       Returns True if two raw names resolve to the same canonical name.
#
#   Internal helpers:
#   -----------------
#   - _camel_to_snake(name: str) -> str
#   - _handle_abbreviations(name: str) -> str
#       Handle known patterns like IP, URL, ID correctly.
#
# RULES:
# ------
#   1. camelCase    → snake_case    (userName → user_name)
#   2. PascalCase   → snake_case    (UserName → user_name)
#   3. ALLCAPS      → lowercase     (IP → ip)
#   4. Mixed abbrev → snake_case    (IpAddress → ip_address)
#   5. Already snake → unchanged    (ip_address → ip_address)
#   6. Remove special characters, collapse multiple underscores
#
# ==============================================

import re
from typing import Dict


class FieldNormalizer:
    """
    Converts field names to canonical snake_case format.
    Maintains a mapping of original names to canonical forms.
    """
    
    def __init__(self):
        """Initialize the normalizer with an empty mapping registry."""
        self._mappings: Dict[str, str] = {}
    
    def normalize(self, name: str) -> str:
        """
        Convert a field name to snake_case.
        
        Args:
            name: Raw field name (e.g., "userName", "IpAddress", "IP")
            
        Returns:
            Canonical snake_case name (e.g., "user_name", "ip_address", "ip")
        """
        if not name:
            return name
        
        # Check if we've already normalized this name
        if name in self._mappings:
            return self._mappings[name]
        
        # Perform normalization
        normalized = self._camel_to_snake(name)
        
        # Store the mapping
        self._mappings[name] = normalized
        
        return normalized
    
    def are_similar(self, name_a: str, name_b: str) -> bool:
        """
        Check if two field names resolve to the same canonical name.
        
        Args:
            name_a: First field name
            name_b: Second field name
            
        Returns:
            True if both names normalize to the same canonical form
        """
        return self.normalize(name_a) == self.normalize(name_b)
    
    def get_mappings(self) -> Dict[str, str]:
        """
        Get all field name mappings.
        
        Returns:
            Dictionary mapping original names to canonical names
        """
        return self._mappings.copy()
    
    def restore_mappings(self, mappings: Dict[str, str]) -> None:
        """
        Restore field name mappings from a previous session.
        
        Args:
            mappings: Dictionary of original_name -> canonical_name
        """
        self._mappings = mappings.copy()
    
    def _camel_to_snake(self, name: str) -> str:
        """
        Convert camelCase/PascalCase to snake_case.
        
        Args:
            name: Input field name
            
        Returns:
            snake_case version of the name
        """
        # Remove any non-alphanumeric characters except underscores
        name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        
        # Handle sequences of capitals (e.g., "XMLParser" -> "xml_parser")
        # Insert underscore before capital letter followed by lowercase
        name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
        
        # Insert underscore before capital letters that follow lowercase letters
        name = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', name)
        
        # Convert to lowercase
        name = name.lower()
        
        # Collapse multiple underscores
        name = re.sub(r'_+', '_', name)
        
        # Remove leading/trailing underscores
        name = name.strip('_')
        
        return name
