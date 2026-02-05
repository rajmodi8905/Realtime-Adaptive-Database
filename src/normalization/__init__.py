# ==============================================
# TOPIC 1: NORMALIZATION
# ==============================================
#
# This package handles everything related to cleaning,
# normalizing, and standardizing raw ingested JSON data
# BEFORE it enters the analysis pipeline.
#
# Modules:
# --------
# - field_normalizer.py  → Normalize field names (camelCase → snake_case, etc.)
# - type_detector.py     → Detect true types of values (IP vs float, UUID, etc.)
# - record_normalizer.py → Normalize a full record (orchestrates the above two)
#
# ==============================================
