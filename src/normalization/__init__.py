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
# - type_detector.py     → Detect true types of values (IP vs float, UUID, etc.)
# - record_normalizer.py → Normalize a full record with aggressive type coercion
#
# ==============================================

from .type_detector import TypeDetector
from .record_normalizer import RecordNormalizer

__all__ = ["TypeDetector", "RecordNormalizer"]
