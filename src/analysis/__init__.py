# ==============================================
# TOPIC 2: ANALYSIS & CLASSIFICATION
# ==============================================
#
# This package handles observing field patterns across records
# and making autonomous placement decisions (SQL vs MongoDB).
#
# Two-step process:
#   Step 1 (Analysis):   Observe data → build statistics per field
#   Step 2 (Classification): Apply heuristics on stats → decide backend
#
# Modules:
# --------
# - field_stats.py      → Data class to hold statistics for one field
# - field_analyzer.py   → Observe records, accumulate stats per field
# - classifier.py       → Apply heuristics on stats, output placement decisions
# - decision.py         → Data classes for PlacementDecision and thresholds
#
# ==============================================
