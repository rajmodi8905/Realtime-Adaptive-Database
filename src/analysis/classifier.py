# ==============================================
# Classifier
# ==============================================
#
# PURPOSE:
#   Takes accumulated FieldStats from the FieldAnalyzer and applies
#   heuristic rules to produce PlacementDecision for each field.
#   This is the "brain" — it decides SQL vs MongoDB.
#
# WHY THIS CLASS EXISTS:
#   The assignment says: "No Hardcoding. Field mappings must be
#   discovered dynamically." This class implements the dynamic
#   discovery using heuristic rules on observed statistics.
#
# CLASS: Classifier
# -----------------
#   Stateless — takes stats in, produces decisions out.
#
#   Constructor:
#   ------------
#   - __init__(thresholds: ClassificationThresholds)
#
#   Methods:
#   --------
#   - classify_all(
#         stats: dict[str, FieldStats],
#         total_records: int
#     ) -> dict[str, PlacementDecision]
#       Classify every observed field. Returns mapping of
#       field_name → PlacementDecision.
#
#   - classify_field(
#         field_name: str,
#         stats: FieldStats,
#         total_records: int
#     ) -> PlacementDecision
#       Classify a single field. Applies rules in order:
#
#       RULE 1: LINKING FIELDS → BOTH
#         If field_name in ("username", "sys_ingested_at", "t_stamp"):
#           → Backend.BOTH (required in both DBs for cross-DB joins)
#
#       RULE 2: NESTED STRUCTURES → MONGODB
#         If stats.is_nested (value is dict or list):
#           → Backend.MONGODB (SQL can't handle nested objects)
#
#       RULE 3: STABLE + PRESENT → SQL
#         If presence_ratio >= threshold AND type_stability >= threshold:
#           → Backend.SQL (structured, predictable, good for SQL)
#
#       RULE 4: EVERYTHING ELSE → MONGODB
#         Low presence, type drift, or sparse fields:
#           → Backend.MONGODB (schema-flexible)
#
#   - _determine_sql_type(stats: FieldStats) -> str
#       Map the dominant detected type to a MySQL column type:
#         "int"      → "BIGINT"
#         "float"    → "DOUBLE"
#         "bool"     → "BOOLEAN"
#         "str"      → "VARCHAR(255)"
#         "ip"       → "VARCHAR(45)"
#         "uuid"     → "CHAR(36)"
#         "datetime" → "DATETIME"
#         default    → "TEXT"
#
# ==============================================

pass
