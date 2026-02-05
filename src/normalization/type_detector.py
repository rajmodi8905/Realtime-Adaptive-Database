# ==============================================
# TypeDetector
# ==============================================
#
# PURPOSE:
#   Detect the TRUE semantic type of a value, not just the Python type.
#   This is critical because JSON only has string, number, bool, null,
#   array, object — but we need to distinguish between:
#     - "192.168.1.1" (IP address) vs "hello" (plain string)
#     - "473af720-92e2-..." (UUID) vs "some-random-text" (string)
#     - "2024-01-15T10:30:00Z" (datetime) vs "Jan 15" (string)
#     - 1.234 (float) vs True (bool, which is a subclass of int in Python!)
#
# WHY THIS CLASS EXISTS:
#   The assignment specifically asks:
#     "How did your system differentiate between a string representing
#      an IP ('1.2.3.4') and a float (1.2)?"
#   This class is the answer.
#
# CLASS: TypeDetector
# -------------------
#   Stateless utility class. Takes a value, returns its detected type as a string.
#
#   Methods:
#   --------
#   - detect(value: Any) -> str
#       Returns one of:
#       "null", "bool", "int", "float", "str", "ip", "uuid",
#       "datetime", "array", "object"
#
#   Internal helpers:
#   -----------------
#   - _is_ip_address(value: str) -> bool
#   - _is_uuid(value: str) -> bool
#   - _is_datetime(value: str) -> bool
#
# DETECTION PRIORITY (order matters!):
# ------------------------------------
#   1. None          → "null"
#   2. bool          → "bool"     (MUST check before int — bool is subclass of int)
#   3. int           → "int"
#   4. float         → "float"
#   5. list          → "array"
#   6. dict          → "object"
#   7. str:
#      a. IP pattern   → "ip"
#      b. UUID pattern → "uuid"
#      c. Datetime     → "datetime"
#      d. Otherwise    → "str"
#
# ==============================================

pass
