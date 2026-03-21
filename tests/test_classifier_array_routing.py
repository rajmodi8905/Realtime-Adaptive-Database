from collections.abc import Sequence

from src.analysis.classifier import Classifier
from src.analysis.field_stats import FieldStats


def _build_array_stats(name: str, arrays: Sequence[Sequence[object]]) -> FieldStats:
    stats = FieldStats(name=name)
    for arr in arrays:
        stats.update(arr, "array")
    return stats


def test_classifier_routes_scalar_arrays_to_sql_when_eligible() -> None:
    classifier = Classifier()
    stats = _build_array_stats("tags", [["a", "b"], ["c"], ["d", "e"], ["f"]])

    decision = classifier.classify_field("tags", stats, total_records=4)

    assert decision.backend.value == "SQL"
    assert decision.canonical_type == "array"


def test_classifier_keeps_object_arrays_in_mongodb() -> None:
    classifier = Classifier()
    stats = _build_array_stats(
        "items",
        [[{"id": 1}], [{"id": 2, "name": "n"}]],
    )

    decision = classifier.classify_field("items", stats, total_records=2)

    assert decision.backend.value == "MONGODB"
    assert decision.canonical_type == "array"


def test_classifier_keeps_large_scalar_arrays_in_mongodb() -> None:
    classifier = Classifier()
    long_arr = list(range(200))
    stats = _build_array_stats("readings", [long_arr, long_arr])

    decision = classifier.classify_field("readings", stats, total_records=2)

    assert decision.backend.value == "MONGODB"


def test_non_array_scalar_rule_unchanged() -> None:
    classifier = Classifier()
    stats = FieldStats(name="temperature")
    for value in [20, 21, 19, 20]:
        stats.update(value, "int")

    decision = classifier.classify_field("temperature", stats, total_records=4)

    assert decision.backend.value == "SQL"
    assert decision.canonical_type == "int"
