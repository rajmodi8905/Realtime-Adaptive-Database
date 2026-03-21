from src.analysis.field_stats import FieldStats


def test_array_metrics_accumulate_for_scalar_arrays() -> None:
    stats = FieldStats(name="tags")

    stats.update(["a", "b"], "array")
    stats.update(["x"], "array")
    stats.update([], "array")

    assert stats.array_observations == 3
    assert stats.array_total_length == 3
    assert stats.array_min_length == 0
    assert stats.array_max_length == 2
    assert stats.array_empty_count == 1
    assert stats.array_scalar_element_count == 3
    assert stats.array_non_scalar_element_count == 0
    assert stats.array_avg_length == 1.0
    assert stats.array_scalar_ratio == 1.0
    assert round(stats.array_empty_ratio, 3) == round(1 / 3, 3)
    assert stats.array_length_span == 2


def test_field_stats_from_dict_backward_compatible_for_legacy_payload() -> None:
    legacy = {
        "name": "tags",
        "nesting_depth": 0,
        "presence_count": 2,
        "type_counts": {"array": 2},
        "null_count": 0,
        "unique_count": 1,
        "is_nested": True,
        "sample_values": [["a"], ["b"]],
    }

    restored = FieldStats.from_dict(legacy)

    assert restored.name == "tags"
    assert restored.array_observations == 0
    assert restored.array_total_length == 0
    assert restored.array_scalar_element_count == 0
    assert restored.array_non_scalar_element_count == 0
