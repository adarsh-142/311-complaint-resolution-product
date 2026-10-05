from utils.json_safety import to_json_safe


def test_json_safety_handles_nested_structures(df_311_engineered):
    first_row = df_311_engineered.limit(1).collect()[0]

    payload = {
        "run_id": "abc",
        "stages": [
            {"name": "analytics", "row": first_row},
        ],
        "set_values": {"a", "b"},
        "tuple_values": (1, 2, 3),
    }

    safe = to_json_safe(payload)

    assert isinstance(safe, dict)
    assert isinstance(safe["stages"][0]["row"], dict)
    assert isinstance(safe["set_values"], list)
    assert isinstance(safe["tuple_values"], list)
