import json
from datetime import date, datetime
from decimal import Decimal

import numpy as np
from pyspark.sql import Row

from utils.json_safety import to_json_safe


def test_to_json_safe_handles_nested_workflow_payload():
    workflow_result = {
        "status": "success",
        "message": "Pipeline executed successfully",
        "results": {
            "validation": {
                "total_rows": np.int64(2),
                "null_counts": {"borough": np.int32(0)},
                "non_null_coverage": {"borough": Decimal("1.0")},
                "captured_at": datetime(2026, 9, 15, 12, 30, 45),
            },
            "analytics": {
                "total_requests": np.int64(2),
                "sla_breach_rate": Decimal("0.5"),
                "borough_stats": [
                    Row(
                        borough="BROOKLYN",
                        avg_resolution_time=Decimal("2.5"),
                        total_requests=np.int64(2),
                    )
                ],
                "summary_dates": {
                    "run_date": date(2026, 9, 15),
                    "run_time": datetime(2026, 9, 15, 12, 30, 45),
                },
            },
            "insights": {
                "insights": ["Backlog is elevated."],
                "signals": {
                    "priority_boroughs": {"BROOKLYN", "QUEENS"},
                    "priority_complaint_types": ("Noise", "Heat"),
                },
            },
            "recommendations": {"recommendations": ["Reduce backlog."]},
            "timings": {
                "fetch": Decimal("1.25"),
                "workflow": np.float64(3.75),
            },
        },
    }

    safe_payload = to_json_safe(workflow_result)

    encoded = json.dumps(safe_payload)
    decoded = json.loads(encoded)

    assert decoded["status"] == "success"
    assert decoded["results"]["validation"]["total_rows"] == 2
    assert decoded["results"]["analytics"]["borough_stats"][0]["borough"] == "BROOKLYN"
    assert decoded["results"]["insights"]["signals"]["priority_boroughs"] == ["BROOKLYN", "QUEENS"]
    assert decoded["results"]["insights"]["signals"]["priority_complaint_types"] == [
        "Noise",
        "Heat",
    ]
