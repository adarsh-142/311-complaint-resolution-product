#!/usr/bin/env python
"""Test script to run the complete pipeline and verify all fixes."""

import json
import sys

from services.pipeline_services import run_pipeline

if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("STARTING PIPELINE TEST")
    print("=" * 80)

    try:
        result = run_pipeline()

        print("\n" + "=" * 80)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print("\nPIPELINE RESULTS:")
        print(json.dumps(result, indent=2))

        if result.get("status") == "success":
            print("\nAll systems operational!")
            sys.exit(0)
        else:
            print("\nPipeline completed with errors")
            sys.exit(1)

    except Exception as e:
        print("\n" + "=" * 80)
        print(f"PIPELINE FAILED: {str(e)}")
        print("=" * 80)
        import traceback

        traceback.print_exc()
        sys.exit(1)
