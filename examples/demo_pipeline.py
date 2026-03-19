#!/usr/bin/env python3
"""
Demo: 3-stage ETL pipeline using ETLOrchestrator + 3 ETLAgents.

Stages
------
1. Extractor  — reads employees from a CSV file
2. Transformer — normalizes keys, removes duplicates, drops null-name rows
3. Loader      — writes the clean records to a JSON file

Run from the repo root:
    python examples/demo_pipeline.py
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

# Add project source to path when running without installation
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from agent_orchestrated_etl.etl_core import (
    ETLOrchestrator,
    PipelineConfig,
)

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)


def main() -> None:
    # -----------------------------------------------------------------------
    # 1. Load pipeline config from YAML
    # -----------------------------------------------------------------------
    yaml_path = ROOT / "data" / "pipeline.yaml"
    if not yaml_path.exists():
        print(f"[ERROR] Pipeline config not found: {yaml_path}")
        sys.exit(1)

    config = PipelineConfig.from_yaml(yaml_path)
    print(f"\n{'='*60}")
    print(f"  Pipeline : {config.name}")
    print(f"  Stages   : {[s.name for s in config.stages]}")
    print(f"{'='*60}\n")

    # -----------------------------------------------------------------------
    # 2. Run the pipeline
    # -----------------------------------------------------------------------
    orchestrator = ETLOrchestrator(config)
    run = orchestrator.run()

    # -----------------------------------------------------------------------
    # 3. Print results
    # -----------------------------------------------------------------------
    print("\n" + "="*60)
    print(run.summary())
    print("="*60)

    # Show a sample of loaded records
    output_path = ROOT / "output" / "employees.json"
    if output_path.exists():
        with output_path.open() as fh:
            records = json.load(fh)
        print(f"\nSample output ({min(3, len(records))} of {len(records)} records):")
        for r in records[:3]:
            print(f"  {r}")
    else:
        print("\n[INFO] No output file produced (in-memory run).")

    print()
    if run.status != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()
