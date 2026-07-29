"""Command-line entrypoint for the finite, synthetic reference pipeline."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable

from .config import PipelineConfig
from .pipeline import FraudPipeline
from .simulator import SimulationConfig, TransactionSimulator


def _write_ndjson(path: Path, records: Iterable[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")))
            handle.write("\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a deterministic synthetic fraud-streaming demo."
    )
    parser.add_argument("--config", default="config/rules.json")
    parser.add_argument("--events", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--accounts", type=int, default=8)
    parser.add_argument("--output-dir", default="artifacts")
    return parser


def run(args: argparse.Namespace) -> dict[str, Any]:
    pipeline_config = PipelineConfig.load(args.config)
    simulation_config = SimulationConfig(
        seed=args.seed,
        event_count=args.events,
        account_count=args.accounts,
    )
    arrivals = TransactionSimulator(simulation_config).generate()
    pipeline = FraudPipeline(pipeline_config)
    alerts = pipeline.process_all(arrivals)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_ndjson(output_dir / "events.ndjson", (event.to_dict() for event in arrivals))
    _write_ndjson(output_dir / "alerts.ndjson", (alert.to_dict() for alert in alerts))

    metrics: dict[str, Any] = pipeline.metrics_snapshot()
    metrics["demo"] = {
        "synthetic_data": True,
        "production_deployment": False,
        "seed": args.seed,
        "requested_unique_events": args.events,
    }
    with (output_dir / "metrics.json").open("w", encoding="utf-8") as handle:
        json.dump(metrics, handle, indent=2, sort_keys=True)
        handle.write("\n")

    return {
        "arrivals": len(arrivals),
        "alerts": len(alerts),
        "output_dir": str(output_dir),
        "synthetic_data": True,
        "production_deployment": False,
    }


def main() -> int:
    args = build_parser().parse_args()
    summary = run(args)
    print(json.dumps(summary, sort_keys=True))
    return 0

