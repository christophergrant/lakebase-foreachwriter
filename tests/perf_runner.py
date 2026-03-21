#!/usr/bin/env python3
"""Run a single performance scenario and optionally enforce thresholds."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from benchmark import BenchmarkConfig


def load_profile(profile_config: Path, profile_name: str, mode: str) -> dict[str, Any]:
    profiles = json.loads(profile_config.read_text())
    if profile_name not in profiles:
        raise ValueError(f"Unknown profile: {profile_name}")

    profile = profiles[profile_name]
    mode_config = profile.get("modes", {}).get(mode)
    if mode_config is None:
        raise ValueError(f"Mode {mode!r} not defined for profile {profile_name!r}")

    defaults = profile.get("defaults", {})
    return {
        "profile_name": profile_name,
        "description": profile.get("description", ""),
        "defaults": defaults,
        "mode_config": mode_config,
    }


def build_config(profile_data: dict[str, Any], mode: str, benchmark_config_cls):
    defaults = profile_data["defaults"]
    mode_config = profile_data["mode_config"]

    return benchmark_config_cls(
        rows_per_second=int(mode_config["rows_per_second"]),
        duration_seconds=int(mode_config["duration_seconds"])
        if "duration_seconds" in mode_config
        else int(defaults["duration_seconds"]),
        batch_size=int(mode_config["batch_size"]),
        batch_interval_ms=int(mode_config["batch_interval_ms"]),
        mode=mode,
        primary_keys=["ts"] if mode == "upsert" else None,
        use_spark=bool(defaults["use_spark"]),
    )


def result_to_dict(
    profile_name: str,
    config: BenchmarkConfig,
    thresholds: dict[str, Any],
    result,
) -> dict[str, Any]:
    return {
        "profile": profile_name,
        "mode": config.mode,
        "config": {
            "rows_per_second": config.rows_per_second,
            "duration_seconds": config.duration_seconds,
            "batch_size": config.batch_size,
            "batch_interval_ms": config.batch_interval_ms,
            "mode": config.mode,
            "use_spark": config.use_spark,
            "primary_keys": config.primary_keys,
        },
        "thresholds": thresholds,
        "result": {
            "total_runtime_seconds": result.total_runtime_seconds,
            "total_rows_generated": result.total_rows_generated,
            "total_rows_written": result.total_rows_written,
            "avg_latency_ms": result.avg_latency_ms,
            "p50_latency_ms": result.p50_latency_ms,
            "p95_latency_ms": result.p95_latency_ms,
            "p99_latency_ms": result.p99_latency_ms,
            "max_latency_ms": result.max_latency_ms,
            "avg_throughput_rps": result.avg_throughput_rps,
            "peak_throughput_rps": result.peak_throughput_rps,
            "success_rate": result.success_rate,
            "performance_score": result.performance_score,
            "errors": result.errors,
        },
    }


def validate_result(payload: dict[str, Any]) -> list[str]:
    result = payload["result"]
    thresholds = payload["thresholds"]
    failures: list[str] = []

    max_error_count = int(thresholds.get("max_error_count", 0))
    if len(result["errors"]) > max_error_count:
        failures.append(
            f"errors={len(result['errors'])} exceeds max_error_count={max_error_count}"
        )

    min_success_rate = float(thresholds.get("min_success_rate", 0.0))
    if result["success_rate"] < min_success_rate:
        failures.append(
            f"success_rate={result['success_rate']:.2f} below min_success_rate={min_success_rate:.2f}"
        )

    min_throughput_rps = float(thresholds.get("min_throughput_rps", 0.0))
    if result["avg_throughput_rps"] < min_throughput_rps:
        failures.append(
            f"avg_throughput_rps={result['avg_throughput_rps']:.2f} below min_throughput_rps={min_throughput_rps:.2f}"
        )

    max_p95_latency_ms = float(thresholds.get("max_p95_latency_ms", float("inf")))
    if result["p95_latency_ms"] > max_p95_latency_ms:
        failures.append(
            f"p95_latency_ms={result['p95_latency_ms']:.2f} exceeds max_p95_latency_ms={max_p95_latency_ms:.2f}"
        )

    if result["total_rows_generated"] <= 0:
        failures.append("total_rows_generated must be > 0")

    if result["total_rows_written"] <= 0:
        failures.append("total_rows_written must be > 0")

    return failures


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a single CI-friendly performance scenario"
    )
    parser.add_argument(
        "--profile-config",
        default=Path(__file__).with_name("performance_profiles.json"),
        type=Path,
        help="Path to the performance profile JSON file",
    )
    parser.add_argument(
        "--profile",
        required=True,
        help="Profile name from performance_profiles.json",
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=["insert", "upsert", "bulk-insert"],
        help="Writer mode to benchmark",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help="Optional path to write the benchmark result JSON",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when the run violates configured thresholds",
    )

    args = parser.parse_args()

    from benchmark import BenchmarkConfig, LakebaseBenchmark

    profile_data = load_profile(args.profile_config, args.profile, args.mode)
    config = build_config(profile_data, args.mode, BenchmarkConfig)
    thresholds = profile_data["mode_config"]["thresholds"]

    benchmark = LakebaseBenchmark(config)
    result = benchmark.run_benchmark(table_prefix=f"perf_{args.profile}_{args.mode}")
    benchmark.print_results(result)

    payload = result_to_dict(args.profile, config, thresholds, result)

    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(payload, indent=2))
        print(f"\nSaved benchmark result to {args.output_json}")

    failures = validate_result(payload)
    if failures:
        print("\nPerformance threshold failures:")
        for failure in failures:
            print(f"  - {failure}")
        if args.strict:
            return 1

    print("\nPerformance thresholds satisfied.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
