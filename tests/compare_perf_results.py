#!/usr/bin/env python3
"""Compare performance results between a baseline and a candidate."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def load_results(directory: Path) -> dict[str, dict]:
    results: dict[str, dict] = {}
    for path in sorted(directory.glob("*.json")):
        payload = json.loads(path.read_text())
        results[payload["mode"]] = payload
    return results


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare candidate benchmark results against a baseline"
    )
    parser.add_argument("--base-dir", required=True, type=Path)
    parser.add_argument("--head-dir", required=True, type=Path)
    parser.add_argument(
        "--max-throughput-regression-pct",
        type=float,
        default=20.0,
        help="Maximum allowed drop in average throughput versus baseline",
    )
    parser.add_argument(
        "--max-p95-regression-pct",
        type=float,
        default=35.0,
        help="Maximum allowed increase in p95 latency versus baseline",
    )
    parser.add_argument(
        "--max-success-rate-drop-pct",
        type=float,
        default=1.0,
        help="Maximum allowed drop in success rate versus baseline",
    )

    args = parser.parse_args()

    base_results = load_results(args.base_dir)
    head_results = load_results(args.head_dir)

    failures: list[str] = []
    compared_modes = sorted(set(base_results) | set(head_results))

    for mode in compared_modes:
        if mode not in base_results:
            failures.append(f"missing baseline result for mode={mode}")
            continue
        if mode not in head_results:
            failures.append(f"missing candidate result for mode={mode}")
            continue

        base = base_results[mode]["result"]
        head = head_results[mode]["result"]

        min_allowed_throughput = base["avg_throughput_rps"] * (
            1.0 - args.max_throughput_regression_pct / 100.0
        )
        max_allowed_p95 = base["p95_latency_ms"] * (
            1.0 + args.max_p95_regression_pct / 100.0
        )
        min_allowed_success_rate = (
            base["success_rate"] - args.max_success_rate_drop_pct
        )

        print(
            f"{mode}: "
            f"throughput {head['avg_throughput_rps']:.2f} vs {base['avg_throughput_rps']:.2f}, "
            f"p95 {head['p95_latency_ms']:.2f}ms vs {base['p95_latency_ms']:.2f}ms, "
            f"success {head['success_rate']:.2f}% vs {base['success_rate']:.2f}%"
        )

        if head["avg_throughput_rps"] < min_allowed_throughput:
            failures.append(
                f"{mode}: throughput dropped from {base['avg_throughput_rps']:.2f} to "
                f"{head['avg_throughput_rps']:.2f}, exceeding allowed {args.max_throughput_regression_pct:.1f}% regression"
            )

        if head["p95_latency_ms"] > max_allowed_p95:
            failures.append(
                f"{mode}: p95 latency increased from {base['p95_latency_ms']:.2f}ms to "
                f"{head['p95_latency_ms']:.2f}ms, exceeding allowed {args.max_p95_regression_pct:.1f}% regression"
            )

        if head["success_rate"] < min_allowed_success_rate:
            failures.append(
                f"{mode}: success rate dropped from {base['success_rate']:.2f}% to "
                f"{head['success_rate']:.2f}%, exceeding allowed {args.max_success_rate_drop_pct:.2f} point drop"
            )

    if failures:
        print("\nPerformance comparison failures:")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    print("\nCandidate is within the allowed performance regression budget.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
