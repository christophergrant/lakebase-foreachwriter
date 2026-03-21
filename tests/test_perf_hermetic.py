import json
import os
import subprocess
import sys
from pathlib import Path


def test_perf_runner_smoke_with_fake_db(tmp_path):
    repo_root = Path(__file__).resolve().parents[1]
    profile_config = tmp_path / "profiles.json"
    profile_config.write_text(
        json.dumps(
            {
                "smoke": {
                    "defaults": {"use_spark": False, "duration_seconds": 1},
                    "modes": {
                        "insert": {
                            "rows_per_second": 200,
                            "batch_size": 50,
                            "batch_interval_ms": 10,
                            "thresholds": {
                                "min_success_rate": 99.0,
                                "min_throughput_rps": 50.0,
                                "max_p95_latency_ms": 1000.0,
                                "max_error_count": 0,
                            },
                        },
                        "upsert": {
                            "rows_per_second": 150,
                            "batch_size": 50,
                            "batch_interval_ms": 10,
                            "thresholds": {
                                "min_success_rate": 99.0,
                                "min_throughput_rps": 40.0,
                                "max_p95_latency_ms": 1000.0,
                                "max_error_count": 0,
                            },
                        },
                        "bulk-insert": {
                            "rows_per_second": 500,
                            "batch_size": 100,
                            "batch_interval_ms": 20,
                            "thresholds": {
                                "min_success_rate": 99.0,
                                "min_throughput_rps": 100.0,
                                "max_p95_latency_ms": 1000.0,
                                "max_error_count": 0,
                            },
                        },
                    },
                }
            }
        )
    )

    env = os.environ.copy()
    env.update(
        {
            "LAKEBASE_WRITER_USE_FAKE_PSYCOPG": "1",
            "LAKEBASE_WRITER_HOST": "fake-host",
            "LAKEBASE_WRITER_USER": "fake-user",
            "LAKEBASE_WRITER_PASSWORD": "fake-password",
            "PYTHONPATH": str(repo_root / "src"),
        }
    )

    for mode in ("insert", "upsert", "bulk-insert"):
        output_json = tmp_path / f"{mode}.json"
        result = subprocess.run(
            [
                sys.executable,
                str(repo_root / "tests" / "perf_runner.py"),
                "--profile-config",
                str(profile_config),
                "--profile",
                "smoke",
                "--mode",
                mode,
                "--strict",
                "--output-json",
                str(output_json),
            ],
            cwd=repo_root,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 0, result.stdout + result.stderr

        payload = json.loads(output_json.read_text())
        assert payload["result"]["total_rows_generated"] > 0
        assert payload["result"]["total_rows_written"] > 0
        assert payload["result"]["success_rate"] >= 99.0
