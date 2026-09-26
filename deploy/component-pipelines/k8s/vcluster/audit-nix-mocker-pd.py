#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Audit six unchanged AIPerf 0.12 summary exports from one mock P/D Job."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import Counter
from datetime import datetime
from pathlib import Path


def read_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.removesuffix("Z"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result_dir", type=Path)
    parser.add_argument("workload", choices=("short", "isl4000", "mooncake"))
    parser.add_argument("trial")
    parser.add_argument("--grace", action="store_true", help="audit the distinct 46-second Mooncake series")
    parser.add_argument("--envoy", action="store_true", help="audit the Envoy generic P/D series")
    args = parser.parse_args()
    result_dir = args.result_dir
    if args.grace and args.workload != "mooncake":
        parser.error("--grace is supported only for Mooncake")
    if args.grace and args.envoy:
        parser.error("--grace and --envoy select different benchmark series")
    job_prefix = "nixpde" if args.envoy else "nixpdg" if args.grace else "nixpd"
    arm = "pd-envoy-generic" if args.envoy else "pd-agw-generic"
    job = f"{job_prefix}-{args.workload}-{arm}-{args.trial}"
    plan_path = result_dir / "benchmark_plan.json"
    plan = read_json(plan_path)
    plan_hash = sha256(plan_path)
    execution = read_json(result_dir / f"execution-{job}.json")
    blockers: list[str] = []
    checks: dict[str, bool] = {}

    def check(name: str, condition: bool) -> None:
        checks[name] = condition
        if not condition:
            blockers.append(name)

    check("execution_completed", execution["status"] == "completed")
    check("plan_identity", execution["plan_sha256"] == plan_hash)
    check(
        "series_identity",
        execution["benchmark_series_id"]
        == plan["benchmark_series_id_by_workload"][args.workload],
    )
    check("vcluster_api", execution["vcluster_api_server"] == plan["execution"]["vcluster_server"])
    check("job_identity", execution["job"]["name"] == job)
    check(
        "job_completion",
        execution["job"]["succeeded"] == 6
        and execution["job"]["failed"] == 0
        and len(execution["pods"]) == 6
        and all(pod["phase"] == "Succeeded" for pod in execution["pods"]),
    )
    check(
        "aiperf_image",
        execution["job"]["spec"]["template"]["spec"]["containers"][0]["image"]
        == plan["execution"]["aiperf_image"],
    )
    node_counts = Counter(pod["node"] for pod in execution["pods"])
    check(
        "client_placement",
        set(node_counts) == set(plan["execution"]["aiperf_nodes"])
        and sorted(node_counts.values()) == [3, 3],
    )
    dataset = plan["workloads"][args.workload]
    dataset_sha_file = result_dir / f"dataset-sha-{job}.tsv"
    if dataset_sha_file.exists():
        fields = dataset_sha_file.read_text(encoding="utf-8").strip().split("\t")
        check("dataset_sha256", fields == [args.workload, dataset["path"], dataset["sha256"]])
    else:
        check("dataset_sha256", False)

    client_records = []
    raw_paths = [
        result_dir / "raw_aiperf" / job / str(index) / "profile_export_aiperf.json"
        for index in range(6)
    ]
    for index, path in enumerate(raw_paths):
        if not path.is_file() or path.stat().st_size == 0:
            blockers.append(f"missing_raw_export_{index}")
            continue
        export = read_json(path)
        config = export["input_config"]
        phase = config["phases"][0]
        check(f"tool_version_{index}", export["aiperf_version"] == "0.12.0")
        check(
            f"endpoint_{index}",
            config["endpoint"]["urls"] == [plan["execution"]["endpoint"]]
            and config["endpoint"]["type"] == "chat"
            and config["endpoint"]["streaming"] is True,
        )
        check(
            f"model_tokenizer_{index}",
            config["models"]["items"][0]["name"] == plan["execution"]["model"]
            and config["tokenizer"]["name"] == plan["execution"]["tokenizer"],
        )
        check(
            f"workload_{index}",
            config["datasets"][0]["path"] == dataset["path"]
            and config["datasets"][0]["random_seed"] == 42,
        )
        check(
            f"phase_{index}",
            phase["duration"]
            == dataset.get("benchmark_duration_seconds", plan["execution"]["benchmark_duration_seconds"])
            and (
                phase["type"] == "fixed_schedule" and phase["requests"] == dataset["rows_per_client"]
                if args.workload == "mooncake"
                else phase["type"] == "concurrency" and phase["concurrency"] == 128
            ),
        )
        errors = sum(item["count"] for item in export["error_summary"])
        check(f"request_errors_{index}", errors == 0 and export["was_cancelled"] is False)
        metrics = {
            key: value
            for key, value in export.items()
            if isinstance(value, dict) and isinstance(value.get("avg"), (int, float))
        }
        check(
            f"finite_metrics_{index}",
            all(math.isfinite(value["avg"]) for value in metrics.values()),
        )
        client_records.append(
            {
                "index": index,
                "raw_path": str(path.relative_to(result_dir)),
                "raw_sha256": sha256(path),
                "start_time": export["start_time"],
                "end_time": export["end_time"],
                "errors": errors,
                "cancelled": export["was_cancelled"],
                "metrics": metrics,
            }
        )

    if args.workload == "mooncake":
        cache_file = result_dir / f"cache-{job}.tsv"
        check(
            "mmap_cache_hits",
            cache_file.is_file()
            and len(cache_file.read_text(encoding="utf-8").splitlines()) == 6,
        )
    check("all_exports_present", len(client_records) == 6)
    if client_records:
        starts = [timestamp(record["start_time"]) for record in client_records]
        ends = [timestamp(record["end_time"]) for record in client_records]
        spread = (max(starts) - min(starts)).total_seconds()
        window = (max(ends) - min(starts)).total_seconds()
        check("synchronized_start", spread <= 3.0)
        check("positive_global_window", window > 0)
        requests = sum(record["metrics"]["request_count"]["avg"] for record in client_records)
        rps_sum = sum(
            record["metrics"]["request_throughput"]["avg"] for record in client_records
        )
        output_tps_sum = (
            sum(
                record["metrics"]["output_token_throughput"]["avg"]
                for record in client_records
            )
            if all("output_token_throughput" in record["metrics"] for record in client_records)
            else None
        )
    else:
        spread = window = requests = rps_sum = 0
        output_tps_sum = None
    if args.workload == "mooncake":
        check("full_trace_count", requests == dataset["rows_per_client"] * 6)
    summary = {
        "job": job,
        "workload": args.workload,
        "benchmark_series_id": plan["benchmark_series_id_by_workload"][args.workload],
        "plan_sha256": plan_hash,
        "requests_successful": int(requests),
        "requests_scheduled": dataset["rows_per_client"] * 6 if args.workload == "mooncake" else None,
        "summed_client_rps_contextual": rps_sum,
        "global_window_seconds": window,
        "globally_normalized_successful_rps": requests / window if window else None,
        "summed_client_output_tps_contextual": output_tps_sum,
        "output_tps_status": "reported" if output_tps_sum is not None else "not_reported_by_aiperf",
        "start_spread_seconds": spread,
        "clients": client_records,
        "metric_scope": "six summary exports; no per-request records or merged percentiles",
    }
    audit = {
        "status": "valid" if not blockers else "invalid",
        "scope": "summary-level descriptive evidence only",
        "job": job,
        "benchmark_series_id": summary["benchmark_series_id"],
        "plan_path": str(plan_path),
        "plan_sha256": plan_hash,
        "checks": checks,
        "blockers": blockers,
        "raw_export_sha256": {record["index"]: record["raw_sha256"] for record in client_records},
        "parser": "Python json standard library",
        "aiperf_version": "0.12.0",
        "limitations": [
            "No per-request profile_export.jsonl; percentiles and duplicate/missing request IDs cannot be recomputed.",
            "Different topology/client placement from aggregate reference; no same-series gain or loss claim.",
            "Synthetic benchmark workers do not measure GPU or NIXL prefill/decode capacity.",
            "Output-token throughput is unavailable when AIPerf omits output_token_throughput; do not infer it from the zero-valued active/effective decode metrics.",
        ],
    }
    for prefix, body in (("audit", audit), ("summary", summary)):
        target = result_dir / f"{prefix}-{job}.json"
        with target.open("w", encoding="utf-8") as stream:
            json.dump(body, stream, indent=2, allow_nan=False)
            stream.write("\n")
    print(json.dumps({"job": job, "status": audit["status"], "blockers": blockers,
                      "summed_client_rps": rps_sum,
                      "globally_normalized_successful_rps": summary["globally_normalized_successful_rps"]}))
    return 0 if not blockers else 1


if __name__ == "__main__":
    raise SystemExit(main())
