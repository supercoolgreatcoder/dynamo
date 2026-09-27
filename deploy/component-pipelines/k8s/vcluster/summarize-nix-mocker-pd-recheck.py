#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Audit and summarize the interleaved six-client P/D mocker recheck."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import statistics
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


ARM_PREFIXES = {
    "pd-agw-static": "nixpds",
    "pd-agw-generic": "nixpd",
    "pd-envoy-generic": "nixpde",
    "pd-envoy-callouts": "nixpdc",
}
PATTERN = re.compile(
    r"^(?P<prefix>nixpds|nixpdg|nixpde|nixpdc|nixpd)-"
    r"(?P<workload>short|isl4000|mooncake)-"
    r"(?P<arm>pd-agw-static|pd-agw-generic|pd-envoy-generic|pd-envoy-callouts)-"
    r"(?P<trial>r[1-9][0-9]*)$"
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def stamp(value: str) -> datetime:
    return datetime.fromisoformat(value.removesuffix("Z"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result_dir", type=Path)
    parser.add_argument("--output", default="benchmark_summary.json")
    parser.add_argument("--execution", default="benchmark_execution.json")
    parser.add_argument("--plan", default="benchmark_plan.json")
    args = parser.parse_args()
    target = args.result_dir / args.output
    if target.exists():
        parser.error(f"refusing to overwrite {target}")
    plan_path = args.result_dir / args.plan
    if not plan_path.is_file():
        parser.error(f"missing frozen benchmark plan {plan_path}")
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    if plan["vcluster_api_server"] != "https://gateway-poc.mkhadkevich-dev:443" or plan["namespace"] != "dynamo-components-v2":
        parser.error("plan is not scoped to the approved vCluster")
    expected_nodes = plan["client_nodes"]
    if len(expected_nodes) != 2 or len(set(expected_nodes)) != 2:
        parser.error("benchmark plan must pin two distinct client nodes")
    arms = plan.get("arms", [plan.get("arm")])
    if not arms or any(arm not in ARM_PREFIXES for arm in arms):
        parser.error("plan contains an unsupported gateway arm")
    workloads = tuple(plan["workloads"])
    if not workloads or any(workload not in ("short", "isl4000", "mooncake") for workload in workloads):
        parser.error("plan contains an unsupported workload")
    trials = plan["trials"]
    if not trials or any(re.fullmatch(r"r[1-9][0-9]*", trial) is None for trial in trials):
        parser.error("plan contains an unsupported trial identifier")
    dataset_hash_path = args.result_dir / "dataset_sha256.tsv"
    if not dataset_hash_path.is_file():
        parser.error(f"missing live dataset hashes {dataset_hash_path}")
    observed_hashes = dict(
        reversed(line.split("\t", 1))
        for line in dataset_hash_path.read_text(encoding="utf-8").splitlines()
    )
    expected_hashes = {
        dataset["path"].replace("/shared/aiperf/", "/shared/nix/aiperf/"): dataset["sha256"]
        for dataset in plan["workloads"].values()
    }
    if observed_hashes != expected_hashes:
        parser.error("live dataset hashes do not match the frozen workload plan")
    execution_path = args.result_dir / args.execution
    if not execution_path.is_file():
        parser.error(f"missing immutable Job execution ledger {execution_path}")
    execution = json.loads(execution_path.read_text(encoding="utf-8"))
    jobs = {job["name"]: job for job in execution["jobs"]}

    runs = []
    cells = defaultdict(list)
    failures = []
    for workload in workloads:
        for arm in arms:
            ordinary_prefix = ARM_PREFIXES[arm]
            for trial in trials:
                prefix = "nixpdg" if arm == "pd-agw-generic" and workload == "mooncake" else ordinary_prefix
                job = f"{prefix}-{workload}-{arm}-{trial}"
                match = PATTERN.fullmatch(job)
                assert match is not None
                root = args.result_dir / "raw_aiperf" / job
                paths = [root / str(index) / "profile_export_aiperf.json" for index in range(6)]
                if not all(path.is_file() for path in paths):
                    failures.append(f"{job}: missing six-client exports")
                    continue
                exports = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
                phases = [export["input_config"]["phases"][0] for export in exports]
                endpoints = [export["input_config"]["endpoint"]["urls"][0] for export in exports]
                datasets = [export["input_config"]["datasets"][0] for export in exports]
                starts = [stamp(export["start_time"]) for export in exports]
                ends = [stamp(export["end_time"]) for export in exports]
                start_spread = (max(starts) - min(starts)).total_seconds()
                global_window = (max(ends) - min(starts)).total_seconds()
                requests = sum(export["request_count"]["avg"] for export in exports)
                errors = sum(sum(item["count"] for item in export["error_summary"]) for export in exports)
                cancelled = any(export["was_cancelled"] for export in exports)
                dataset_plan = plan["workloads"][workload]
                duration = dataset_plan["duration_seconds"]
                cache_file = args.result_dir / f"cache-{job}.tsv"
                cache_lines = cache_file.read_text(encoding="utf-8").splitlines() if cache_file.is_file() else []
                job_record = jobs.get(job, {})
                pod_records = job_record.get("pods", [])
                node_counts = Counter(pod["node"] for pod in pod_records)
                checks = {
                    "vcluster_execution": execution["vcluster_api_server"] == plan["vcluster_api_server"]
                    and execution["namespace"] == plan["namespace"],
                    "job_completed": job_record.get("succeeded") == 6 and job_record.get("failed") == 0
                    and len(pod_records) == 6 and all(pod["phase"] == "Succeeded" for pod in pod_records),
                    "client_placement": set(node_counts) == set(expected_nodes)
                    and sorted(node_counts.values()) == [plan["clients_per_node"]] * 2,
                    "aiperf_image": job_record.get("image") == plan["aiperf_image"],
                    "six_clients": len(exports) == 6,
                    "aiperf_0_12": all(export["aiperf_version"] == "0.12.0" for export in exports),
                    "endpoint_identity": len(set(endpoints)) == 1 and endpoints[0] == f"http://dynamo-{arm}:8080/v1/chat/completions",
                    "dataset_identity": len({json.dumps(dataset, sort_keys=True) for dataset in datasets}) == 1,
                    "dataset_workload": all(dataset["path"] == dataset_plan["path"] for dataset in datasets),
                    "duration": all(phase["duration"] == duration for phase in phases),
                    "load_policy": all(
                        (phase["type"] == "fixed_schedule" and phase["requests"] == dataset_plan["requests_per_client"])
                        if workload == "mooncake" else
                        (phase["type"] == "concurrency" and phase["concurrency"] == plan["concurrency_per_client"])
                        for phase in phases
                    ),
                    "no_errors": errors == 0 and not cancelled,
                    "synchronized_start": start_spread <= 3,
                    "positive_global_window": global_window > 0,
                    "full_mooncake_trace": workload != "mooncake" or requests == dataset_plan["requests_per_client"] * plan["clients_per_job"],
                    "prebuilt_mmap_cache": workload != "mooncake" or (
                        len(cache_lines) == 6
                        and sorted(line.split("\t", 1)[0] for line in cache_lines) == [str(index) for index in range(6)]
                        and all("Memory-mapped dataset cache HIT" in line and "skipping tokenizer + composer" in line for line in cache_lines)
                    ),
                }
                failed = [key for key, passed in checks.items() if not passed]
                if failed:
                    failures.append(f"{job}: {', '.join(failed)}")
                run = {
                    "job": job, "workload": workload, "arm": arm, "trial": trial,
                    "valid": not failed, "checks": checks,
                    "raw_export_sha256": {str(index): sha256(path) for index, path in enumerate(paths)},
                    "cache_evidence_sha256": sha256(cache_file) if cache_file.is_file() else None,
                    "dataset": datasets[0]["path"], "endpoint": endpoints[0],
                    "requests_successful": int(requests), "request_errors": errors,
                    "cancelled": cancelled, "start_spread_seconds": start_spread,
                    "global_window_seconds": global_window,
                    "summed_client_rps": sum(export["request_throughput"]["avg"] for export in exports),
                    "global_window_rps": requests / global_window if global_window > 0 else None,
                }
                runs.append(run)
                cells[(workload, arm)].append(run)

    summary = {
        "status": "valid" if not failures and len(runs) == len(workloads) * len(arms) * len(trials) else "invalid",
        "scope": "six AIPerf summary exports per Job; mock P/D workers, not GPU inference",
        "metric": "summed client RPS for same-series medians; global-window RPS for historical context",
        "plan_file": args.plan,
        "plan_sha256": sha256(plan_path),
        "dataset_sha256_file": "dataset_sha256.tsv",
        "dataset_sha256_file_sha256": sha256(dataset_hash_path),
        "execution_file": args.execution,
        "execution_sha256": sha256(execution_path),
        "expected_client_nodes": expected_nodes,
        "failures": failures,
        "runs": runs,
        "cells": [
            {
                "workload": workload, "arm": arm,
                "valid_runs": sum(run["valid"] for run in cells[(workload, arm)]),
                "median_summed_client_rps": statistics.median(run["summed_client_rps"] for run in cells[(workload, arm)] if run["valid"]) if any(run["valid"] for run in cells[(workload, arm)]) else None,
                "median_global_window_rps": statistics.median(run["global_window_rps"] for run in cells[(workload, arm)] if run["valid"]) if any(run["valid"] for run in cells[(workload, arm)]) else None,
            }
            for workload in workloads for arm in arms
        ],
    }
    target.write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"status": summary["status"], "runs": len(runs), "failures": failures}))
    return 0 if summary["status"] == "valid" else 1


if __name__ == "__main__":
    raise SystemExit(main())
