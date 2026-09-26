#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Audit record-complete ISL4000 P/D runs and compare their interior windows."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


def read_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def utc_ns(value: str) -> int:
    stamp = datetime.fromisoformat(value.removesuffix("Z"))
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    else:
        stamp = stamp.astimezone(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = stamp - epoch
    return ((delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds) * 1000


def arm_job(arm: str, trial: str) -> str:
    return f"nixpdr-isl4000-pd-{arm}-generic-{trial}"


def audit_arm(result_dir: Path, arm: str, trial: str, plan: dict, plan_hash: str) -> int:
    job = arm_job(arm, trial)
    execution = read_json(result_dir / f"execution-{job}.json")
    checks: dict[str, bool] = {}
    blockers: list[str] = []

    def check(name: str, passed: bool) -> None:
        checks[name] = bool(passed)
        if not passed:
            blockers.append(name)

    check("execution_completed", execution["status"] == "completed")
    check("plan_identity", execution["plan_sha256"] == plan_hash)
    check("series_identity", execution["benchmark_series_id"] == plan["benchmark_series_id"])
    check("vcluster_api", execution["vcluster_api_server"] == plan["execution"]["vcluster_server"])
    check("job_identity", execution["job"]["name"] == job)
    check(
        "job_completion",
        execution["job"]["succeeded"] == 6
        and execution["job"]["failed"] == 0
        and len(execution["pods"]) == 6
        and all(pod["phase"] == "Succeeded" for pod in execution["pods"]),
    )
    node_counts = Counter(pod["node"] for pod in execution["pods"])
    check(
        "client_placement",
        set(node_counts) == set(plan["execution"]["aiperf_nodes"])
        and sorted(node_counts.values()) == [3, 3],
    )
    container = execution["job"]["spec"]["template"]["spec"]["containers"][0]
    check("aiperf_image", container["image"] == plan["execution"]["aiperf_image"])
    command = " ".join(container["args"])
    check("record_export_flags", "--export-level records --slice-duration 1" in command)
    dataset = plan["workload"]
    dataset_sha_file = result_dir / f"dataset-sha-{job}.tsv"
    check(
        "dataset_sha256",
        dataset_sha_file.is_file()
        and dataset_sha_file.read_text(encoding="utf-8").strip().split("\t")
        == ["isl4000", dataset["path"], dataset["sha256"]],
    )

    exports = []
    for index in range(6):
        path = result_dir / "raw_aiperf" / job / str(index) / "profile_export_aiperf.json"
        if not path.is_file() or path.stat().st_size == 0:
            blockers.append(f"missing_export_{index}")
            continue
        exported = read_json(path)
        config = exported["input_config"]
        phase = config["phases"][0]
        check(f"tool_version_{index}", exported["aiperf_version"] == plan["execution"]["aiperf_version"])
        check(
            f"endpoint_{index}",
            config["endpoint"]["urls"] == [plan["execution"]["endpoints"][f"pd-{arm}-generic"]]
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
            and config["datasets"][0]["random_seed"] == plan["execution"]["seed"],
        )
        check(
            f"phase_{index}",
            phase["type"] == "concurrency"
            and phase["concurrency"] == plan["execution"]["concurrency_per_client"]
            and phase["duration"] == plan["execution"]["benchmark_duration_seconds"],
        )
        check(f"record_export_{index}", "jsonl" in config["artifacts"]["records"])
        check(
            f"request_errors_{index}",
            not exported["error_summary"] and exported["was_cancelled"] is False,
        )
        exports.append(exported)
    check("all_exports_present", len(exports) == 6)
    if len(exports) != 6:
        return write_arm(result_dir, job, plan, plan_hash, checks, blockers, {}, [])

    starts = [utc_ns(exported["start_time"]) for exported in exports]
    ends = [utc_ns(exported["end_time"]) for exported in exports]
    window_start = max(starts) + 2_000_000_000
    window_end = window_start + plan["measurement"]["common_window_seconds"] * 1_000_000_000
    check("common_window_covered", all(end >= window_end + 2_000_000_000 for end in ends))
    client_records = []
    all_request_ids: set[str] = set()
    for index, exported in enumerate(exports):
        path = result_dir / "raw_aiperf" / job / str(index) / "profile_export.jsonl"
        if not path.is_file() or path.stat().st_size == 0:
            blockers.append(f"missing_records_{index}")
            continue
        digest = hashlib.sha256()
        count = 0
        common_count = 0
        malformed = 0
        duplicate = 0
        with path.open("rb") as stream:
            for line in stream:
                digest.update(line)
                try:
                    record = json.loads(line)
                    metadata = record["metadata"]
                    request_id = metadata["x_request_id"]
                    start_ns = metadata["request_start_ns"]
                    end_ns = metadata["request_end_ns"]
                    latency = record["metrics"]["request_latency"]["value"]
                    if (
                        metadata["benchmark_phase"] != "profiling"
                        or metadata["was_cancelled"] is not False
                        or not isinstance(request_id, str)
                        or not request_id
                        or not isinstance(start_ns, int)
                        or not isinstance(end_ns, int)
                        or end_ns < start_ns
                        or not isinstance(latency, (int, float))
                        or not math.isfinite(latency)
                        or latency < 0
                    ):
                        malformed += 1
                        continue
                    count += 1
                    if request_id in all_request_ids:
                        duplicate += 1
                    else:
                        all_request_ids.add(request_id)
                    if window_start <= start_ns <= end_ns <= window_end:
                        common_count += 1
                except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                    malformed += 1
        reported = exported["request_count"]["avg"]
        check(f"record_count_{index}", count == reported)
        check(f"record_integrity_{index}", malformed == 0 and duplicate == 0)
        client_records.append(
            {
                "index": index,
                "requests_reported": int(reported),
                "records_valid": count,
                "common_window_requests": common_count,
                "malformed_records": malformed,
                "duplicate_request_ids": duplicate,
                "raw_record_path": str(path.relative_to(result_dir)),
                "raw_record_sha256": digest.hexdigest(),
                "raw_summary_sha256": hash_file(
                    result_dir / "raw_aiperf" / job / str(index) / "profile_export_aiperf.json"
                ),
                "client_start_time": exported["start_time"],
                "client_end_time": exported["end_time"],
                "summed_client_rps_contextual": exported["request_throughput"]["avg"],
                "reported_metrics": {
                    key: value
                    for key, value in exported.items()
                    if isinstance(value, dict) and isinstance(value.get("avg"), (int, float))
                },
            }
        )
    check("all_record_files_present", len(client_records) == 6)
    totals = {
        "window_start_ns": window_start,
        "window_end_ns": window_end,
        "window_seconds": plan["measurement"]["common_window_seconds"],
        "common_window_successful_requests": sum(item["common_window_requests"] for item in client_records),
        "common_window_rps": sum(item["common_window_requests"] for item in client_records)
        / plan["measurement"]["common_window_seconds"],
        "all_profile_requests": sum(item["records_valid"] for item in client_records),
        "client_start_spread_seconds": (max(starts) - min(starts)) / 1_000_000_000,
    }
    return write_arm(result_dir, job, plan, plan_hash, checks, blockers, totals, client_records)


def write_arm(
    result_dir: Path,
    job: str,
    plan: dict,
    plan_hash: str,
    checks: dict[str, bool],
    blockers: list[str],
    totals: dict,
    clients: list[dict],
) -> int:
    audit = {
        "status": "valid" if not blockers else "invalid",
        "scope": "record-complete common-window gateway diagnosis",
        "job": job,
        "benchmark_series_id": plan["benchmark_series_id"],
        "plan_sha256": plan_hash,
        "checks": checks,
        "blockers": blockers,
        "parser": "Python standard-library json streaming over immutable AIPerf 0.12 records",
        "next_action": "continue_analysis" if not blockers else "rerun_benchmark",
    }
    summary = {
        "job": job,
        "benchmark_series_id": plan["benchmark_series_id"],
        "plan_sha256": plan_hash,
        **totals,
        "clients": clients,
        "limitations": [
            "Synthetic workers do not measure GPU inference or NIXL capacity.",
            "Record export is a new measurement series; do not compare its absolute rate to summary-only trials.",
            "One valid trial per arm cannot establish a small performance difference without a measured noise floor.",
        ],
    }
    for prefix, body in (("audit", audit), ("summary", summary)):
        with (result_dir / f"{prefix}-{job}.json").open("w", encoding="utf-8") as stream:
            json.dump(body, stream, indent=2, allow_nan=False)
            stream.write("\n")
    print(json.dumps({"job": job, "status": audit["status"], "blockers": blockers,
                      "common_window_rps": totals.get("common_window_rps")}))
    return 0 if not blockers else 1


def compare(result_dir: Path, trial: str, plan: dict, plan_hash: str) -> int:
    reports = {}
    for arm in ("agw", "envoy"):
        job = arm_job(arm, trial)
        audit = read_json(result_dir / f"audit-{job}.json")
        summary = read_json(result_dir / f"summary-{job}.json")
        if audit["status"] != "valid" or audit["plan_sha256"] != plan_hash:
            raise SystemExit(f"cannot compare invalid or mismatched run: {job}")
        reports[arm] = summary
    agw = reports["agw"]["common_window_rps"]
    envoy = reports["envoy"]["common_window_rps"]
    pilot = []
    for pilot_trial in ("r1", "r2", "r3"):
        pilot_job = arm_job("agw", pilot_trial)
        audit_path = result_dir / f"audit-{pilot_job}.json"
        summary_path = result_dir / f"summary-{pilot_job}.json"
        if not audit_path.is_file() or not summary_path.is_file():
            continue
        pilot_audit = read_json(audit_path)
        pilot_summary = read_json(summary_path)
        if pilot_audit["status"] != "valid" or pilot_audit["plan_sha256"] != plan_hash:
            continue
        pilot.append(pilot_summary["common_window_rps"])
    pilot_complete = len(pilot) == 3
    if pilot_complete:
        median_agw = statistics.median(pilot)
        full_range_pct = (max(pilot) - min(pilot)) / median_agw * 100
        noise_floor = {
            "method": "half of the observed three-run AGW range divided by its median",
            "percent": full_range_pct / 2,
            "agw_pilot_rps": pilot,
        }
        minimum_detectable_effect = {
            "method": "full observed three-run AGW range divided by its median; operational, not a confidence interval",
            "percent": full_range_pct,
        }
        observed_change = (envoy - median_agw) / median_agw * 100
        within_spread = abs(observed_change) <= full_range_pct
        verdict = "inconclusive_within_empirical_spread" if within_spread else "inconclusive_requires_paired_confirmation"
        repeat_decision = "not_needed" if within_spread else "necessary"
        reason = (
            "The single Envoy result lies within the AGW three-run observed spread; this series cannot distinguish a gateway-specific difference."
            if within_spread
            else "The Envoy result exceeds the AGW pilot spread, but paired confirmation is needed before a gateway ranking."
        )
    else:
        median_agw = agw
        noise_floor = None
        minimum_detectable_effect = None
        observed_change = (envoy - agw) / agw * 100
        verdict = "inconclusive_until_noise_floor_measured"
        repeat_decision = "necessary"
        reason = "One valid record-level run per arm; common windows remove start-skew accounting but do not establish run-to-run uncertainty."
    analysis = {
        "benchmark_series_id": plan["benchmark_series_id"],
        "plan_sha256": plan_hash,
        "metric": "common_window_successful_requests_per_second",
        "agw_rps": agw,
        "envoy_rps": envoy,
        "envoy_signed_percent_change": (envoy - agw) / agw * 100,
        "agw_pilot_median_rps": median_agw,
        "envoy_vs_agw_pilot_median_percent": observed_change,
        "series_noise_floor": noise_floor,
        "minimum_detectable_effect": minimum_detectable_effect,
        "verdict": verdict,
        "reason": reason,
        "repeat_decision": repeat_decision,
        "repeat_rationale": (
            "The once-per-series AGW n=3 noise-floor pilot is complete; another run is not justified solely by a delta within its observed spread."
            if pilot_complete and repeat_decision == "not_needed"
            else "series noise-floor pilot (n=3 total), or paired confirmation if the observed difference exceeds that pilot"
        ),
        "pilot_configuration": "pd-agw-generic",
        "pilot_target_valid_runs": 3,
        "jobs": {arm: reports[arm]["job"] for arm in reports},
    }
    with (result_dir / f"analysis-{trial}.json").open("w", encoding="utf-8") as stream:
        json.dump(analysis, stream, indent=2, allow_nan=False)
        stream.write("\n")
    print(json.dumps(analysis))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result_dir", type=Path)
    parser.add_argument("mode", choices=("agw", "envoy", "compare"))
    parser.add_argument("trial")
    args = parser.parse_args()
    plan_path = args.result_dir / "benchmark_plan.json"
    plan = read_json(plan_path)
    plan_hash = hash_file(plan_path)
    if args.mode == "compare":
        return compare(args.result_dir, args.trial, plan, plan_hash)
    return audit_arm(args.result_dir, args.mode, args.trial, plan, plan_hash)


if __name__ == "__main__":
    raise SystemExit(main())
