#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Audit AIPerf per-client JSONL against one synchronized Mooncake window.

The sum of each client's reported RPS is not a common-window gateway rate.
This script streams all request records and uses one global start/end interval.
"""

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result_dir", type=Path)
    parser.add_argument("job")
    parser.add_argument("--clients", type=int, default=24)
    parser.add_argument("--scheduled-per-client", type=int, default=22699)
    args = parser.parse_args()

    root = args.result_dir / "raw_aiperf" / args.job
    summaries = sorted(root.glob("*/profile_export_aiperf.json"))
    if len(summaries) != args.clients:
        raise SystemExit(f"expected {args.clients} client summaries, got {len(summaries)}")

    clients = []
    seen_requests = set()
    global_start_ns = None
    global_last_start_ns = None
    global_end_ns = None
    total_records = 0
    total_errors = 0
    total_cancelled = 0
    invalid_timestamps = 0
    for summary_path in summaries:
        client_id = summary_path.parent.name
        summary = json.loads(summary_path.read_text())
        records_path = summary_path.parent / "profile_export.jsonl"
        digest = hashlib.sha256()
        record_count = 0
        client_start_ns = None
        client_end_ns = None
        with records_path.open("rb") as records:
            for raw in records:
                digest.update(raw)
                row = json.loads(raw)
                metadata = row["metadata"]
                request_id = metadata["x_request_id"]
                if request_id in seen_requests:
                    raise SystemExit(f"duplicate request ID: {request_id}")
                seen_requests.add(request_id)
                start_ns = metadata["request_start_ns"]
                end_ns = metadata["request_end_ns"]
                if start_ns > end_ns:
                    invalid_timestamps += 1
                client_start_ns = start_ns if client_start_ns is None else min(client_start_ns, start_ns)
                client_end_ns = end_ns if client_end_ns is None else max(client_end_ns, end_ns)
                global_last_start_ns = start_ns if global_last_start_ns is None else max(global_last_start_ns, start_ns)
                total_cancelled += bool(metadata.get("was_cancelled"))
                record_count += 1
        if record_count == 0:
            raise SystemExit(f"no request records for client {client_id}")
        global_start_ns = client_start_ns if global_start_ns is None else min(global_start_ns, client_start_ns)
        global_end_ns = client_end_ns if global_end_ns is None else max(global_end_ns, client_end_ns)
        total_records += record_count
        errors = sum(item.get("count", 0) for item in summary.get("error_summary", []))
        total_errors += errors
        starts_at = datetime.fromisoformat(summary["start_time"]).replace(tzinfo=timezone.utc)
        clients.append({
            "client_id": client_id,
            "record_count": record_count,
            "summary_request_count": summary["request_count"]["avg"],
            "scheduled_count": args.scheduled_per_client,
            "summary_rps": summary["request_throughput"]["avg"],
            "request_latency_mean_ms": summary["request_latency"]["avg"],
            "request_latency_p99_ms": summary["request_latency"]["p99"],
            "replay_schedule_degraded": bool(summary["replay_sched_degraded"]["avg"]),
            "replay_lag_p99_ms": summary["replay_sched_lag_p99"]["avg"],
            "profile_start_utc": starts_at.isoformat(),
            "first_request_start_ns": client_start_ns,
            "last_request_end_ns": client_end_ns,
            "errors": errors,
            "records_bytes": records_path.stat().st_size,
            "records_sha256": digest.hexdigest(),
        })

    start_times = [datetime.fromisoformat(client["profile_start_utc"]) for client in clients]
    window_seconds = (global_end_ns - global_start_ns) / 1e9
    arrival_window_seconds = (global_last_start_ns - global_start_ns) / 1e9
    scheduled_count = args.clients * args.scheduled_per_client
    cache_path = args.result_dir / f"cache-{args.job}.tsv"
    cache_hits = len(cache_path.read_text().splitlines()) if cache_path.is_file() else 0
    record_counts_match = all(
        client["record_count"] == client["summary_request_count"] + client["errors"]
        for client in clients
    )
    degraded_count = sum(client["replay_schedule_degraded"] for client in clients)
    replay_lag_p99_ms_max = max(client["replay_lag_p99_ms"] for client in clients)
    start_spread_seconds = (max(start_times) - min(start_times)).total_seconds()
    successful_requests = total_records - total_errors - total_cancelled
    gates = {
        "all_request_records_present": record_counts_match,
        "all_scheduled_requests_completed": successful_requests == scheduled_count,
        "no_record_timestamp_inversion": invalid_timestamps == 0,
        "zero_errors": total_errors == 0,
        "zero_cancelled_records": total_cancelled == 0,
        "all_client_cache_hits": cache_hits == args.clients,
        "no_degraded_clients": degraded_count == 0,
        "replay_lag_p99_ms_at_most_500": replay_lag_p99_ms_max <= 500,
        "profile_start_spread_seconds_at_most_3": start_spread_seconds <= 3,
    }
    result = {
        "job": args.job,
        "clients": args.clients,
        "scheduled_requests": scheduled_count,
        "recorded_requests": total_records,
        "successful_requests": successful_requests,
        "completion_fraction": successful_requests / scheduled_count,
        "summary_rps_sum_contextual_only": sum(client["summary_rps"] for client in clients),
        "global_window_start_utc": datetime.fromtimestamp(global_start_ns / 1e9, tz=timezone.utc).isoformat(),
        "global_window_end_utc": datetime.fromtimestamp(global_end_ns / 1e9, tz=timezone.utc).isoformat(),
        "global_window_seconds": window_seconds,
        "arrival_window_seconds": arrival_window_seconds,
        "synchronized_arrival_rps": total_records / arrival_window_seconds,
        "synchronized_recorded_rps": total_records / window_seconds,
        "synchronized_successful_rps": successful_requests / window_seconds,
        "profile_start_spread_seconds": start_spread_seconds,
        "degraded_clients": degraded_count,
        "replay_lag_p99_ms_max": replay_lag_p99_ms_max,
        "errors": total_errors,
        "cancelled_records": total_cancelled,
        "cache_hits": cache_hits,
        "quality_gates": gates,
        "capacity_valid": all(gates.values()),
        "raw_records_location": str(root),
        "raw_records_vcluster_nfs_path": f"/shared/nix/aiperf/results/{args.job}",
        "per_client": clients,
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
