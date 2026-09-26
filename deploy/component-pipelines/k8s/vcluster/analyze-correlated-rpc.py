#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Join opt-in gateway and facade RPC samples by request ID."""

import argparse
import json
import re
from datetime import datetime
from pathlib import Path

FIELD = re.compile(r"(?:^|\s)([a-z_][a-z_0-9]*)=([^\s]+)")


def events(paths: list[Path], target: str, **filters: str) -> dict[str, dict]:
    found = {}
    for path in paths:
        with path.open(encoding="utf-8") as log:
            for line in log:
                if f"\t{target}\t" not in line:
                    continue
                fields = dict(FIELD.findall(line))
                if any(fields.get(key) != value for key, value in filters.items()):
                    continue
                request_id = fields.get("request_id")
                if request_id is None or not request_id.endswith("00"):
                    continue
                if request_id in found:
                    raise ValueError(f"duplicate {target} sample for {request_id}")
                timestamp = line.split("\t", 1)[0].replace("Z", "+00:00")
                fields["timestamp_us"] = int(
                    datetime.fromisoformat(timestamp).timestamp() * 1e6
                )
                found[request_id] = fields
    return found


def stats(values: list[int]) -> dict:
    if not values:
        return {"count": 0}
    ordered = sorted(values)
    return {
        "count": len(values),
        "mean_us": sum(values) / len(values),
        "p50_us": ordered[len(values) // 2],
        "p95_us": ordered[min(len(values) - 1, int(len(values) * 0.95))],
        "min_us": ordered[0],
        "max_us": ordered[-1],
        "negative_count": sum(value < 0 for value in values),
    }


def prefill(gateway: dict, handlers: dict, terminals: dict) -> dict:
    joined = gateway.keys() & handlers.keys() & terminals.keys()
    measures = {
        "gateway_header_wait": [],
        "facade_handler": [],
        "outside_facade_handler": [],
        "gateway_start_to_facade_entry_cross_clock": [],
        "facade_handler_to_gateway_headers_cross_clock": [],
        "gateway_handoff_stream": [],
        "facade_terminal_to_gateway_end_cross_clock": [],
    }
    for request_id in joined:
        client = gateway[request_id]
        handler = handlers[request_id]
        terminal = terminals[request_id]
        header_us = int(client["headers_us"])
        stream_us = int(client["stream_us"])
        handler_us = int(handler["elapsed_us"])
        gateway_end = client["timestamp_us"]
        gateway_start = gateway_end - header_us - stream_us
        gateway_headers = gateway_end - stream_us
        facade_handler_done = handler["timestamp_us"]
        facade_entry = facade_handler_done - handler_us
        measures["gateway_header_wait"].append(header_us)
        measures["facade_handler"].append(handler_us)
        measures["outside_facade_handler"].append(header_us - handler_us)
        measures["gateway_start_to_facade_entry_cross_clock"].append(
            facade_entry - gateway_start
        )
        measures["facade_handler_to_gateway_headers_cross_clock"].append(
            gateway_headers - facade_handler_done
        )
        measures["gateway_handoff_stream"].append(stream_us)
        measures["facade_terminal_to_gateway_end_cross_clock"].append(
            gateway_end - terminal["timestamp_us"]
        )
    return {
        "gateway_samples": len(gateway),
        "facade_handler_samples": len(handlers),
        "facade_terminal_samples": len(terminals),
        "joined_samples": len(joined),
        "metrics": {name: stats(values) for name, values in measures.items()},
    }


def selector(gateway: dict, handlers: dict) -> dict:
    joined = gateway.keys() & handlers.keys()
    measures = {
        "gateway_rpc_wait": [],
        "facade_handler": [],
        "outside_facade_handler": [],
        "gateway_start_to_facade_entry_cross_clock": [],
        "facade_handler_to_gateway_return_cross_clock": [],
    }
    for request_id in joined:
        client = gateway[request_id]
        handler = handlers[request_id]
        rpc_us = int(client["elapsed_us"])
        handler_us = int(handler["elapsed_us"])
        gateway_end = client["timestamp_us"]
        facade_done = handler["timestamp_us"]
        measures["gateway_rpc_wait"].append(rpc_us)
        measures["facade_handler"].append(handler_us)
        measures["outside_facade_handler"].append(rpc_us - handler_us)
        measures["gateway_start_to_facade_entry_cross_clock"].append(
            facade_done - handler_us - (gateway_end - rpc_us)
        )
        measures["facade_handler_to_gateway_return_cross_clock"].append(
            gateway_end - facade_done
        )
    return {
        "gateway_samples": len(gateway),
        "facade_handler_samples": len(handlers),
        "joined_samples": len(joined),
        "metrics": {name: stats(values) for name, values in measures.items()},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result_dir", type=Path)
    parser.add_argument("job")
    args = parser.parse_args()
    directory = args.result_dir
    gateway_log = directory / f"gateway-{args.job}.log"
    prefill_logs = sorted(directory.glob(f"prefill-{args.job}-*.log"))
    selector_logs = sorted(directory.glob(f"selector-{args.job}-*.log"))
    if not gateway_log.is_file() or not prefill_logs or not selector_logs:
        parser.error("gateway, prefill, and selector logs for the job are required")
    gateway_prefill = events([gateway_log], "dynamo_static_rpc_split")
    gateway_selector = events([gateway_log], "dynamo_static_selector_rpc")
    prefill_handlers = events(
        prefill_logs,
        "dynamo_component_rpc_sample",
        component="worker",
        operation="generate_raw",
        phase="handler",
    )
    prefill_terminals = events(
        prefill_logs,
        "dynamo_component_rpc_sample",
        component="worker",
        operation="generate_raw",
        phase="terminal",
    )
    selector_handlers = events(
        selector_logs,
        "dynamo_component_rpc_sample",
        component="selector",
        operation="select",
    )
    result = {
        "job": args.job,
        "prefill": prefill(gateway_prefill, prefill_handlers, prefill_terminals),
        "selector": selector(gateway_selector, selector_handlers),
        "clock_note": (
            "Cross-clock segments require synchronized Pod-node clocks; "
            "outside-facade durations do not."
        ),
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
