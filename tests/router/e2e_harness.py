# SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import json
import logging
import os
import time
from contextlib import ExitStack, contextmanager
from typing import Any

from tests.router.common import (
    _test_router_basic,
    _test_router_decisions,
    _test_router_decisions_disagg,
    _test_router_indexers_sync,
)
from tests.router.helper import (
    generate_random_suffix,
    get_runtime,
    send_inflight_request_payloads,
    wait_for_frontend_ready,
)
from tests.router.router_process import FrontendRouterProcess
from tests.utils.constants import DefaultPort
from tests.utils.port_utils import allocate_ports, deallocate_ports
from tests.utils.test_output import resolve_test_output_path

logger = logging.getLogger(__name__)


def resolve_router_gpu_start_index(gpu_start_index: int) -> int:
    override = os.environ.get("DYNAMO_ROUTER_E2E_GPU_START_INDEX")
    if override is None:
        return gpu_start_index
    try:
        base_index = int(override)
    except ValueError as exc:
        raise ValueError(
            "DYNAMO_ROUTER_E2E_GPU_START_INDEX must be an integer"
        ) from exc
    return base_index + gpu_start_index


def _resolve_router_e2e_num_requests(default: int = 10) -> int:
    override = os.environ.get("DYNAMO_ROUTER_E2E_NUM_REQUESTS")
    if override is None:
        return default
    try:
        value = int(override)
    except ValueError as exc:
        raise ValueError("DYNAMO_ROUTER_E2E_NUM_REQUESTS must be an integer") from exc
    if value <= 0:
        raise ValueError("DYNAMO_ROUTER_E2E_NUM_REQUESTS must be positive")
    return value


@contextmanager
def maybe_router_gms_servers():
    if os.environ.get("DYNAMO_ROUTER_E2E_ENABLE_GMS") != "1":
        yield
        return

    from tests.gpu_memory_service.common.gms import GMSServer

    devices = os.environ.get("DYNAMO_ROUTER_E2E_GMS_DEVICES", "0,1")
    tags = os.environ.get("DYNAMO_ROUTER_E2E_GMS_TAGS", "weights,kv_cache")
    device_ids = [int(part.strip()) for part in devices.split(",") if part.strip()]
    tag_names = [part.strip() for part in tags.split(",") if part.strip()]

    with ExitStack() as stack:
        for device in device_ids:
            for tag in tag_names:
                stack.enter_context(GMSServer(device=device, tag=tag))
        yield


TEST_PROMPT = (
    "In a quiet meadow tucked between rolling hills, a plump gray rabbit nibbled on "
    "clover beneath the shade of a gnarled oak tree. Its ears twitched at the faint "
    "rustle of leaves, but it remained calm, confident in the safety of its burrow "
    "just a few hops away. The late afternoon sun warmed its fur, and tiny dust "
    "motes danced in the golden light as bees hummed lazily nearby. Though the "
    "rabbit lived a simple life, every day was an adventure of scents, shadows, and "
    "snacks-an endless search for the tastiest patch of greens and the softest spot "
    "to nap."
)


def allocate_frontend_ports(request, count: int) -> list[int]:
    ports = allocate_ports(count, DefaultPort.FRONTEND.value)
    request.addfinalizer(lambda: deallocate_ports(ports))
    return ports


def build_test_payload(model_name: str) -> dict[str, Any]:
    return {
        "model": model_name,
        "messages": [{"role": "user", "content": TEST_PROMPT}],
        "stream": True,
        "max_tokens": 10,
    }


def _repo_root() -> str:
    return os.environ.get(
        "DYNAMO_REPO_ROOT",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")),
    )


def _resolve_path(path: str) -> str:
    return path if os.path.isabs(path) else os.path.join(_repo_root(), path)


def _resolve_int_env(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return value


def _load_mooncake_trace_rows(
    trace_path: str, *, offset: int, limit: int
) -> list[dict]:
    rows: list[dict] = []
    with open(trace_path, encoding="utf-8") as trace_file:
        for line_idx, line in enumerate(trace_file):
            if line_idx < offset:
                continue
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
            if len(rows) >= limit:
                break
    if not rows:
        raise ValueError(f"no Mooncake trace rows loaded from {trace_path}")
    return rows


def _load_stress_tokenizer(model_name: str):
    try:
        from transformers import AutoTokenizer
    except ImportError:
        logger.warning("transformers is unavailable; using approximate prompt sizing")
        return None

    try:
        return AutoTokenizer.from_pretrained(
            model_name, local_files_only=True, trust_remote_code=True
        )
    except Exception as exc:
        logger.warning(
            "failed to load tokenizer for %s; using approximate prompt sizing: %s",
            model_name,
            exc,
        )
        return None


def _build_mooncake_prompt(
    tokenizer, row: dict, *, request_idx: int, target_tokens: int
) -> str:
    hash_ids = row.get("hash_ids") or [request_idx]
    words = [f"trace_{request_idx}"]
    for pos in range(max(64, target_tokens * 2)):
        hash_id = hash_ids[pos % len(hash_ids)]
        words.append(f"kv_{hash_id}_{pos % 97}")
    raw_prompt = " ".join(words)

    if tokenizer is None:
        # Keep fallback prompts conservative. Synthetic hash words can tokenize
        # into many subword pieces on model-specific tokenizers, which can turn
        # an approximate 512-token prompt into several thousand real tokens.
        approx_words = [f"trace_{request_idx}"] + ["x"] * max(1, target_tokens)
        return " ".join(approx_words)

    token_ids = tokenizer.encode(raw_prompt, add_special_tokens=False)
    while len(token_ids) < target_tokens:
        raw_prompt = raw_prompt + " " + raw_prompt
        token_ids = tokenizer.encode(raw_prompt, add_special_tokens=False)
    return tokenizer.decode(token_ids[:target_tokens], skip_special_tokens=True)


def build_mooncake_trace_payloads(model_name: str) -> list[dict[str, Any]]:
    default_trace_path = os.path.join(
        "lib", "bench", "testdata", "mooncake_trace_1000.jsonl"
    )
    trace_path = _resolve_path(
        os.environ.get("DYNAMO_ROUTER_E2E_MOONCAKE_TRACE", default_trace_path)
    )
    trace_requests = _resolve_int_env(
        "DYNAMO_ROUTER_E2E_MOONCAKE_TRACE_REQUESTS",
        _resolve_router_e2e_num_requests(),
    )
    trace_offset = _resolve_int_env(
        "DYNAMO_ROUTER_E2E_MOONCAKE_TRACE_OFFSET", 0, minimum=0
    )
    repeat = _resolve_int_env("DYNAMO_ROUTER_E2E_MOONCAKE_REPEAT", 1)
    max_input_tokens = _resolve_int_env(
        "DYNAMO_ROUTER_E2E_MOONCAKE_MAX_INPUT_TOKENS", 768
    )
    max_output_tokens = _resolve_int_env(
        "DYNAMO_ROUTER_E2E_MOONCAKE_MAX_OUTPUT_TOKENS", 64
    )

    rows = _load_mooncake_trace_rows(
        trace_path, offset=trace_offset, limit=trace_requests
    )
    tokenizer = _load_stress_tokenizer(model_name)
    payloads: list[dict[str, Any]] = []
    for repeat_idx in range(repeat):
        for row_idx, row in enumerate(rows):
            request_idx = repeat_idx * len(rows) + row_idx
            target_input = max(
                1, min(int(row.get("input_length", 1)), max_input_tokens)
            )
            target_output = max(
                1, min(int(row.get("output_length", 1)), max_output_tokens)
            )
            payloads.append(
                {
                    "model": model_name,
                    "messages": [
                        {
                            "role": "user",
                            "content": _build_mooncake_prompt(
                                tokenizer,
                                row,
                                request_idx=request_idx,
                                target_tokens=target_input,
                            ),
                        }
                    ],
                    "stream": True,
                    "max_tokens": target_output,
                    "temperature": 0.0,
                }
            )

    logger.info(
        "Loaded Mooncake trace stress payloads: trace=%s rows=%d repeat=%d "
        "payloads=%d max_input_tokens=%d max_output_tokens=%d",
        trace_path,
        len(rows),
        repeat,
        len(payloads),
        max_input_tokens,
        max_output_tokens,
    )
    return payloads


class ManagedEngineProcessMixin:
    process_name = "worker"
    cleanup_name = "worker resources"
    init_delay_seconds = 5
    init_delay_reason = "initialize before starting next worker"
    cleanup_delay_seconds = 2

    def __enter__(self):
        logger.info(
            "[%s] Starting %d worker processes sequentially...",
            self.__class__.__name__,
            len(self.worker_processes),
        )

        for i, process in enumerate(self.worker_processes):
            logger.info(
                "[%s] Starting %s %d...", self.__class__.__name__, self.process_name, i
            )
            try:
                process._logger = logging.getLogger(process.__class__.__name__)
                process._command_name = process.command[0]
                process.log_dir = resolve_test_output_path(process.log_dir)
                os.makedirs(process.log_dir, exist_ok=True)
                log_name = f"{process._command_name}.log.txt"
                process._log_path = os.path.join(process.log_dir, log_name)

                if process.data_dir:
                    process._remove_directory(process.data_dir)

                process._terminate_all_matching_process_names()
                logger.info(
                    "[%s] Launching process %d (pid will be assigned)...",
                    self.__class__.__name__,
                    i,
                )
                process._start_process()
                logger.info(
                    "[%s] Worker %d launched with PID: %s",
                    self.__class__.__name__,
                    i,
                    process.proc.pid if process.proc else "unknown",
                )
                time.sleep(process.delayed_start)

                if i < len(self.worker_processes) - 1:
                    logger.info(
                        "[%s] Waiting %ss for worker %d to %s...",
                        self.__class__.__name__,
                        self.init_delay_seconds,
                        i,
                        self.init_delay_reason,
                    )
                    time.sleep(self.init_delay_seconds)

            except Exception:
                logger.exception(
                    "[%s] Failed to start worker %d", self.__class__.__name__, i
                )
                try:
                    process.__exit__(None, None, None)
                except Exception as cleanup_err:
                    logger.warning(
                        "[%s] Error during cleanup: %s",
                        self.__class__.__name__,
                        cleanup_err,
                    )
                raise

        logger.info(
            "[%s] All %d workers launched with sequential initialization.",
            self.__class__.__name__,
            len(self.worker_processes),
        )
        logger.info(
            "[%s] Waiting for health checks to complete...", self.__class__.__name__
        )

        for i, process in enumerate(self.worker_processes):
            logger.info(
                "[%s] Checking health for worker %d...", self.__class__.__name__, i
            )
            try:
                elapsed = process._check_ports(process.timeout)
                process._check_urls(process.timeout - elapsed)
                process._check_funcs(process.timeout - elapsed)
                logger.info(
                    "[%s] Worker %d health checks passed", self.__class__.__name__, i
                )
            except Exception:
                logger.error(
                    "[%s] Worker %d health check failed", self.__class__.__name__, i
                )
                self.__exit__(None, None, None)
                raise

        logger.info(
            "[%s] All workers started successfully and passed health checks!",
            self.__class__.__name__,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for i, process in enumerate(self.worker_processes):
            logger.info("Stopping %s %d", self.process_name, i)
            process.__exit__(exc_type, exc_val, exc_tb)

        logger.info("Waiting for %s to fully clean up...", self.cleanup_name)
        time.sleep(self.cleanup_delay_seconds)


def get_engine_endpoint(engine_workers, request_plane: str, component_name: str):
    runtime = get_runtime(request_plane=request_plane)
    return runtime.endpoint(f"{engine_workers.namespace}.{component_name}.generate")


def run_basic_router_test(
    *,
    engine_process_cls,
    engine_args_name: str,
    engine_args: dict[str, Any],
    num_workers: int,
    single_gpu: bool,
    request,
    request_plane: str,
    block_size: int,
    model_name: str,
    frontend_timeout: int = 180,
):
    with maybe_router_gms_servers():
        with engine_process_cls(
            request,
            num_workers=num_workers,
            single_gpu=single_gpu,
            request_plane=request_plane,
            **{engine_args_name: engine_args},
        ) as engine_workers:
            frontend_port = allocate_frontend_ports(request, 1)[0]
            _test_router_basic(
                engine_workers=engine_workers,
                block_size=block_size,
                request=request,
                frontend_port=frontend_port,
                test_payload=build_test_payload(model_name),
                num_requests=_resolve_router_e2e_num_requests(),
                frontend_timeout=frontend_timeout,
                store_backend="etcd",
                request_plane=request_plane,
            )


def run_mooncake_router_stress_test(
    *,
    engine_process_cls,
    engine_args_name: str,
    engine_args: dict[str, Any],
    num_workers: int,
    single_gpu: bool,
    request,
    request_plane: str,
    block_size: int,
    model_name: str,
    frontend_timeout: int = 600,
) -> dict[str, Any]:
    with maybe_router_gms_servers():
        with engine_process_cls(
            request,
            num_workers=num_workers,
            single_gpu=single_gpu,
            request_plane=request_plane,
            **{engine_args_name: engine_args},
        ) as engine_workers:
            frontend_port = allocate_frontend_ports(request, 1)[0]
            with FrontendRouterProcess(
                request,
                block_size,
                frontend_port,
                engine_workers.namespace,
                "etcd",
                request_plane=request_plane,
                router_mode="kv",
            ):
                frontend_url = f"http://localhost:{frontend_port}"
                asyncio.run(
                    wait_for_frontend_ready(
                        frontend_url=frontend_url,
                        expected_num_workers=engine_workers.num_workers,
                        timeout=frontend_timeout,
                        engine_workers=engine_workers,
                        store_backend="etcd",
                        request_plane=request_plane,
                    )
                )
                payloads = build_mooncake_trace_payloads(model_name)
                concurrency = _resolve_int_env(
                    "DYNAMO_ROUTER_E2E_MOONCAKE_CONCURRENCY",
                    min(32, len(payloads)),
                )
                timeout_s = _resolve_int_env(
                    "DYNAMO_ROUTER_E2E_MOONCAKE_REQUEST_TIMEOUT_S", 900
                )
                max_retries = _resolve_int_env(
                    "DYNAMO_ROUTER_E2E_MOONCAKE_MAX_RETRIES", 8, minimum=0
                )
                retry_backoff_s = float(
                    os.environ.get("DYNAMO_ROUTER_E2E_MOONCAKE_RETRY_BACKOFF_S", "0.25")
                )

                # Keep benchmark summaries honest: model load readiness only proves
                # the request path works. Backends can still pay one-time kernel
                # compilation/autotune on the first realistic prompt. Run a small
                # trace-shaped warmup set and exclude it from measured latency.
                warmup_requests = _resolve_int_env(
                    "DYNAMO_ROUTER_E2E_MOONCAKE_WARMUP_REQUESTS",
                    min(4, len(payloads)),
                    minimum=0,
                )
                warmup_summary: dict[str, Any] | None = None
                if warmup_requests > 0:
                    warmup_payloads = payloads[: min(warmup_requests, len(payloads))]
                    warmup_concurrency = _resolve_int_env(
                        "DYNAMO_ROUTER_E2E_MOONCAKE_WARMUP_CONCURRENCY",
                        min(concurrency, len(warmup_payloads)),
                    )
                    warmup_summary = asyncio.run(
                        send_inflight_request_payloads(
                            [f"{frontend_url}/v1/chat/completions"],
                            warmup_payloads,
                            max_concurrency=warmup_concurrency,
                            request_timeout_s=timeout_s,
                            max_retries=max_retries,
                            retry_backoff_s=retry_backoff_s,
                            summary_label="Mooncake warmup (excluded)",
                        )
                    )

                summary = asyncio.run(
                    send_inflight_request_payloads(
                        [f"{frontend_url}/v1/chat/completions"],
                        payloads,
                        max_concurrency=concurrency,
                        request_timeout_s=timeout_s,
                        max_retries=max_retries,
                        retry_backoff_s=retry_backoff_s,
                        summary_label="Mooncake measured",
                    )
                )
                if warmup_summary is not None:
                    summary["warmup"] = warmup_summary

                max_p99_ms = os.environ.get("DYNAMO_ROUTER_E2E_MOONCAKE_MAX_P99_MS")
                if max_p99_ms is not None:
                    p99_ms = summary["latency_ms"]["p99"]
                    assert p99_ms <= float(max_p99_ms), (
                        f"Mooncake trace p99 latency {p99_ms:.2f} ms exceeds "
                        f"limit {max_p99_ms} ms"
                    )
                return summary


def run_router_decisions_test(
    *,
    engine_process_cls,
    engine_args_name: str,
    engine_args: dict[str, Any],
    request,
    request_plane: str,
    model_name: str,
    block_size: int,
    component_name: str,
    num_workers: int,
    single_gpu: bool,
    test_dp_rank: bool,
    extra_process_kwargs: dict[str, Any] | None = None,
    initial_wait: float = 0.25,
):
    process_kwargs = extra_process_kwargs or {}
    with maybe_router_gms_servers():
        with engine_process_cls(
            request,
            num_workers=num_workers,
            single_gpu=single_gpu,
            request_plane=request_plane,
            **{engine_args_name: engine_args},
            **process_kwargs,
        ) as engine_workers:
            endpoint = get_engine_endpoint(
                engine_workers, request_plane, component_name
            )
            _test_router_decisions(
                engine_workers,
                endpoint,
                model_name,
                request,
                test_dp_rank=test_dp_rank,
                block_size=block_size,
                initial_wait=initial_wait,
            )


def run_disagg_router_decisions_test(
    *,
    engine_process_cls,
    engine_args_name: str,
    engine_args: dict[str, Any],
    request,
    request_plane: str,
    model_name: str,
    block_size: int,
    num_prefill_workers: int,
    num_decode_workers: int,
    prefill_process_kwargs: dict[str, Any] | None = None,
    decode_process_kwargs: dict[str, Any] | None = None,
    strict_timing: bool = True,
    progressive_request_count: int = 4,
):
    shared_namespace = f"test-namespace-{generate_random_suffix()}"
    frontend_port = allocate_frontend_ports(request, 1)[0]

    prefill_kwargs = {
        "namespace": shared_namespace,
        **(prefill_process_kwargs or {}),
    }
    decode_kwargs = {
        "namespace": shared_namespace,
        **(decode_process_kwargs or {}),
    }

    with maybe_router_gms_servers():
        with engine_process_cls(
            request,
            num_workers=num_prefill_workers,
            request_plane=request_plane,
            **{engine_args_name: engine_args},
            **prefill_kwargs,
        ) as prefill_workers:
            with engine_process_cls(
                request,
                num_workers=num_decode_workers,
                request_plane=request_plane,
                **{engine_args_name: engine_args},
                **decode_kwargs,
            ) as decode_workers:
                _test_router_decisions_disagg(
                    prefill_workers=prefill_workers,
                    decode_workers=decode_workers,
                    block_size=block_size,
                    request=request,
                    frontend_port=frontend_port,
                    test_payload=build_test_payload(model_name),
                    request_plane=request_plane,
                    strict_timing=strict_timing,
                    progressive_request_count=progressive_request_count,
                )


def run_indexers_sync_test(
    *,
    engine_process_cls,
    engine_args_name: str,
    engine_args: dict[str, Any],
    request,
    runtime_services_dynamic_ports,
    store_backend: str,
    durable_kv_events: bool,
    request_plane: str,
    block_size: int,
    model_name: str,
    num_workers: int,
    extra_process_kwargs: dict[str, Any] | None = None,
):
    nats_process, _etcd_process = runtime_services_dynamic_ports
    process_kwargs = extra_process_kwargs or {}

    with maybe_router_gms_servers():
        with engine_process_cls(
            request,
            num_workers=num_workers,
            single_gpu=True,
            request_plane=request_plane,
            store_backend=store_backend,
            durable_kv_events=durable_kv_events,
            **{engine_args_name: engine_args},
            **process_kwargs,
        ) as engine_workers:
            _test_router_indexers_sync(
                engine_workers=engine_workers,
                block_size=block_size,
                model_name=model_name,
                num_workers=num_workers,
                store_backend=store_backend,
                request_plane=request_plane,
                test_nats_interruption=not durable_kv_events,
                nats_server=nats_process if not durable_kv_events else None,
                durable_kv_events=durable_kv_events,
                standalone_indexer_url=getattr(
                    engine_workers, "standalone_indexer_url", None
                ),
                standalone_indexer_b_url=getattr(
                    engine_workers, "standalone_indexer_b_url", None
                ),
                test_zmq_replay=bool(
                    getattr(engine_workers, "standalone_indexer_url", None)
                ),
            )
