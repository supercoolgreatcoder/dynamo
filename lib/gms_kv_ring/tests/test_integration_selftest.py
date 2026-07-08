# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Startup patch-contract self-test (X9, redesign 5)."""

import pytest
from gpu_memory_service.integrations.common import integration_selftest as st


@pytest.fixture(autouse=True)
def _clean_registry():
    st.reset_for_test()
    yield
    st.reset_for_test()


def test_noop_when_leases_disabled(monkeypatch):
    monkeypatch.delenv("GMS_KV_LEASES", raising=False)
    monkeypatch.delenv("GMS_SGLANG_KV_LEASES", raising=False)
    # Not installed + leases off -> must not raise.
    st.gms_verify_integration("sglang")


def test_raises_when_leases_enabled_but_patch_absent(monkeypatch):
    monkeypatch.setenv("GMS_KV_LEASES", "1")
    with pytest.raises(RuntimeError, match="did not take effect"):
        st.gms_verify_integration("sglang")


def test_passes_when_leases_enabled_and_patch_marked(monkeypatch):
    monkeypatch.setenv("GMS_KV_LEASES", "1")
    st.mark_installed("sglang", "kv_leases")
    st.gms_verify_integration("sglang")  # no raise


def test_per_engine_env_overrides_global(monkeypatch):
    monkeypatch.delenv("GMS_KV_LEASES", raising=False)
    monkeypatch.setenv("GMS_TRTLLM_KV_LEASES", "1")
    with pytest.raises(RuntimeError):
        st.gms_verify_integration("trtllm")
    # A different engine without its own flag stays disabled -> no raise.
    st.gms_verify_integration("vllm")


def test_marks_are_engine_scoped(monkeypatch):
    monkeypatch.setenv("GMS_KV_LEASES", "1")
    st.mark_installed("vllm", "kv_leases")
    st.gms_verify_integration("vllm")  # marked -> ok
    with pytest.raises(RuntimeError):
        st.gms_verify_integration("sglang")  # not marked -> fatal
