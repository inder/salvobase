"""
Tests for the per-night variance gate in scripts/bench/check_perf_gap.py.

These tests do NOT call GitHub. They synthesize input rows, exercise the pure
helper functions (compute_per_night, compute_median_ratio, variance_gate), and
also monkeypatch the gh() helper to assert that handle_condition_0 honors the
gate.

Run from repo root:
    python3 -m pytest tests/bench -v
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import types
import unittest.mock as mock

import pytest


# ---------------------------------------------------------------------------
# Module loader — bench scripts live outside any package, so we load them by path.
# ---------------------------------------------------------------------------

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _load_module(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader, f"could not load {path}"
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


check_perf_gap = _load_module(
    "check_perf_gap", REPO_ROOT / "scripts/bench/check_perf_gap.py"
)
print_ratios = _load_module(
    "print_ratios", REPO_ROOT / "scripts/bench/print_ratios.py"
)


# ---------------------------------------------------------------------------
# Synthetic data helpers
# ---------------------------------------------------------------------------

WORKLOADS = ["A", "B", "C", "D", "F"]
THREADS = [1, 4, 8, 16]


def synth_night(
    date: str,
    salvobase_ops: float,
    mongo_ops: float,
    samples_per_combo: int = 3,
    jitter: float = 0.0,
) -> list[dict]:
    """
    Produce a list of result rows for one date. Each (wl, threads, target)
    combination emits `samples_per_combo` rows with the same nominal ops, plus
    optional ±jitter applied symmetrically so the median stays at the target.
    """
    rows: list[dict] = []
    offsets = [0.0] if samples_per_combo == 1 else [-jitter, 0.0, jitter][:samples_per_combo]
    for wl in WORKLOADS:
        for t in THREADS:
            for offset in offsets:
                rows.append({
                    "workload": wl,
                    "threads": t,
                    "target": "salvobase",
                    "ops_per_sec": salvobase_ops + offset,
                    "p50_ms": 1.0,
                    "p99_ms": 5.0,
                    "date": date,
                })
                rows.append({
                    "workload": wl,
                    "threads": t,
                    "target": "mongodb",
                    "ops_per_sec": mongo_ops,
                    "p50_ms": 0.5,
                    "p99_ms": 2.0,
                    "date": date,
                })
    return rows


def synth_history(ratios_newest_first: list[float], mongo_ops: float = 1000.0) -> list[dict]:
    """
    Build a history given a list of desired per-night ratios, newest first.
    Day numbering: 2026-05-29, 2026-05-28, … going back.
    """
    rows: list[dict] = []
    base_day = 29
    base_month = 5
    for i, ratio in enumerate(ratios_newest_first):
        d = base_day - i
        # Keep dates simple — they only need to sort lexicographically.
        date = f"2026-{base_month:02d}-{d:02d}" if d > 0 else f"2026-04-{(d + 30):02d}"
        rows.extend(synth_night(date, ratio * mongo_ops, mongo_ops))
    return rows


# ---------------------------------------------------------------------------
# compute_per_night — pivot + aggregation
# ---------------------------------------------------------------------------

class TestComputePerNight:
    def test_aggregates_samples_to_per_night_median(self):
        rows = synth_night("2026-05-29", salvobase_ops=500, mongo_ops=1000, samples_per_combo=3)
        night_overall, night_per_wl = check_perf_gap.compute_per_night(rows)
        assert "2026-05-29" in night_overall
        assert night_overall["2026-05-29"] == pytest.approx(0.5, rel=1e-6)
        # All workloads should appear
        assert set(night_per_wl["2026-05-29"].keys()) == set(WORKLOADS)
        for wl in WORKLOADS:
            assert night_per_wl["2026-05-29"][wl] == pytest.approx(0.5, rel=1e-6)

    def test_ignores_workload_e(self):
        rows = synth_night("2026-05-29", 500, 1000)
        rows.extend([
            {"workload": "E", "threads": 16, "target": "salvobase",
             "ops_per_sec": 0.01, "p50_ms": 1, "p99_ms": 1, "date": "2026-05-29"},
            {"workload": "E", "threads": 16, "target": "mongodb",
             "ops_per_sec": 1000.0, "p50_ms": 1, "p99_ms": 1, "date": "2026-05-29"},
        ])
        _, night_per_wl = check_perf_gap.compute_per_night(rows)
        assert "E" not in night_per_wl["2026-05-29"]

    def test_handles_missing_mongo_pair(self):
        rows = [
            {"workload": "A", "threads": 1, "target": "salvobase",
             "ops_per_sec": 500.0, "p50_ms": 1, "p99_ms": 1, "date": "2026-05-29"},
            # No mongodb partner — should be skipped, not crash.
        ]
        night_overall, night_per_wl = check_perf_gap.compute_per_night(rows)
        assert night_overall == {}
        assert night_per_wl == {}


# ---------------------------------------------------------------------------
# variance_gate — the core deliverable
# ---------------------------------------------------------------------------

class TestVarianceGate:
    def test_should_not_alert_when_within_2_sigma(self):
        # 10 nights at ~37% with ±1pp jitter — current 3-night median ≈ trailing 7-night median.
        ratios = [0.38, 0.37, 0.37, 0.37, 0.38, 0.37, 0.36, 0.37, 0.38, 0.37]
        rows = synth_history(ratios)
        should_alert, reason = check_perf_gap.variance_gate(rows)
        assert should_alert is False, f"expected silence, got alert: {reason}"
        assert "no significant change" in reason

    def test_should_alert_when_more_than_2_sigma_below_trailing(self):
        # Trailing 7 nights cluster tightly around 37%; current 3 nights drop to 22%.
        trailing = [0.37, 0.38, 0.36, 0.37, 0.38, 0.37, 0.36]
        current = [0.22, 0.21, 0.22]
        rows = synth_history(current + trailing)
        should_alert, reason = check_perf_gap.variance_gate(rows)
        assert should_alert is True, f"expected alert, got silence: {reason}"
        assert "significant regression" in reason
        assert "σ below" in reason

    def test_should_alert_when_trailing_window_has_fewer_than_3_nights(self):
        # Only 5 total nights → trailing window is 2 → must bypass the gate.
        ratios = [0.20, 0.21, 0.22, 0.37, 0.38]
        rows = synth_history(ratios)
        should_alert, reason = check_perf_gap.variance_gate(rows)
        assert should_alert is True
        assert "bypassing gate" in reason

    def test_should_alert_when_only_one_night_of_data(self):
        rows = synth_history([0.20])
        should_alert, reason = check_perf_gap.variance_gate(rows)
        assert should_alert is True
        assert "bypassing gate" in reason

    def test_silences_one_sigma_drops_on_the_measured_noise_floor(self):
        # 6pp stddev is the measured noise floor (see project_bench_noise_floor.md).
        # A 7pp drop is roughly 1σ — must NOT alert.
        trailing = [0.30, 0.36, 0.24, 0.32, 0.28, 0.34, 0.26]  # mean ~30%, σ ≈ 4.4pp
        current = [0.23, 0.24, 0.25]  # ~7pp below trailing median
        rows = synth_history(current + trailing)
        should_alert, reason = check_perf_gap.variance_gate(rows)
        assert should_alert is False, f"expected silence, got alert: {reason}"

    def test_zero_stddev_trailing_still_alerts_on_drop(self):
        # Defensive: if all trailing nights are identical, σ=0. Any drop must still alert.
        trailing = [0.37] * 7
        current = [0.30, 0.30, 0.30]
        rows = synth_history(current + trailing)
        should_alert, reason = check_perf_gap.variance_gate(rows)
        assert should_alert is True


# ---------------------------------------------------------------------------
# handle_condition_0 — must NOT call gh when gate silences
# ---------------------------------------------------------------------------

class TestHandleCondition0:
    def test_does_not_create_or_comment_when_gate_silences(self):
        # Trailing + current clustered tightly around 37% — gate silences.
        ratios = [0.37] * 10
        rows = synth_history(ratios)
        per_wl = {wl: 0.37 for wl in WORKLOADS}

        with mock.patch.object(check_perf_gap, "gh") as gh_mock, \
             mock.patch.object(check_perf_gap, "create_issue") as create_mock, \
             mock.patch.object(check_perf_gap, "comment_on_issue") as comment_mock:
            check_perf_gap.handle_condition_0(
                median_ratio=0.37,
                north_star=0.90,
                per_wl=per_wl,
                rows=rows,
                dry_run=False,
            )
            assert create_mock.call_count == 0
            assert comment_mock.call_count == 0
            # find_open_issue uses gh too — but we should never have gotten there.
            assert gh_mock.call_count == 0

    def test_creates_issue_when_gate_passes_and_no_existing(self):
        # Trailing tight, current drops far → gate fires.
        trailing = [0.40, 0.41, 0.39, 0.40, 0.40, 0.41, 0.39]
        current = [0.20, 0.21, 0.20]
        rows = synth_history(current + trailing)
        per_wl = {wl: 0.20 for wl in WORKLOADS}

        with mock.patch.object(check_perf_gap, "find_open_issue", return_value=None), \
             mock.patch.object(check_perf_gap, "create_issue") as create_mock, \
             mock.patch.object(check_perf_gap, "comment_on_issue") as comment_mock:
            check_perf_gap.handle_condition_0(
                median_ratio=0.20,
                north_star=0.90,
                per_wl=per_wl,
                rows=rows,
                dry_run=False,
            )
            assert create_mock.call_count == 1
            assert comment_mock.call_count == 0

    def test_creates_issue_when_history_too_short_to_gate(self):
        # Only 2 nights → gate bypasses → alert proceeds. This guards against
        # the gate eating real signal on a fresh install or after a long outage.
        rows = synth_history([0.20, 0.21])
        per_wl = {wl: 0.20 for wl in WORKLOADS}

        with mock.patch.object(check_perf_gap, "find_open_issue", return_value=None), \
             mock.patch.object(check_perf_gap, "create_issue") as create_mock:
            check_perf_gap.handle_condition_0(
                median_ratio=0.20,
                north_star=0.90,
                per_wl=per_wl,
                rows=rows,
                dry_run=False,
            )
            assert create_mock.call_count == 1


# ---------------------------------------------------------------------------
# print_ratios — aggregation produces error bars
# ---------------------------------------------------------------------------

class TestPrintRatios:
    def test_build_lookup_aggregates_samples(self):
        rows = synth_night("2026-05-29", salvobase_ops=500, mongo_ops=1000,
                           samples_per_combo=3, jitter=50.0)
        lookup = print_ratios.build_lookup(rows)
        sb = lookup[("A", 1, "salvobase")]
        assert sb["n"] == 3
        assert sb["ops_med"] == pytest.approx(500, rel=1e-6)
        assert sb["ops_std"] > 0  # jitter present

    def test_build_lookup_zero_stddev_with_one_sample(self):
        rows = synth_night("2026-05-29", 500, 1000, samples_per_combo=1)
        lookup = print_ratios.build_lookup(rows)
        sb = lookup[("A", 1, "salvobase")]
        assert sb["n"] == 1
        assert sb["ops_std"] == 0.0


# ---------------------------------------------------------------------------
# Workflow contract — 3 samples per combination
# ---------------------------------------------------------------------------

class TestWorkflowSampling:
    """
    Static check on .github/workflows/benchmark.yml — guards against silently
    losing the 3× sampling loop in a future edit. The workflow body is read as
    text rather than parsed as YAML; that's sufficient for shape assertions.
    """

    def test_workflow_has_three_sample_loop(self):
        wf = (REPO_ROOT / ".github/workflows/benchmark.yml").read_text()
        assert "for SAMPLE in 1 2 3" in wf, (
            "benchmark.yml must wrap the per-combination run loop with "
            "`for SAMPLE in 1 2 3` (see #715)"
        )

    def test_workflow_validation_threshold_is_six(self):
        wf = (REPO_ROOT / ".github/workflows/benchmark.yml").read_text()
        # Sanity-check: validation must expect ≥6 rows per workload now that we sample 3×.
        assert "expected ≥6" in wf, (
            "benchmark.yml validation threshold must be ≥6 rows per workload "
            "after 3× sampling change"
        )
