"""
Tests for the per-workload achievement gate in scripts/bench/check_perf_gap.py.

Regression coverage for #737: the achievement gate previously used the overall
median, which silently passed when some workloads beat MongoDB while others
were far behind. The gate now uses the worst per-workload median.

Run from repo root:
    python3 -m pytest tests/bench/test_achievement_gate.py -v
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import unittest.mock as mock


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


# ---------------------------------------------------------------------------
# determine_routing — the new gate
# ---------------------------------------------------------------------------

class TestDetermineRouting:
    def test_true_achievement_every_workload_passes(self):
        # Every workload at or above north star → Condition 2.
        per_wl = {"A": 0.91, "B": 1.02, "C": 1.10, "D": 1.05, "F": 0.95}
        cond, gap, worst_ratio, worst_wl = check_perf_gap.determine_routing(
            median_ratio=1.02, per_wl=per_wl, north_star=0.90,
        )
        assert cond == 2
        assert worst_wl == "A"
        assert worst_ratio == 0.91
        assert gap <= 0

    def test_false_achievement_blocked_when_median_passes_but_min_fails(self):
        # The exact bug being fixed: overall median (94.5%) ≥ north_star (90%)
        # but workload A at 62.3% is far behind. Old code declared achievement;
        # new code must route to Condition 0 (worst-wl gap > 10pp).
        per_wl = {"A": 0.623, "B": 1.027, "C": 1.105, "D": 1.050, "F": 0.771}
        cond, gap, worst_ratio, worst_wl = check_perf_gap.determine_routing(
            median_ratio=0.945, per_wl=per_wl, north_star=0.90,
        )
        assert cond == 0, "false achievement leaked through"
        assert worst_wl == "A"
        assert worst_ratio == 0.623
        assert abs(gap - (0.90 - 0.623)) < 1e-9

    def test_all_workloads_more_than_10pp_behind_routes_to_condition_0(self):
        per_wl = {"A": 0.40, "B": 0.50, "C": 0.55, "D": 0.60, "F": 0.45}
        cond, gap, _, worst_wl = check_perf_gap.determine_routing(
            median_ratio=0.50, per_wl=per_wl, north_star=0.90,
        )
        assert cond == 0
        assert worst_wl == "A"
        assert gap > 0.10

    def test_worst_workload_within_10pp_routes_to_condition_1(self):
        # Worst workload at 82% → gap = 8pp → Condition 1.
        per_wl = {"A": 0.82, "B": 0.95, "C": 0.99, "D": 0.92, "F": 0.88}
        cond, gap, _, worst_wl = check_perf_gap.determine_routing(
            median_ratio=0.92, per_wl=per_wl, north_star=0.90,
        )
        assert cond == 1
        assert worst_wl == "A"
        assert 0 < gap <= 0.10

    def test_boundary_at_exactly_10pp(self):
        # Worst workload exactly 10pp behind → gap == 0.10 → NOT Condition 0.
        # (gap > 0.10 is the strict-greater threshold.)
        per_wl = {"A": 0.80, "B": 0.95, "C": 1.00, "D": 0.95, "F": 0.95}
        cond, gap, _, _ = check_perf_gap.determine_routing(
            median_ratio=0.95, per_wl=per_wl, north_star=0.90,
        )
        assert cond == 1
        assert abs(gap - 0.10) < 1e-9

    def test_boundary_at_exactly_north_star(self):
        # Worst workload exactly at north star → gap == 0 → Condition 2.
        per_wl = {"A": 0.90, "B": 0.95, "C": 1.00, "D": 0.95, "F": 0.95}
        cond, gap, worst_ratio, worst_wl = check_perf_gap.determine_routing(
            median_ratio=0.95, per_wl=per_wl, north_star=0.90,
        )
        assert cond == 2
        assert worst_wl == "A"
        assert worst_ratio == 0.90
        assert gap == 0

    def test_empty_per_wl_falls_back_to_overall_median(self):
        # Defensive: if per_wl is empty (no paired data), use median_ratio.
        cond, gap, worst_ratio, worst_wl = check_perf_gap.determine_routing(
            median_ratio=0.50, per_wl={}, north_star=0.90,
        )
        assert cond == 0
        assert worst_wl is None
        assert worst_ratio == 0.50
        assert abs(gap - 0.40) < 1e-9

    def test_empty_per_wl_and_none_median(self):
        # Maximally degenerate: per_wl empty AND median None. Should not crash;
        # worst_ratio defaults to 0.0 → gap = north_star → Condition 0.
        cond, gap, worst_ratio, worst_wl = check_perf_gap.determine_routing(
            median_ratio=None, per_wl={}, north_star=0.90,
        )
        assert cond == 0
        assert worst_wl is None
        assert worst_ratio == 0.0
        assert gap == 0.90


# ---------------------------------------------------------------------------
# Body rendering — must reflect the worst-workload bar
# ---------------------------------------------------------------------------

class TestBodyRendering:
    def test_p0_body_title_names_the_worst_workload(self):
        per_wl = {"A": 0.62, "B": 1.03, "C": 1.10, "D": 1.05, "F": 0.77}
        title, body = check_perf_gap.p0_body(
            median_ratio=0.945, north_star=0.90, per_wl=per_wl,
        )
        assert "worst workload A" in title
        assert "62.0%" in title
        # The misleading old behavior (showing 94.5% as "currently") must not return.
        assert "currently 94.5%" not in title

    def test_p0_body_displays_overall_for_context(self):
        per_wl = {"A": 0.62, "B": 1.03, "C": 1.10, "D": 1.05, "F": 0.77}
        _, body = check_perf_gap.p0_body(
            median_ratio=0.945, north_star=0.90, per_wl=per_wl,
        )
        assert "94.5%" in body, "overall median should still be visible for context"
        assert "62.0%" in body

    def test_c1_body_title_names_the_worst_workload(self):
        per_wl = {"A": 0.82, "B": 0.95, "C": 0.99, "D": 0.92, "F": 0.88}
        title, _ = check_perf_gap.c1_body(
            median_ratio=0.92, north_star=0.90, per_wl=per_wl,
        )
        assert "worst workload A" in title
        assert "82.0%" in title

    def test_achieved_body_mentions_worst_workload_when_per_wl_given(self):
        per_wl = {"A": 0.91, "B": 1.02, "C": 1.10, "D": 1.05, "F": 0.95}
        _, body = check_perf_gap.achieved_body(
            median_ratio=1.02, north_star=0.90, per_wl=per_wl,
        )
        assert "Worst workload: **A**" in body
        assert "91.0%" in body

    def test_achieved_body_omits_worst_line_when_per_wl_missing(self):
        _, body = check_perf_gap.achieved_body(median_ratio=0.95, north_star=0.90)
        assert "Worst workload" not in body

    def test_p0_body_does_not_leak_NA_when_per_wl_empty(self):
        # Defensive: empty per_wl must not produce a title containing "N/A".
        title, body = check_perf_gap.p0_body(
            median_ratio=0.50, north_star=0.90, per_wl={},
        )
        assert "N/A" not in title
        assert "N/A" not in body
        # Should fall back to overall-median wording.
        assert "overall median" in title.lower() or "overall median" in body.lower()

    def test_c1_body_does_not_leak_NA_when_per_wl_empty(self):
        title, body = check_perf_gap.c1_body(
            median_ratio=0.85, north_star=0.90, per_wl={},
        )
        assert "N/A" not in title
        assert "N/A" not in body

    def test_p0_body_uses_explicit_worst_when_provided(self):
        # Threading worst_wl/worst_ratio from determine_routing must take
        # precedence over recomputing from per_wl — single source of truth.
        per_wl = {"A": 0.62, "B": 1.03}
        title, body = check_perf_gap.p0_body(
            median_ratio=0.83, north_star=0.90, per_wl=per_wl,
            worst_wl="A", worst_ratio=0.62,
        )
        assert "worst workload A" in title
        assert "62.0%" in title


# ---------------------------------------------------------------------------
# Routing integration — main() picks the right handler
# ---------------------------------------------------------------------------

class TestMainRouting:
    def _run_main(self, median_ratio: float, per_wl: dict, north_star: float = 0.90):
        """Run main() with patched I/O. Returns dict of which handlers were called."""
        called = {"c0": False, "c1": False, "c2": False}

        def _record_c0(*args, **kwargs):
            called["c0"] = True

        def _record_c1(*args, **kwargs):
            called["c1"] = True

        def _record_c2(*args, **kwargs):
            called["c2"] = True

        with mock.patch.object(check_perf_gap, "load_north_star", return_value=north_star), \
             mock.patch.object(check_perf_gap, "load_results", return_value=[{"placeholder": True}]), \
             mock.patch.object(check_perf_gap, "compute_median_ratio",
                               return_value=(median_ratio, per_wl)), \
             mock.patch.object(check_perf_gap, "handle_condition_0", side_effect=_record_c0), \
             mock.patch.object(check_perf_gap, "handle_condition_1", side_effect=_record_c1), \
             mock.patch.object(check_perf_gap, "handle_condition_2", side_effect=_record_c2), \
             mock.patch.object(sys, "argv", ["check_perf_gap.py", "--dry-run"]):
            check_perf_gap.main()
        return called

    def test_main_routes_to_c0_when_worst_workload_far_behind(self):
        # The bug-fix scenario: overall median passes, A is at 62%.
        per_wl = {"A": 0.623, "B": 1.027, "C": 1.105, "D": 1.050, "F": 0.771}
        called = self._run_main(median_ratio=0.945, per_wl=per_wl)
        assert called == {"c0": True, "c1": False, "c2": False}

    def test_main_routes_to_c1_when_worst_workload_close_to_bar(self):
        per_wl = {"A": 0.82, "B": 0.95, "C": 0.99, "D": 0.92, "F": 0.88}
        called = self._run_main(median_ratio=0.92, per_wl=per_wl)
        assert called == {"c0": False, "c1": True, "c2": False}

    def test_main_routes_to_c2_only_when_every_workload_passes(self):
        per_wl = {"A": 0.91, "B": 1.02, "C": 1.10, "D": 1.05, "F": 0.95}
        called = self._run_main(median_ratio=1.02, per_wl=per_wl)
        assert called == {"c0": False, "c1": False, "c2": True}

    def test_main_does_not_route_to_c2_when_overall_passes_but_min_fails(self):
        # Lock in the regression: overall median ≥ north_star MUST NOT alone fire Condition 2.
        per_wl = {"A": 0.50, "B": 1.20, "C": 1.30, "D": 1.10, "F": 0.95}
        # Overall median across these is ~1.10 — comfortably above 0.90 — but A is at 50%.
        called = self._run_main(median_ratio=1.10, per_wl=per_wl)
        assert called["c2"] is False, "Condition 2 leaked despite worst workload below bar"
        assert called["c0"] is True


# ---------------------------------------------------------------------------
# Variance gate still functions (regression guard for #715 behavior)
# ---------------------------------------------------------------------------

class TestVarianceGateUnchanged:
    """
    The achievement gate fix (#737) operates on per-workload data, but the
    variance gate from #715 still suppresses noise-floor jitter on the overall
    series. Ensure the gate function still exists with the same signature and
    returns the same shape — full behavior is covered in test_variance_gate.py.
    """

    def test_variance_gate_still_returns_bool_and_reason(self):
        # 3 nights, all identical → trailing window too short → gate bypasses.
        rows = []
        for date in ("2026-05-29", "2026-05-28", "2026-05-27"):
            for wl in ("A", "B", "C", "D", "F"):
                for t in (1, 4, 8, 16):
                    rows.append({"workload": wl, "threads": t, "target": "salvobase",
                                 "ops_per_sec": 500.0, "p50_ms": 1, "p99_ms": 1, "date": date})
                    rows.append({"workload": wl, "threads": t, "target": "mongodb",
                                 "ops_per_sec": 1000.0, "p50_ms": 1, "p99_ms": 1, "date": date})
        should_alert, reason = check_perf_gap.variance_gate(rows)
        assert isinstance(should_alert, bool)
        assert isinstance(reason, str) and len(reason) > 0
