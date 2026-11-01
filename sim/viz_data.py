"""
Loaders shared by the report figures (sim/plots.py) and the dashboard
(dashboard/app.py). Everything here reads files the pipeline already wrote -
job output under artifacts/mined, the policies, results.json - so neither
consumer needs the cluster to be running.
"""

import json
import os

import pandas as pd

import build_policy as BP
import line_sim as LS

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MINED = os.path.join(ROOT, "artifacts", "mined")

STATIONS = [s[0] for s in LS.LINE]
STATION_NAMES = {s[0]: s[1] for s in LS.LINE}

# Reference categorical palette, slots 1-6 in fixed order: colour follows the
# station, never its rank, in every chart.
STATION_COLORS = dict(zip(STATIONS, ["#2a78d6", "#eb6834", "#1baf7a",
                                     "#eda100", "#e87ba4", "#008300"]))
POLICY_COLORS = {"A · baseline": "#898781", "B · mined": "#2a78d6"}
GOOD, CRITICAL = "#0ca30c", "#d03b3b"


def path(*parts):
    return os.path.join(ROOT, *parts)


def load_json(*parts):
    with open(path(*parts)) as f:
        return json.load(f)


def station_table(mined=MINED):
    """One row per station: J1 OEE, J2 utilisation, J3 limits, J4 slope."""
    oee = BP.load_oee(mined)
    bott = BP.load_bottleneck(mined)
    lim = BP.load_limits(mined)
    slopes = BP.load_degradation_slopes(mined)
    rows = []
    for sid in STATIONS:
        o, b, l, s = oee.get(sid, {}), bott.get(sid, {}), lim.get(sid, {}), slopes.get(sid, {})
        rows.append({
            "station": sid, "name": STATION_NAMES[sid],
            "oee": o.get("oee"), "availability": o.get("availability"),
            "performance": o.get("performance"), "quality": o.get("quality"),
            "defect_rate": o.get("defect_rate"),
            "utilisation": b.get("utilisation"), "mean_cycle_s": b.get("mean_cycle_s"),
            "blocked_s": b.get("blocked_s"), "starved_s": b.get("starved_s"),
            "torque_mean": l.get("mean"), "torque_sd": l.get("sd"),
            "slope_per_h": s.get("slope_per_h"), "fresh_torque": s.get("intercept"),
        })
    return pd.DataFrame(rows)


def time_split(mined=MINED):
    """Busy / blocked / starved share of each station's clock, from J2."""
    rows = []
    for sid, d in BP.read_kv_rows(os.path.join(mined, "bottleneck", "part-*")):
        busy = float(d["cycles"]) * float(d["mean_cycle_s"])
        blocked, starved = float(d["blocked_s"]), float(d["starved_s"])
        total = busy + blocked + starved
        for state, v in (("busy", busy), ("blocked", blocked), ("starved", starved)):
            rows.append({"station": sid, "state": state, "share": v / total, "hours": v / 3600})
    return pd.DataFrame(rows).sort_values(["station", "state"])


def degradation_points(mined=MINED, fitted_only=False):
    """J4 rows: mean torque per (station, age bucket), plus rise above fresh.
    fitted_only keeps the buckets build_policy's slope fit uses (n >= 20% of
    the fullest bucket) - the thin tail is noise."""
    slopes = BP.load_degradation_slopes(mined)
    rows = []
    for sid, d in BP.read_kv_rows(os.path.join(mined, "degradation", "part-*")):
        fresh = slopes.get(sid, {}).get("intercept")
        mean = float(d["mean"])
        rows.append({"station": sid, "age_h": float(d["age_h"]), "n": int(d["n"]),
                     "mean_torque": mean, "sd": float(d["sd"]),
                     "rise_pct": (mean / fresh - 1) * 100 if fresh else None})
    df = pd.DataFrame(rows).sort_values(["station", "age_h"])
    if fitted_only:
        df = df[df.n >= 0.2 * df.groupby("station").n.transform("max")]
    return df


def policy_table(a, b):
    """Long table of every policy field, A vs B, per station."""
    rows = []
    for sid in STATIONS:
        for field in ("speed_setpoint", "torque_lcl", "torque_ucl",
                      "pm_interval_cycles", "buffer_capacity"):
            va, vb = a["stations"][sid][field], b["stations"][sid][field]
            rows.append({"station": sid, "field": field, "A": va, "B": vb,
                         "changed": va != vb})
    return pd.DataFrame(rows)


def ab_table(results):
    rows = []
    for key, m in results["metrics"].items():
        a = m["a_mean"]
        rows.append({"metric": m["label"], "key": key, "direction": m["direction"],
                     "A": a, "B": m["b_mean"], "delta": m["delta_mean"],
                     "ci95": m["delta_ci95"], "delta_pct": m["delta_pct"],
                     "ci_pct": m["delta_ci95"] / a * 100 if a else 0.0,
                     "verdict": m["verdict"]})
    return pd.DataFrame(rows)
