#!/usr/bin/env python3
"""
Turn the four MapReduce outputs into the next policy file.

This step is deliberately NOT a MapReduce job. It consumes about two dozen rows
- the aggregated output of J1-J4 - and forcing kilobytes through YARN to produce
kilobytes would be cargo-cult Hadoop. The defensible line in the viva is that
the batch tier is where the *aggregation* happens, and policy synthesis is a
control decision taken on the aggregate. Say that rather than pretending.

What each job contributes:

  J3 limits       -> torque_lcl / torque_ucl        (k-sigma control limits)
  J4 degradation  -> pm_interval_cycles             (from the mean-vs-age slope)
  J2 bottleneck   -> buffer_capacity                (decouple the constraint)
  J1 oee          -> reported for context; drives nothing on its own

Usage:
  python sim/build_policy.py --mined artifacts/mined --base policy_A.json \
                             --out artifacts/policy_B.json
"""

import argparse
import glob
import json
import os
import re

# A tool is maintained once its torque has risen this fraction above fresh.
# Lower = more frequent maintenance. This is the one tuning knob of the policy
# synthesiser, and it should be stated in the report rather than buried.
TARGET_TORQUE_RISE = 0.15

PM_MIN_CYCLES = 400
PM_MAX_CYCLES = 20000
BUFFER_MIN = 4
BUFFER_MAX = 24


def read_kv_rows(path_glob):
    """Job output is 'station \\t k=v \\t k=v ...'. Returns list of (key, dict)."""
    rows = []
    for path in sorted(glob.glob(path_glob)):
        if os.path.basename(path).startswith("_"):
            continue
        with open(path) as f:
            for line in f:
                line = line.rstrip("\n")
                if not line.strip():
                    continue
                parts = line.split("\t")
                key = parts[0].strip()
                d = {}
                for p in parts[1:]:
                    if "=" in p:
                        k, v = p.split("=", 1)
                        d[k.strip()] = v.strip()
                    else:
                        d.setdefault("_extra", []).append(p.strip())
                rows.append((key, d))
    return rows


def load_limits(mined):
    out = {}
    for sid, d in read_kv_rows(os.path.join(mined, "limits", "part-*")):
        out[sid] = {"mean": float(d["mean"]), "sd": float(d["sd"]),
                    "lcl": float(d["lcl"]), "ucl": float(d["ucl"]), "n": int(d["n"])}
    return out


def load_bottleneck(mined):
    out = {}
    for sid, d in read_kv_rows(os.path.join(mined, "bottleneck", "part-*")):
        out[sid] = {"utilisation": float(d["utilisation"]),
                    "mean_cycle_s": float(d["mean_cycle_s"]),
                    "blocked_s": float(d["blocked_s"]),
                    "starved_s": float(d["starved_s"])}
    return out


def load_oee(mined):
    out = {}
    for sid, d in read_kv_rows(os.path.join(mined, "oee", "part-*")):
        out[sid] = {"oee": float(d["oee"]), "availability": float(d["availability"]),
                    "performance": float(d["performance"]), "quality": float(d["quality"]),
                    "defect_rate": float(d["defect_rate"])}
    return out


def load_degradation_slopes(mined):
    """
    J4 emits one row per (station, age bucket). Fit mean torque against age in
    hours by ordinary least squares to get a degradation rate in torque-per-hour.
    Buckets with few samples are dropped - the tail of an age distribution is
    thin and would otherwise dominate the fit.
    """
    per = {}
    for sid, d in read_kv_rows(os.path.join(mined, "degradation", "part-*")):
        try:
            age = float(d["age_h"]); mean = float(d["mean"]); n = int(d["n"])
        except (KeyError, ValueError):
            continue
        per.setdefault(sid, []).append((age, mean, n))

    slopes = {}
    for sid, pts in per.items():
        if not pts:
            continue
        nmax = max(p[2] for p in pts)
        pts = [p for p in pts if p[2] >= 0.2 * nmax]
        if len(pts) < 3:
            continue
        xs = [p[0] for p in pts]; ys = [p[1] for p in pts]
        mx = sum(xs) / len(xs); my = sum(ys) / len(ys)
        den = sum((x - mx) ** 2 for x in xs)
        if den <= 0:
            continue
        slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den
        slopes[sid] = {"slope_per_h": slope, "intercept": my - slope * mx,
                       "buckets": len(pts)}
    return slopes


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mined", default="artifacts/mined",
                    help="directory holding oee/ bottleneck/ limits/ degradation/ job output")
    ap.add_argument("--base", default="policy_A.json")
    ap.add_argument("--out", default="artifacts/policy_B.json")
    ap.add_argument("--sigma", type=float, default=3.0)
    args = ap.parse_args()

    base = json.load(open(args.base))
    limits = load_limits(args.mined)
    bott = load_bottleneck(args.mined)
    oee = load_oee(args.mined)
    slopes = load_degradation_slopes(args.mined)

    if not limits:
        raise SystemExit(f"no limits output under {args.mined}/limits - did J3 run?")
    if not bott:
        raise SystemExit(f"no bottleneck output under {args.mined}/bottleneck - did J2 run?")

    policy = json.loads(json.dumps(base))
    policy["policy_id"] = "B-mined"
    policy["provenance"] = (
        "derived by MapReduce from run=baseline: control limits from J3, "
        f"PM intervals from J4 degradation slopes (target rise {TARGET_TORQUE_RISE:.0%}), "
        "buffers from the J2 constraint"
    )

    # --- the constraint, and the stations feeding it -----------------------
    bottleneck = max(bott.items(), key=lambda kv: kv[1]["utilisation"])[0]
    order = sorted(policy["stations"].keys())
    bidx = order.index(bottleneck) if bottleneck in order else None

    changes = []

    for sid in order:
        st = policy["stations"][sid]
        before = dict(st)

        # J3 -> control limits
        if sid in limits:
            st["torque_lcl"] = round(limits[sid]["mean"] - args.sigma * limits[sid]["sd"], 3)
            st["torque_ucl"] = round(limits[sid]["mean"] + args.sigma * limits[sid]["sd"], 3)

        # J4 -> preventive-maintenance interval
        if sid in slopes and sid in bott and sid in limits:
            slope = slopes[sid]["slope_per_h"]
            fresh = slopes[sid]["intercept"]
            cycle_s = bott[sid]["mean_cycle_s"]
            util = bott[sid]["utilisation"]
            # The degradation curve's x-axis is WALL-CLOCK age, but a maintenance
            # interval is counted in cycles - and a station spends part of the
            # clock blocked and starved, not cutting. Dividing by processing time
            # alone overstates the interval by 1/utilisation (1.05-1.5x here),
            # which compounded with the slope error to give intervals ~2x too long.
            wall_per_cycle = cycle_s / util if util > 0 else cycle_s
            if slope > 1e-9 and fresh > 0 and wall_per_cycle > 0:
                allowed_rise = TARGET_TORQUE_RISE * fresh
                pm_hours = allowed_rise / slope
                pm_cycles = int(round(pm_hours * 3600.0 / wall_per_cycle))
                st["pm_interval_cycles"] = max(PM_MIN_CYCLES, min(PM_MAX_CYCLES, pm_cycles))

        # J2 -> protect the constraint with buffer
        if bidx is not None:
            i = order.index(sid)
            if i == bidx or i == bidx - 1:
                st["buffer_capacity"] = max(BUFFER_MIN,
                                            min(BUFFER_MAX, int(round(before["buffer_capacity"] * 1.5))))

        changed = {k: (before[k], st[k]) for k in st if before.get(k) != st[k]}
        if changed:
            changes.append((sid, changed))

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(policy, f, indent=2)

    # --- report -----------------------------------------------------------
    print(f"constraint (highest utilisation): {bottleneck}")
    steep = max(slopes.items(), key=lambda kv: kv[1]["slope_per_h"])[0] if slopes else "n/a"
    print(f"fastest degradation (steepest slope): {steep}")
    print()
    print(f"{'station':<9}{'OEE':>7}{'util':>7}{'torque mu':>11}{'sigma':>8}"
          f"{'slope/h':>10}{'PM A':>8}{'PM B':>8}")
    for sid in order:
        o = oee.get(sid, {}); b = bott.get(sid, {}); l = limits.get(sid, {})
        s = slopes.get(sid, {})
        print(f"{sid:<9}{o.get('oee', 0):>7.3f}{b.get('utilisation', 0):>7.3f}"
              f"{l.get('mean', 0):>11.2f}{l.get('sd', 0):>8.3f}"
              f"{s.get('slope_per_h', 0):>10.4f}"
              f"{base['stations'][sid]['pm_interval_cycles']:>8}"
              f"{policy['stations'][sid]['pm_interval_cycles']:>8}")
    print()
    print(f"-> {args.out}")
    print()
    print("Now score it:")
    print(f"  python sim/run_ab.py --a {args.base} --b {args.out} --seeds 20 --days 30 --split 20")


if __name__ == "__main__":
    main()
