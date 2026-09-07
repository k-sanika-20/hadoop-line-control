#!/usr/bin/env python3
"""
Seeded A/B evaluation harness.

Runs two policies over the same simulated horizon with the *same* random seeds,
so demand pattern, service-time draws and fault arrivals are identical and the
only difference between the arms is the policy. Reports mean and 95% confidence
interval on the paired difference.

Two properties this enforces, both of which are easy to get wrong and fatal if
you do:

  --split N   mine on days 1..N, evaluate on days N+1..end. Without it you are
              tuning and testing on the same data, which is the same error as
              reporting training accuracy.

  --seeds K   K independent runs, paired. A single seed proves nothing: this line
              swings several percent on noise alone.

Usage:
  python sim/run_ab.py --a policy_A.json --b artifacts/policy_B.json --seeds 20 --days 30 --split 20
"""

import argparse
import json
import statistics
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import line_sim as LS


METRICS = [
    ("throughput_good_parts", "throughput (good parts)", "higher"),
    ("total_unplanned_downtime_h", "unplanned downtime (h)", "lower"),
    ("line_oee", "line OEE", "higher"),
]


def evaluate(policy, seed, days, split):
    """Simulate the full horizon, then score only the held-out tail."""
    stations, events, horizon_s = LS.simulate_line(policy, days, seed)
    if split:
        cut_ms = split * LS.SHIFT_SECONDS * 1000
        events = [e for e in events if e[0] >= cut_ms]
        eval_days = days - split
        horizon_s = eval_days * LS.SHIFT_SECONDS
    else:
        eval_days = days
    return LS.summarise(policy, stations, events, horizon_s, eval_days)


def ci95(xs):
    m = statistics.mean(xs)
    if len(xs) < 2:
        return m, 0.0
    return m, 1.96 * statistics.stdev(xs) / len(xs) ** 0.5


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--a", default="policy_A.json", help="baseline policy")
    ap.add_argument("--b", required=True, help="policy to test (normally Hadoop-mined)")
    ap.add_argument("--seeds", type=int, default=20)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--split", type=int, default=20,
                    help="mine on days 1..SPLIT, score on the rest; 0 disables the split")
    ap.add_argument("--out", default="artifacts/results.json")
    args = ap.parse_args()

    A = LS.load_policy(args.a)
    B = LS.load_policy(args.b)

    print(f"A = {A['policy_id']:<16} {args.a}")
    print(f"B = {B['policy_id']:<16} {args.b}")
    print(f"{args.seeds} paired seeds, {args.days} days, scoring days "
          f"{args.split + 1}..{args.days}" if args.split else f"{args.seeds} paired seeds, {args.days} days")
    print()

    runs = []
    for i, seed in enumerate(range(1, args.seeds + 1), 1):
        ra = evaluate(A, seed, args.days, args.split)
        rb = evaluate(B, seed, args.days, args.split)
        runs.append((ra, rb))
        print(f"\r  seed {i}/{args.seeds}", end="", flush=True)
    print("\r" + " " * 30 + "\r", end="")

    results = {
        "policy_a": A["policy_id"],
        "policy_b": B["policy_id"],
        "seeds": args.seeds,
        "days": args.days,
        "split_day": args.split,
        "metrics": {},
    }

    print(f"{'metric':<26}{'A':>12}{'B':>12}{'delta':>12}{'95% CI':>11}  verdict")
    print("-" * 86)

    for key, label, direction in METRICS:
        a = [ra[key] for ra, _ in runs]
        b = [rb[key] for _, rb in runs]
        d = [y - x for x, y in zip(a, b)]
        ma, _ = ci95(a)
        mb, _ = ci95(b)
        md, hd = ci95(d)

        significant = abs(md) > hd
        better = (md > 0) if direction == "higher" else (md < 0)
        if not significant:
            verdict = "no effect"
        elif better:
            verdict = "B better"
        else:
            verdict = "B WORSE"

        pct = (md / ma * 100) if ma else 0.0
        print(f"{label:<26}{ma:>12.3f}{mb:>12.3f}{md:>+12.3f}{hd:>11.3f}  "
              f"{verdict}  ({pct:+.2f}%)")

        results["metrics"][key] = {
            "label": label,
            "direction": direction,
            "a_mean": round(ma, 4),
            "b_mean": round(mb, 4),
            "delta_mean": round(md, 4),
            "delta_ci95": round(hd, 4),
            "delta_pct": round(pct, 3),
            "significant": bool(significant),
            "verdict": verdict,
        }

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(results, f, indent=2)
    print()
    print(f"-> {args.out}")
    print()
    print("Report every row, including the ones where B is worse. A result table")
    print("in which everything improved is the one examiners disbelieve.")


if __name__ == "__main__":
    main()
