#!/usr/bin/env python3
"""
Discrete-event simulator for a six-station robotic assembly line.

The line is a tandem queue with finite buffers, so stations block when the
downstream buffer is full and starve when the upstream one is empty. Tools wear
as they cut, which lengthens cycle times and raises defect probability; wear is
reset by maintenance, which is either preventive (short, planned) or corrective
(long, unplanned, triggered by a wear-dependent hazard).

Everything the line obeys comes from a policy file: per-station speed setpoint,
SPC control limits, preventive-maintenance interval and buffer capacity. Nothing
is hardcoded. That is what makes the control loop closable - Hadoop mines the
telemetry this produces, writes a new policy, and the line is re-run under it.

Outputs, all plain CSV because gzip is not splittable and would defeat
block-level parallelism in HDFS:

  telemetry/dt=<date>/station=<S>/part-0.csv   ts_ms,station,signal,value
  part_events/dt=<date>/part-0.csv             ts_end_ms,station,part_id,cycle_ms,
                                               blocked_ms,starved_ms,down_ms,status,defect_code
  maintenance/maintenance.csv                  ts_ms,station,event,duration_ms
  run_summary.json                             ground truth, for cross-checking MapReduce output

Usage:
  python sim/line_sim.py --policy policy_A.json --run baseline --days 7
  python sim/line_sim.py --emit-default-policy policy_A.json
"""

import argparse
import json
import math
import os
import shutil
from datetime import datetime, timedelta, timezone

import numpy as np

# --------------------------------------------------------------------------
# Line specification. These are physical properties of the plant, NOT policy:
# the controller cannot change them, and Hadoop must discover them from data.
#
# Ground truth deliberately planted here, so the MapReduce jobs have a known
# answer to be validated against:
#   S03 is the true bottleneck  - longest nominal cycle time
#   S04 degrades fastest        - highest wear rate, so it needs the shortest PM interval
# --------------------------------------------------------------------------

LINE = [
    # id     name          nominal_s  torque  sigma  wear_rate  defect_base
    ("S01", "frame-load",     11.0,    28.0,  1.10,   0.55e-4,   0.0035),
    ("S02", "weld",           12.5,    44.0,  2.30,   1.10e-4,   0.0060),
    ("S03", "fasten",         14.8,    39.0,  1.70,   0.90e-4,   0.0050),   # bottleneck
    ("S04", "adhesive",       12.0,    33.0,  1.45,   2.30e-4,   0.0055),   # fastest wear
    ("S05", "vision-inspect", 10.5,    18.0,  0.80,   0.30e-4,   0.0015),
    ("S06", "unload",         11.2,    24.0,  1.00,   0.50e-4,   0.0025),
]

SIGNALS = ("torque", "motor_temp", "vibration", "spindle_load")

SHIFT_SECONDS = 8 * 3600          # one 8-hour shift per simulated day
PM_DURATION_S = 240.0             # planned maintenance stop
CORRECTIVE_MEAN_S = 1500.0        # unplanned failure: far more expensive, which is the point
WEAR_CYCLE_PENALTY = 0.22         # cycle time inflation at wear_frac = 1
WEAR_DEFECT_SLOPE = 0.075         # defect probability added at wear_frac = 1
SPEED_DEFECT_SLOPE = 0.045        # defect probability added per unit of over-speed
WEAR_TORQUE_GAIN = 0.30           # torque rise at wear_frac = 1
DERATE_FACTOR = 0.85              # speed multiplier while a station is derated
DERATE_CYCLES = 40                # how long a control-limit breach holds the station down
WEAR_SCALE = 1.5                  # scales per-station wear_rate into wear_frac per cycle


# --------------------------------------------------------------------------
# Policy
# --------------------------------------------------------------------------

def default_policy():
    """Policy A: the static, hand-set baseline. Uniform, plausible, and wrong -
    it treats every station identically when the plant is not identical."""
    stations = {}
    for sid, _name, nominal, torque, sigma, _wear, _defect in LINE:
        stations[sid] = {
            "speed_setpoint": 1.00,
            "torque_lcl": round(torque - 4 * sigma, 2),
            "torque_ucl": round(torque + 4 * sigma, 2),
            "pm_interval_cycles": 3000,
            "buffer_capacity": 8,
            "ideal_cycle_s": nominal,
        }
    return {
        "policy_id": "A-baseline",
        "provenance": "hand-set uniform baseline",
        "stations": stations,
    }


def load_policy(path):
    with open(path) as f:
        p = json.load(f)
    for sid, _n, _c, _t, _s, _w, _d in LINE:
        if sid not in p["stations"]:
            raise ValueError(f"policy {path} is missing station {sid}")
    return p


# --------------------------------------------------------------------------
# Discrete-event pass: cycle-by-cycle over the tandem line
# --------------------------------------------------------------------------

class StationState:
    def __init__(self, spec, pol, rng):
        self.id, self.name, self.nominal, self.torque0, self.sigma, self.wear_rate, self.defect_base = spec
        self.speed = float(pol["speed_setpoint"])
        self.lcl = float(pol["torque_lcl"])
        self.ucl = float(pol["torque_ucl"])
        self.pm_interval = int(pol["pm_interval_cycles"])
        self.buffer = int(pol["buffer_capacity"])
        self.ideal_cycle_s = float(pol["ideal_cycle_s"])
        self.rng = rng

        self.cycles_since_pm = 0
        self.derate_left = 0
        self.wear_marks = []      # (t_end_s, wear_frac) sampled once per cycle
        self.maintenance = []     # (t_s, event, duration_s)
        self.down_intervals = []  # (t_start_s, t_end_s)

    def wear_frac(self):
        # Per-station wear rate is a physical property of the tool, not policy.
        # S04 wears ~4x faster than S01, which is what the PM interval must discover.
        return min(1.0, self.cycles_since_pm * self.wear_rate * WEAR_SCALE)

    def effective_speed(self):
        return self.speed * (DERATE_FACTOR if self.derate_left > 0 else 1.0)

    def torque_proxy(self):
        """What the station's torque signal is centred on this cycle. The edge
        controller compares this against the policy's UCL."""
        return self.torque0 * self.effective_speed() * (1.0 + WEAR_TORQUE_GAIN * self.wear_frac())

    def service_seconds(self):
        w = self.wear_frac()
        base = self.nominal / self.effective_speed()
        base *= 1.0 + WEAR_CYCLE_PENALTY * w
        return max(0.5, base * self.rng.lognormal(mean=0.0, sigma=0.06))

    def defect_probability(self):
        p = self.defect_base
        p += WEAR_DEFECT_SLOPE * self.wear_frac()
        p += SPEED_DEFECT_SLOPE * max(0.0, self.effective_speed() - 1.0)
        if self.derate_left > 0:
            p *= 0.55          # the control action works: slowing down cuts defects
        return min(0.9, p)

    def failure_hazard(self):
        """Probability this cycle ends in an unplanned failure. Rises steeply
        with wear, which is why the PM interval is worth tuning."""
        w = self.wear_frac()
        return 2.0e-5 + 9.0e-4 * (w ** 3)


def simulate_line(policy, days, seed):
    rng = np.random.default_rng(seed)
    stations = [StationState(spec, policy["stations"][spec[0]], rng) for spec in LINE]
    n_st = len(stations)
    horizon_s = days * SHIFT_SECONDS

    # d[i][n] = departure time of part n from station i
    depart = [[] for _ in range(n_st)]
    events = []          # per-part-per-station rows
    part_id = 0

    def d(i, n):
        if n < 0:
            return 0.0
        return depart[i][n]

    while True:
        n = part_id
        feed_time = n * 9.0        # raw-part feed rate; faster than any station, so S01 is rarely starved
        blocked_upstream = False

        for i, st in enumerate(stations):
            arrival = feed_time if i == 0 else d(i - 1, n)
            ready = d(i, n - 1) if n > 0 else 0.0

            # blocking: cannot finish part n until part (n - buffer) has left the next station
            if i < n_st - 1:
                room = d(i + 1, n - st.buffer - 1) if (n - st.buffer - 1) >= 0 else 0.0
            else:
                room = 0.0

            start = max(arrival, ready, room)
            starved = max(0.0, arrival - max(ready, room))
            blocked = max(0.0, room - max(ready, arrival))

            svc = st.service_seconds()
            end = start + svc

            # maintenance decided at the end of the cycle
            down = 0.0
            st.cycles_since_pm += 1

            if rng.random() < st.failure_hazard():
                dur = float(rng.exponential(CORRECTIVE_MEAN_S))
                st.maintenance.append((end, "corrective", dur))
                st.down_intervals.append((end, end + dur))
                down = dur
                st.cycles_since_pm = 0
                st.derate_left = 0
            elif st.cycles_since_pm >= st.pm_interval:
                dur = PM_DURATION_S
                st.maintenance.append((end, "preventive", dur))
                st.down_intervals.append((end, end + dur))
                down = dur
                st.cycles_since_pm = 0
                st.derate_left = 0

            # the edge controller: an SPC breach derates the station for a while
            if st.torque_proxy() > st.ucl:
                st.derate_left = DERATE_CYCLES
            elif st.derate_left > 0:
                st.derate_left -= 1

            failed = rng.random() < st.defect_probability()
            code = ""
            if failed:
                code = ["misalign", "cold-joint", "torque-out", "adhesion", "surface"][
                    int(rng.integers(0, 5))
                ]

            depart[i].append(end + down)
            st.wear_marks.append((end, st.wear_frac()))

            events.append(
                (
                    int(end * 1000),
                    st.id,
                    part_id,
                    int(svc * 1000),
                    int(blocked * 1000),
                    int(starved * 1000),
                    int(down * 1000),
                    "FAIL" if failed else "PASS",
                    code,
                )
            )

            if end > horizon_s:
                blocked_upstream = True

        part_id += 1
        if blocked_upstream or depart[-1][-1] > horizon_s:
            break

    return stations, events, horizon_s


# --------------------------------------------------------------------------
# Telemetry pass: vectorised, driven by the wear and downtime timelines above
# --------------------------------------------------------------------------

def write_telemetry(out_dir, stations, horizon_s, hz, days, start_date, seed):
    rng = np.random.default_rng(seed + 991)
    total_rows = 0

    for st in stations:
        marks = np.asarray(st.wear_marks, dtype=float)
        mark_t, mark_w = marks[:, 0], marks[:, 1]
        downs = np.asarray(st.down_intervals, dtype=float).reshape(-1, 2)

        for day in range(days):
            t0 = day * SHIFT_SECONDS
            t1 = t0 + SHIFT_SECONDS
            ts = np.arange(t0, t1, 1.0 / hz)
            if ts.size == 0:
                continue

            idx = np.clip(np.searchsorted(mark_t, ts), 0, mark_w.size - 1)
            wear = mark_w[idx]

            is_down = np.zeros(ts.size, dtype=bool)
            for a, b in downs:
                if b >= t0 and a <= t1:
                    is_down |= (ts >= a) & (ts < b)

            n = ts.size
            torque = (
                st.torque0 * st.speed * (1.0 + WEAR_TORQUE_GAIN * wear)
                + rng.normal(0.0, st.sigma, n)
            )
            torque[is_down] *= 0.05

            temp = (
                34.0
                + 26.0 * (st.torque0 / 44.0)
                + 14.0 * wear
                + 3.0 * np.sin(2 * np.pi * ts / 1800.0)
                + rng.normal(0.0, 0.9, n)
            )
            temp[is_down] -= 9.0

            vib = (
                0.42 * (1.0 + 1.8 * wear)
                + np.abs(rng.standard_t(4, n)) * 0.11
            )
            vib[is_down] *= 0.1

            load = np.clip(
                62.0 * st.speed * (1.0 + 0.35 * wear) + rng.normal(0.0, 3.5, n), 0.0, 100.0
            )
            load[is_down] = 0.0

            date = (start_date + timedelta(days=day)).strftime("%Y-%m-%d")
            d = os.path.join(out_dir, "telemetry", f"dt={date}", f"station={st.id}")
            os.makedirs(d, exist_ok=True)

            ts_ms = (ts * 1000).astype(np.int64)
            chunks = []
            for name, arr in zip(SIGNALS, (torque, temp, vib, load)):
                block = np.empty(n, dtype=object)
                sid = st.id
                vals = np.round(arr, 4)
                block = [f"{a},{sid},{name},{b}" for a, b in zip(ts_ms.tolist(), vals.tolist())]
                chunks.append("\n".join(block))
                total_rows += n

            with open(os.path.join(d, "part-0.csv"), "w", newline="\n") as f:
                f.write("ts_ms,station,signal,value\n")
                f.write("\n".join(chunks))
                f.write("\n")

    return total_rows


def write_part_events(out_dir, events, days, start_date):
    by_day = {}
    for row in events:
        day = min(days - 1, row[0] // 1000 // SHIFT_SECONDS)
        by_day.setdefault(day, []).append(row)

    for day, rows in by_day.items():
        date = (start_date + timedelta(days=day)).strftime("%Y-%m-%d")
        d = os.path.join(out_dir, "part_events", f"dt={date}")
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "part-0.csv"), "w", newline="\n") as f:
            f.write("ts_end_ms,station,part_id,cycle_ms,blocked_ms,starved_ms,down_ms,status,defect_code\n")
            for r in rows:
                f.write(",".join(str(x) for x in r) + "\n")
    return sum(len(v) for v in by_day.values())


def write_maintenance(out_dir, stations):
    d = os.path.join(out_dir, "maintenance")
    os.makedirs(d, exist_ok=True)
    n = 0
    with open(os.path.join(d, "maintenance.csv"), "w", newline="\n") as f:
        f.write("ts_ms,station,event,duration_ms\n")
        rows = []
        for st in stations:
            for t, ev, dur in st.maintenance:
                rows.append((int(t * 1000), st.id, ev, int(dur * 1000)))
        rows.sort()
        for r in rows:
            f.write(",".join(str(x) for x in r) + "\n")
            n += 1
    return n


# --------------------------------------------------------------------------
# Ground truth, so MapReduce output can be checked rather than trusted
# --------------------------------------------------------------------------

def summarise(policy, stations, events, horizon_s, days):
    per = {}
    for st in stations:
        per[st.id] = {
            "name": st.name,
            "parts": 0,
            "good": 0,
            "run_ms": 0,
            "down_ms": 0,
            "blocked_ms": 0,
            "starved_ms": 0,
            "ideal_cycle_s": st.ideal_cycle_s,
            "preventive": sum(1 for _t, e, _d in st.maintenance if e == "preventive"),
            "corrective": sum(1 for _t, e, _d in st.maintenance if e == "corrective"),
        }

    for ts_end, sid, _pid, cycle, blocked, starved, down, status, _code in events:
        s = per[sid]
        s["parts"] += 1
        s["good"] += 1 if status == "PASS" else 0
        s["run_ms"] += cycle
        s["down_ms"] += down
        s["blocked_ms"] += blocked
        s["starved_ms"] += starved

    planned_ms = horizon_s * 1000.0
    for sid, s in per.items():
        availability = max(0.0, (planned_ms - s["down_ms"])) / planned_ms
        performance = (
            (s["ideal_cycle_s"] * 1000.0 * s["parts"]) / s["run_ms"] if s["run_ms"] else 0.0
        )
        quality = s["good"] / s["parts"] if s["parts"] else 0.0
        s["availability"] = round(availability, 4)
        s["performance"] = round(min(1.0, performance), 4)
        s["quality"] = round(quality, 4)
        s["oee"] = round(availability * min(1.0, performance) * quality, 4)
        s["defect_rate"] = round(1.0 - quality, 5)

    for sid, s_ in per.items():
        idle = s_["blocked_ms"] + s_["starved_ms"]
        s_["idle_ms"] = idle
        s_["utilisation"] = round(s_["run_ms"] / (s_["run_ms"] + idle), 4) if s_["run_ms"] else 0.0
        s_["mean_cycle_s"] = round(s_["run_ms"] / 1000.0 / s_["parts"], 3) if s_["parts"] else 0.0

    throughput = per[LINE[-1][0]]["good"]
    # The bottleneck of a tandem line is the station that is idle least - it is
    # never waiting, everyone else waits on it.
    bottleneck = min(per.items(), key=lambda kv: kv[1]["idle_ms"])[0]

    return {
        "policy_id": policy["policy_id"],
        "days": days,
        "horizon_s": horizon_s,
        "throughput_good_parts": throughput,
        "throughput_per_day": round(throughput / days, 1),
        "line_oee": round(sum(s["oee"] for s in per.values()) / len(per), 4),
        "total_unplanned_downtime_h": round(
            sum(s["down_ms"] for s in per.values()) / 3.6e6, 2
        ),
        "bottleneck_by_ground_truth": bottleneck,
        "stations": per,
    }


# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--policy", default="policy_A.json")
    ap.add_argument("--run", default="baseline", help="name of this run; becomes data/run=<name>/")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--hz", type=float, default=5.0, help="telemetry sample rate per signal per station")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="data")
    ap.add_argument("--start-date", default="2026-08-03")
    ap.add_argument("--emit-default-policy", metavar="PATH",
                    help="write the baseline policy to PATH and exit")
    args = ap.parse_args()

    if args.emit_default_policy:
        with open(args.emit_default_policy, "w") as f:
            json.dump(default_policy(), f, indent=2)
        print(f"wrote baseline policy -> {args.emit_default_policy}")
        return

    policy = load_policy(args.policy)
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    out_dir = os.path.join(args.out, f"run={args.run}")
    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    est = len(LINE) * len(SIGNALS) * args.hz * SHIFT_SECONDS * args.days
    print(f"policy   : {policy['policy_id']}  ({args.policy})")
    print(f"run      : {args.run}   seed={args.seed}  days={args.days}  hz={args.hz}")
    print(f"telemetry: ~{est/1e6:.1f}M rows  (~{est*46/1e9:.2f} GB)")
    print()

    print("[1/4] simulating the line ...")
    stations, events, horizon_s = simulate_line(policy, args.days, args.seed)
    print(f"      {len(events):,} station-cycles over {horizon_s/3600:.1f} h")

    print("[2/4] writing part events ...")
    n_ev = write_part_events(out_dir, events, args.days, start_date)
    print(f"      {n_ev:,} rows")

    print("[3/4] writing maintenance log ...")
    n_mt = write_maintenance(out_dir, stations)
    print(f"      {n_mt:,} rows")

    print("[4/4] writing telemetry ...")
    n_tel = write_telemetry(out_dir, stations, horizon_s, args.hz, args.days, start_date, args.seed)
    print(f"      {n_tel:,} rows")

    summary = summarise(policy, stations, events, horizon_s, args.days)
    with open(os.path.join(out_dir, "run_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    print()
    print(f"throughput      {summary['throughput_good_parts']:,} good parts "
          f"({summary['throughput_per_day']}/day)")
    print(f"line OEE        {summary['line_oee']:.3f}")
    print(f"downtime        {summary['total_unplanned_downtime_h']} h")
    print(f"bottleneck      {summary['bottleneck_by_ground_truth']} (ground truth)")
    print()
    print(f"{'station':<8}{'OEE':>7}{'avail':>7}{'perf':>7}{'qual':>7}{'defect%':>9}"
          f"{'util':>7}{'cycle_s':>9}{'corr':>6}{'prev':>6}")
    for sid, s in summary["stations"].items():
        print(f"{sid:<8}{s['oee']:>7.3f}{s['availability']:>7.3f}{s['performance']:>7.3f}"
              f"{s['quality']:>7.3f}{s['defect_rate']*100:>9.2f}{s['utilisation']:>7.3f}"
              f"{s['mean_cycle_s']:>9.2f}{s['corrective']:>6}{s['preventive']:>6}")
    print()
    print(f"-> {out_dir}")


if __name__ == "__main__":
    main()
