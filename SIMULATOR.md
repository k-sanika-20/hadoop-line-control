# The simulator

`sim/line_sim.py` is a discrete-event model of a six-station tandem line with
finite buffers. It replaces the throwaway `random.gauss` seed data: cycle times,
defects, faults and telemetry now come from a plant that has structure, and that
structure is what the MapReduce jobs are supposed to discover.

## Run it

```powershell
python sim\line_sim.py --emit-default-policy policy_A.json   # once
python sim\line_sim.py --policy policy_A.json --run baseline --days 30 --hz 5
.\scripts\ingest.ps1 -Run baseline
```

`--days 30 --hz 5` produces roughly 104M telemetry rows (~4.7 GB). Start with
`--days 7 --hz 1` (~5M rows) while you are still debugging jobs, then scale up
once they are correct — a wrong job is much cheaper to find on 200 MB.

Output is plain CSV, deliberately. Gzip is not splittable, so a compressed file
is read by exactly one mapper and you would lose the block-level parallelism
that is the entire reason for using HDFS. That is worth saying in the viva.

## Schemas

**`telemetry/dt=<date>/station=<S>/part-0.csv`**

```
ts_ms,station,signal,value
```

`signal` is one of `torque`, `motor_temp`, `vibration`, `spindle_load`.

> **This is the change that will break your current jobs.** There are now four
> signals interleaved in one file. `ControlLimits` must filter to
> `signal == "torque"` before computing mean and sigma, or it will pool torque
> (~40), temperature (~60), vibration (~0.5) and load (~62) into one meaningless
> distribution. Filtering in the mapper also drops 75% of the shuffle volume.

**`part_events/dt=<date>/part-0.csv`**

```
ts_end_ms,station,part_id,cycle_ms,blocked_ms,starved_ms,down_ms,status,defect_code
```

This is what real OEE comes from — you cannot compute it from telemetry alone:

- Availability = (planned − down_ms) / planned
- Performance = (ideal_cycle_s × 1000 × parts) / Σ cycle_ms
- Quality = PASS / parts
- OEE = A × P × Q

`blocked_ms` and `starved_ms` are what identify the bottleneck. The bottleneck of
a tandem line is the station that is idle *least* — it never waits, everyone else
waits on it. A count of rows per station cannot find it.

**`maintenance/maintenance.csv`**

```
ts_ms,station,event,duration_ms
```

Small — a few dozen rows. That makes it the natural small side of a **map-side
join** via `DistributedCache`, against the large telemetry table. Joining
telemetry to the preceding maintenance event gives cycles-since-maintenance,
which is what a degradation curve is actually plotted against.

**`run_summary.json`** — ground truth computed by the simulator itself. Cross-check
every MapReduce job against it. If your OEE job disagrees with `run_summary.json`,
your job is wrong; do not proceed until they match.

## Ground truth planted in the model

| | |
|---|---|
| **S03 `fasten`** | longest nominal cycle → the **true bottleneck** (highest utilisation) |
| **S04 `adhesive`** | ~4× the wear rate of S01 → **degrades fastest**, takes unplanned failures, worst OEE |

These are physical properties of the plant in `LINE`, not policy. A correct
bottleneck job must find S03; a correct degradation job must find S04. If your
jobs point somewhere else, they are wrong — which is exactly the check the old
seed data could not give you.

## Closing the loop

Everything the line obeys is read from the policy file: `speed_setpoint`,
`torque_lcl` / `torque_ucl`, `pm_interval_cycles`, `buffer_capacity`. The edge
controller derates a station for 40 cycles when its torque breaches the UCL,
which cuts defects and costs throughput — so the limits your `ControlLimits` job
mines genuinely change how the line behaves.

`sim/run_ab.py` scores a mined policy against the baseline:

```powershell
python sim\run_ab.py --a policy_A.json --b artifacts\policy_B.json --seeds 20 --days 30 --split 20
```

`--split 20` mines on days 1–20 and scores on days 21–30. Without it you are
tuning and testing on the same data. `--seeds 20` runs paired seeds and reports
95% confidence intervals; a single seed swings several percent on noise alone.

`policy_B_oracle.json` is a **reference, not a deliverable** — its PM intervals
are derived from the simulator's true wear rates, so it shows roughly what a
well-mined policy should recover. Use it to check whether your J5 output is in
the right region. Do not submit it as a result.

## What the oracle policy actually achieves

20 paired seeds, 30 days, scored on days 21–30:

| metric | A baseline | B oracle | delta | 95% CI | verdict |
|---|---|---|---|---|---|
| throughput (good parts) | 17259.5 | 17434.4 | +174.9 | ±103.8 | B better (+1.01%) |
| unplanned downtime (h) | 4.78 | 3.90 | −0.88 | ±0.50 | B better (−18.34%) |
| line OEE | 0.922 | 0.920 | −0.002 | ±0.001 | **B worse** (−0.23%) |

The mixed result is the honest one and worth presenting as-is. The win is in
unplanned downtime, not throughput. OEE goes very slightly *down* because
lengthening the maintenance interval on the slow-wearing stations saves stop
time but lets wear accumulate, which inflates cycle time and costs performance.
That trade-off is a real finding about the plant, and a result table where every
metric improved is the one examiners disbelieve.
