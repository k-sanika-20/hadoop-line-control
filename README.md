# Hadoop Line Control
 
A distributed control system for a **simulated** six-station robotic assembly line, where Apache
Hadoop is the plant-wide supervisory tier and the control loop actually closes through it.
 
Course project for **IM60210 — Distributed Analytics for Industrial Automation**, IIT Kharagpur.
 
---
 
## The idea
 
MapReduce has minutes of latency; a robot's control loop runs in milliseconds. That looks like a
contradiction until you notice that a factory control system is a *hierarchy*, not a single loop —
this is the standard ISA-95 automation pyramid:
 
| Level | Timescale | What decides | In this project |
|---|---|---|---|
| 1 | 1–10 ms | servo and PLC loops | inside the robot controller — deliberately out of scope |
| 2 | seconds | run / slow / hold / stop per cell | six edge controllers |
| 3 | hours–days | *what the setpoints should be* | **Hadoop** |
 
Nobody would put a servo loop on MapReduce. But deciding that station 3's torque alarm should move
from 42 Nm to 38 Nm, based on weeks of telemetry from every robot on the line, is a genuinely large
batch computation — and it *is* control, because the number goes back down to the machine and
changes what it does.
 
```
             ┌──────────────────────────────────────────────────────────┐
             │  LEVEL 3 · PLANT ANALYTICS · hours–days                   │
             │  Apache Hadoop — 3-node HDFS + YARN                       │
             │  J1 OEE   J2 constraint   J3 SPC limits   J4 degradation  │
             └──────────────────────────────────────────────────────────┘
                    ▲                                        │
   telemetry → HDFS │                                        │ policy.json → setpoints,
   4.8M rows/run    │                                        ▼ alarm limits, PM schedule
             ┌──────────────────────────────────────────────────────────┐
             │  LEVEL 2 · CELL SUPERVISORY CONTROL · seconds             │
             │  S01 load │ S02 weld │ S03 fasten │ S04 adhesive │ …      │
             └──────────────────────────────────────────────────────────┘
             ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
               LEVEL 1 · SERVO LOOPS · 1–10 ms — out of scope
             └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
```
 
Everything the line obeys — speed setpoints, SPC limits, preventive-maintenance intervals, buffer
capacities — is read from `policy.json`. Nothing is hardcoded. That is what makes the loop closable:
Hadoop mines the telemetry, writes a new policy, and the line is re-run under it.
 
## Results
 
Six stations, 7 simulated days, **4.84M telemetry rows** in HDFS across 3 DataNodes at replication 2.
Four MapReduce jobs on YARN, policy regenerated, scored over 20 paired seeds on a held-out horizon.
 
### Closed-loop evaluation
 
Mined on days 1–20, scored on days 21–30, same seeds in both arms:
 
| Metric | A · static baseline | B · mined | Δ | 95% CI | Verdict |
|---|---|---|---|---|---|
| Throughput (good parts) | 17259.5 | 17433.5 | +174.1 | ±124.7 | better, **+1.01%** |
| Unplanned downtime (h) | 4.776 | 3.913 | −0.864 | ±0.579 | better, **−18.08%** |
| Line OEE | 0.922 | 0.918 | −0.004 | ±0.001 | worse, −0.47% |
 
Mined maintenance intervals span **1478 to 12143 cycles** — an 8× spread recovered from telemetry
alone, against a baseline that set every station to 3000.
 
OEE falls slightly, and that is a real finding rather than a defect: stretching the maintenance
interval on slow-wearing stations saves stop time but lets wear accumulate, which inflates cycle
time and costs performance. The gain is concentrated in downtime, not throughput.
 
### Validation against planted ground truth
 
The simulator writes `run_summary.json` with the true values, and two facts are deliberately planted
in the plant model so the jobs have a known answer to be checked against.
 
| Check | Planted | Mined | |
|---|---|---|---|
| Line constraint (J2) | S03 | **S03** | utilisation 0.957, highest on the line |
| Fastest degradation (J4) | S04 | **S04** | slope 0.763 torque/h, 6.9× the slowest station |
| Per-station OEE (J1) | `run_summary.json` | **exact match** | 0.957 / 0.919 / 0.934 / 0.823 / 0.974 / 0.959 |
 
## The first mined policy was worse than the baseline
 
Run 1 scored **−3.8% throughput and +21.6% unplanned downtime** against the static policy. Two
independent measurement errors, multiplying:
 
**1. Stopped machines still emit telemetry.** A halted station reports near-zero torque, and stops
occur at the *end* of a wear cycle — so those readings landed in the highest age buckets and
flattened the fitted degradation slope.
 
| | S01 | S02 | S03 | S04 | S05 | S06 |
|---|---|---|---|---|---|---|
| measured / true slope | 0.73 | 0.84 | 0.80 | 0.59 | 0.48 | 0.64 |
| excluding non-running samples | **0.98** | **0.97** | **0.96** | **0.99** | **0.92** | **0.91** |
 
The same contamination inflated σ by 1.5–4× (S01: 2.280 measured against a 1.10 noise floor),
leaving the mined control limits far too wide for the derate action to ever fire.
 
**2. Cycles versus wall-clock.** The degradation curve's x-axis is wall-clock age, but a maintenance
interval is counted in *cycles* — and stations sit blocked or starved 15–34% of the time. Dividing by
processing time instead of wall-clock overstates the interval by 1/utilisation.
 
The two compose exactly. For S06: `1/0.64 × 1/0.711 = 2.20`, against an observed interval ratio of
`14757/6667 = 2.21`.
 
Both are fixed (`-D lc.min.value` excludes non-running samples; `build_policy.py` converts through
utilisation). The failed run is kept in the write-up on purpose — the ground-truth checks are what
caught it, and that is the argument for planting them.
 
## The MapReduce layer
 
Four jobs, each chosen so it both computes something the control system needs and demonstrates a
distinct Hadoop technique. Full reasoning in [`JOBS.md`](JOBS.md).
 
| Job | Computes | Technique |
|---|---|---|
| **J1 `oee`** | Availability × Performance × Quality per station | combiner over a custom `Writable` of sufficient statistics |
| **J2 `bottleneck`** | utilisation, idle time, longest unbroken blocked run | secondary sort — composite key, custom partitioner, grouping comparator |
| **J3 `limits`** | mean, σ, 3σ control limits | combiner + numerically stable parallel variance |
| **J4 `degradation`** | mean torque vs time-since-maintenance | map-side join via `DistributedCache` |
 
Measured on the 7-day run:
 
| | |
|---|---|
| J3 combiner: map output → shuffled | **52.8 MB → 2,184 bytes** (1.20M records → 42) |
| J3 mapper-side signal filtering | **75.1%** of rows dropped before the network |
| J1 combiner | 74,460 records → 42 |
| J2 combiner | **0** — deliberately, to preserve secondary-sort ordering |
| Data locality (J4) | 33 of 42 map tasks data-local |
| Non-running samples excluded | 8,587 |
 
Two deliberate choices worth stating:
 
- **J2 has no combiner.** Folding values map-side would destroy the per-record ordering the reducer
  depends on. The `Combine input records=0` counter is the evidence.
- **J3 uses Chan's parallel variance**, not `(sumSq − sum²/n)/n`. The textbook form subtracts two
  large nearly-equal numbers and can return a negative variance on data centred far from zero — like
  motor temperature at ~60.
**Policy synthesis is not a MapReduce job.** `sim/build_policy.py` consumes the four job outputs —
about two dozen rows — and writes the next `policy.json`. Forcing kilobytes through YARN to produce
kilobytes would be cargo-cult Hadoop. The batch tier is where the *aggregation* happens; policy
synthesis is a control decision taken on the aggregate.
 
## Cluster
 
Six containers on one Docker network: a NameNode, a ResourceManager, three workers each running a
DataNode and a NodeManager, and a client container to issue commands from. Replication factor 2
across three DataNodes, so the cluster survives losing one.
 
Sized to fit an 8 GB Docker allocation on a 16 GB laptop — the memory values in `hadoop-conf/` are
deliberate, not defaults.
 
- NameNode UI — http://localhost:9870
- ResourceManager UI — http://localhost:8088
## Reproducing
 
Requires Docker Desktop (≥8 GB allocated), Python 3.10+, and Git. **No local JDK needed** — the
MapReduce jobs compile inside a Maven container.
 
```powershell
.\scripts\up.ps1                                                   # start the cluster
python sim\line_sim.py --emit-default-policy policy_A.json         # baseline policy
python sim\line_sim.py --policy policy_A.json --run baseline --days 7 --hz 1
.\scripts\ingest.ps1 -Run baseline                                 # load into HDFS
.\scripts\build.ps1                                                # compile the jobs
.\scripts\mine.ps1  -Run baseline                                  # run J1-J4, build policy B
python sim\run_ab.py --a policy_A.json --b artifacts\policy_B.json --seeds 20 --days 30 --split 20
```
 
On PowerShell 5.1, prefix each script with `powershell -ExecutionPolicy Bypass -File`.
 
Scale up with `--days 30 --hz 5` (~104M rows, ~4.7 GB) once the jobs are verified on the small run.
Output is plain CSV, deliberately: gzip is not splittable, so a compressed file is read by exactly
one mapper and you lose the block-level parallelism that is the whole reason for HDFS.
 
## Layout
 
```
docker-compose.yml     6-container HDFS + YARN cluster
hadoop-conf/           site XMLs, mounted into every container
jobs/                  Java MapReduce jobs (Maven)
sim/                   line simulator, policy synthesis, A/B harness
scripts/               PowerShell helpers
policy_A.json          static baseline policy
policy_B_oracle.json   reference policy from the simulator's true wear rates - NOT a result
artifacts/             mined policy and results
data/                  generated telemetry - gitignored
```
 
`JOBS.md` covers the MapReduce layer; `SIMULATOR.md` covers the data model and schemas.
 
## Limitations and future work
 
- The line is **simulated**. No public dataset offers robot telemetry *and* the ability to change the
  control policy and re-run, which is what closing the loop requires.
- 20 seeds is underpowered for the downtime effect at this variance; the direction is stable across
  runs but a tighter interval would need ~60.
- No streaming tier. Level 2 currently gets sub-second decisions only from static limits — a
  streaming layer alongside the batch tier is the honest architecture, and naming what batch cannot
  do is part of the argument.
- Policies are statistically mined, not learned. A Level 3 optimiser rather than a summariser is the
  natural next step.