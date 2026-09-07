# The MapReduce layer

Four jobs, each chosen so that it both computes something the control system
needs and demonstrates a distinct Hadoop technique. All are `ToolRunner` drivers,
so `-D` options work from the command line, and all export counters.

Run them through one entry point:

```
hadoop jar line-control-jobs.jar            # lists the jobs
hadoop jar line-control-jobs.jar oee        <part_events> <out>
hadoop jar line-control-jobs.jar bottleneck <part_events> <out>
hadoop jar line-control-jobs.jar limits     <telemetry> <out> -D lc.signal=torque -D lc.sigma=3.0
hadoop jar line-control-jobs.jar degradation <telemetry> <maintenance.csv> <out>
```

Or all four plus policy synthesis: `.\scripts\mine.ps1 -Run baseline`

| Job | Computes | Technique | Reads |
|---|---|---|---|
| **J1 `oee`** | Availability × Performance × Quality per station | combiner over a custom `Writable` of sufficient statistics | part_events |
| **J2 `bottleneck`** | utilisation, idle time, longest unbroken blocked run | secondary sort: composite key, custom partitioner, grouping comparator | part_events |
| **J3 `limits`** | mean, σ, k-σ control limits per station | combiner + numerically stable parallel variance | telemetry |
| **J4 `degradation`** | mean torque vs time-since-maintenance | map-side join via `DistributedCache` | telemetry ⨝ maintenance |

## Why each technique, in one line each

**J1 — combiner over sufficient statistics.** Every field in `OeeWritable` is
additive, so folding partials map-side gives bit-identical results. The ratios
are formed once, in the reducer: averaging per-cycle ratios is a different and
wrong number. Ideal cycle time is the best demonstrated cycle for the station —
standard practice when the nameplate rate isn't published, and it keeps the job
self-contained.

**J2 — secondary sort.** A count of rows per station cannot find a bottleneck: in
a serial line every station processes almost the same number of parts by
construction, so any difference is noise. The constraint is the station that is
idle *least*. Detecting runs of consecutive blocked cycles needs records in time
order, and a station accumulates millions of them, so buffering and sorting in
the reducer would blow the heap. Secondary sort pushes the ordering into the
shuffle, which is sorting anyway. **There is deliberately no combiner** — folding
map-side would destroy the per-record ordering the reducer depends on.

**J3 — stable variance, and mapper-side filtering.** Telemetry interleaves four
signals; pooling torque (~40), motor_temp (~60), vibration (~0.5) and
spindle_load (~62) gives limits that mean nothing. Filtering in the mapper also
removes ~75% of the shuffle before it touches the network — the job prints the
exact percentage. The merge uses Chan's parallel variance formula rather than
`(sumSq − sum²/n)/n`; the textbook form subtracts two large nearly-equal numbers
and can return a *negative* variance on data centred far from zero.

**J4 — map-side join.** The maintenance log is a few dozen rows; telemetry is
tens of millions. Shipping the small side to every mapper via the distributed
cache and joining in memory avoids the shuffle entirely. A reduce-side join here
would push the whole telemetry table across the network to accomplish nothing.
Knowing which join to pick, and being able to say why, is the point.

## Policy synthesis is not a MapReduce job

`sim/build_policy.py` consumes the four job outputs — about two dozen rows — and
writes the next `policy.json`. Forcing kilobytes through YARN to produce
kilobytes would be cargo-cult Hadoop. The honest framing, and the better viva
answer: **the batch tier is where the aggregation happens; policy synthesis is a
control decision taken on the aggregate.**

What drives what:

- J3 → `torque_lcl` / `torque_ucl`
- J4 → `pm_interval_cycles`, from the fitted mean-vs-age slope and a stated
  target: maintain once torque has risen 15% above fresh
- J2 → `buffer_capacity` on the constraint and the station feeding it
- J1 → reported for context; drives nothing on its own

## Validating against ground truth

The simulator writes `run_summary.json` with the true values. Check every job
against it before trusting any of it:

- J1's OEE per station must match `run_summary.json`
- J2 must name **S03** as the highest-utilisation station
- J4 must give **S04** the steepest slope

On synthetic job output shaped exactly like the real thing, `build_policy.py`
recovers PM intervals within 1% of `policy_B_oracle.json`, which was derived
from the simulator's true wear rates. That is the end-to-end check that the
mining chain works.

## Full loop

```powershell
python sim\line_sim.py --policy policy_A.json --run baseline --days 30 --hz 5
.\scripts\ingest.ps1 -Run baseline
.\scripts\build.ps1
.\scripts\mine.ps1 -Run baseline
python sim\run_ab.py --a policy_A.json --b artifacts\policy_B.json --seeds 20 --days 30 --split 20
```
