# Hadoop Line Control

A distributed control system for a robotic manufacturing assembly line, built on Apache Hadoop.

Course project for **IM60210 Distributed Analytics for Industrial Automation**, IIT Kharagpur.

---

## The idea in one paragraph

A factory control system is a hierarchy, not a single loop. Servo loops run in milliseconds inside
the robot controller; cell supervisory control runs in seconds; and plant-wide analysis - deciding
what the setpoints, alarm limits and maintenance intervals *should be* - runs over hours and days.
Hadoop belongs on that third tier, and this project puts it there. Six simulated robotic stations
emit telemetry into HDFS; MapReduce jobs mine weeks of it for OEE, line bottlenecks, statistical
process-control limits and tool-degradation curves; and the result is written back out as a policy
file that the station controllers obey. The loop closes through the batch layer.

## Status

| | |
|---|---|
| Cluster | not yet verified |
| Simulator | not started |
| MapReduce jobs | 1 of 5 (SmokeCount, gate job) |
| Closed-loop experiment | not started |

## Cluster

Six containers on one Docker network: a NameNode, a ResourceManager, three workers each running a
DataNode and a NodeManager, and a client container to issue commands from. Replication factor 2
across three DataNodes, so the cluster survives losing one - which is also the demo.

Sized to fit an 8 GB Docker allocation on a 16 GB laptop. See `hadoop-conf/` for the site XMLs;
the memory numbers there are deliberate, not defaults.

```powershell
.\scripts\up.ps1        # start, wait for safe mode to clear, report live nodes
.\scripts\build.ps1     # compile the MapReduce jars in a Maven container
.\scripts\gate.ps1      # prove a stock job AND your own jar run on YARN
.\scripts\down.ps1      # stop  (-Wipe also deletes HDFS data)
```

- NameNode UI — http://localhost:9870
- ResourceManager UI — http://localhost:8088

## Results

<!-- filled from artifacts/results.json at the end of Day 2 -->

_Pending._

## Repository layout

```
hadoop-conf/     Hadoop site XMLs, mounted into every container
docker-compose.yml
jobs/            Java MapReduce jobs (Maven)
scripts/         PowerShell helpers
sim/             assembly-line simulator            (not yet written)
data/            generated telemetry - gitignored
artifacts/       metrics and policy files - gitignored
```
