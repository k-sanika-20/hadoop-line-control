"""
Hadoop Line Control - interactive dashboard.

    streamlit run dashboard/app.py

Reads what the pipeline already wrote (artifacts/, policy files), so every tab
except "Cluster" works with the cluster stopped. The "Closed loop" tab can
re-synthesise the policy with different settings and re-score it against the
baseline on the simulator, live; the "Cluster" tab polls the NameNode and
ResourceManager REST APIs, which is the view to keep open during failover.ps1.
"""

import os
import statistics
import sys

import altair as alt
import pandas as pd
import requests
import streamlit as st

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "sim"))
import build_policy as BP  # noqa: E402
import line_sim as LS      # noqa: E402
import run_ab as AB        # noqa: E402
import viz_data as V       # noqa: E402

st.set_page_config(page_title="Hadoop Line Control", page_icon="🏭", layout="wide")

STATION_SCALE = alt.Scale(domain=V.STATIONS, range=[V.STATION_COLORS[s] for s in V.STATIONS])
STATE_SCALE = alt.Scale(domain=["busy", "blocked", "starved"], range=["#2a78d6", "#eb6834", "#1baf7a"])
POLICY_SCALE = alt.Scale(domain=list(V.POLICY_COLORS), range=list(V.POLICY_COLORS.values()))
NAMENODE, RESOURCEMANAGER = "http://localhost:9870", "http://localhost:8088"


# --------------------------------------------------------------------------
# Data
# --------------------------------------------------------------------------

@st.cache_data
def load_all():
    if not os.path.isdir(os.path.join(V.MINED, "limits")):
        return None
    summary = None
    for p in (("data", "run=baseline", "run_summary.json"), ("artifacts", "run_summary.json")):
        if os.path.exists(V.path(*p)):
            summary = V.load_json(*p)
            break
    return {
        "stations": V.station_table(),
        "time_split": V.time_split(),
        "degradation": V.degradation_points(fitted_only=True),
        "policy_a": V.load_json("policy_A.json"),
        "policy_b": V.load_json("artifacts", "policy_B.json"),
        "results": V.load_json("artifacts", "results.json"),
        "summary": summary,
    }


@st.cache_data(show_spinner=False)
def baseline_run(seed, days, split):
    """Policy A never changes, so its runs are cached across what-if requests."""
    return AB.evaluate(V.load_json("policy_A.json"), seed, days, split)


def get_json(url):
    try:
        r = requests.get(url, timeout=2)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


D = load_all()
if D is None:
    st.error("No job output under artifacts/mined. Run the pipeline first: "
             "`.\\scripts\\mine.ps1 -Run baseline`.")
    st.stop()

S = D["stations"]
constraint = S.loc[S.utilisation.idxmax(), "station"]
fastest = S.loc[S.slope_per_h.idxmax(), "station"]


# --------------------------------------------------------------------------
# Header
# --------------------------------------------------------------------------

st.title("Hadoop Line Control")
st.caption("A distributed control system for a simulated six-station robotic assembly line. "
           "Hadoop is the plant-wide supervisory tier (ISA-95 level 3): it mines telemetry, "
           "writes a new policy, and the line runs under it.")

tabs = st.tabs(["Overview", "Line analytics", "Wear & SPC", "Policy", "Closed loop",
                "Validation", "Cluster"])


# --------------------------------------------------------------------------
# Overview
# --------------------------------------------------------------------------

with tabs[0]:
    m = D["results"]["metrics"]
    c = st.columns(4)
    c[0].metric("Telemetry rows mined", "4.84M", help="6 stations x 4 signals x 7 days at 1 Hz, in HDFS at replication 2")
    c[1].metric("Line constraint (J2)", f"{constraint} {V.STATION_NAMES[constraint]}",
                f"utilisation {S.utilisation.max():.1%}", delta_color="off")
    c[2].metric("Throughput, mined vs baseline", f"{m['throughput_good_parts']['b_mean']:,.0f}",
                f"{m['throughput_good_parts']['delta_pct']:+.2f}%")
    c[3].metric("Unplanned downtime", f"{m['total_unplanned_downtime_h']['b_mean']:.2f} h",
                f"{m['total_unplanned_downtime_h']['delta_pct']:+.1f}%", delta_color="inverse")

    st.subheader("The line")
    cols = st.columns(len(V.STATIONS))
    for col, (_, r) in zip(cols, S.iterrows()):
        with col.container(border=True):
            tag = " · constraint" if r.station == constraint else (" · fastest wear" if r.station == fastest else "")
            st.markdown(f"**{r.station}** {r['name']}  \n<span style='color:#898781'>{tag or '&nbsp;'}</span>",
                        unsafe_allow_html=True)
            st.metric("utilisation", f"{r.utilisation:.0%}")
            st.metric("OEE", f"{r.oee:.3f}")
            st.progress(min(1.0, r.utilisation))
    st.caption("Parts flow left to right through finite buffers. A station is *blocked* when the buffer "
               "after it is full and *starved* when the one before it is empty.")

    st.subheader("How the loop closes")
    st.markdown(
        "| Level | Timescale | Decides | Here |\n|---|---|---|---|\n"
        "| 1 | 1-10 ms | servo and PLC loops | inside the robot controller (out of scope) |\n"
        "| 2 | seconds | run / derate / stop per cell | six edge controllers in the simulator |\n"
        "| 3 | hours-days | **what the setpoints should be** | **Hadoop: J1-J4 on a 3-node HDFS + YARN cluster** |\n\n"
        "Telemetry → HDFS → MapReduce (OEE, constraint, SPC limits, degradation) → `build_policy.py` → "
        "`policy_B.json` → the line re-runs under it → scored against the static baseline on held-out days."
    )


# --------------------------------------------------------------------------
# Line analytics (J1, J2)
# --------------------------------------------------------------------------

with tabs[1]:
    left, right = st.columns(2)
    with left:
        st.subheader("Where each station's time goes (J2)")
        t = D["time_split"].copy()
        t["order"] = t.state.map({"busy": 0, "blocked": 1, "starved": 2})
        chart = alt.Chart(t).mark_bar(cornerRadiusEnd=3, stroke="white", strokeWidth=1).encode(
            x=alt.X("share:Q", stack="normalize", axis=alt.Axis(format="%", title=None)),
            y=alt.Y("station:N", sort=V.STATIONS, title=None),
            color=alt.Color("state:N", scale=STATE_SCALE, legend=alt.Legend(orient="top", title=None)),
            order="order:Q",
            tooltip=["station", "state", alt.Tooltip("share:Q", format=".1%"),
                     alt.Tooltip("hours:Q", format=".1f", title="hours")],
        ).properties(height=280)
        st.altair_chart(chart, width="stretch")
        st.caption(f"The constraint is the station that is idle least: **{constraint}**. "
                   "Found by a secondary-sort job, because a row count per station cannot see it.")
    with right:
        st.subheader("OEE breakdown (J1)")
        o = S.melt(id_vars="station", value_vars=["availability", "performance", "quality"],
                   var_name="factor", value_name="value")
        # Bars run up from the 80% baseline, not from zero, so nothing spills
        # below the plot when the axis is truncated.
        chart = alt.Chart(o).mark_bar(cornerRadiusEnd=3).encode(
            x=alt.X("station:N", title=None, sort=V.STATIONS, axis=alt.Axis(labelAngle=0)),
            xOffset="factor:N",
            y=alt.Y("value:Q", scale=alt.Scale(domain=[0.8, 1.0], zero=False),
                    axis=alt.Axis(format="%", title=None, values=[0.8, 0.85, 0.9, 0.95, 1.0])),
            y2=alt.datum(0.8),
            color=alt.Color("factor:N", scale=alt.Scale(
                domain=["availability", "performance", "quality"], range=["#2a78d6", "#eb6834", "#1baf7a"]),
                legend=alt.Legend(orient="top", title=None)),
            tooltip=["station", "factor", alt.Tooltip("value:Q", format=".2%")],
        ).properties(height=280)
        st.altair_chart(chart, width="stretch")
        st.caption("Y axis starts at 80% to separate close values. OEE = A x P x Q, "
                   f"lowest at **{S.loc[S.oee.idxmin(), 'station']}**.")

    st.subheader("Per-station table")
    st.dataframe(
        S[["station", "name", "oee", "availability", "performance", "quality", "defect_rate",
           "utilisation", "mean_cycle_s", "blocked_s", "starved_s"]],
        hide_index=True, width="stretch",
        column_config={
            "oee": st.column_config.ProgressColumn("OEE", format="%.3f", min_value=0.8, max_value=1.0),
            "utilisation": st.column_config.ProgressColumn("utilisation", format="%.3f", min_value=0, max_value=1),
            "defect_rate": st.column_config.NumberColumn(format="%.4f"),
        })


# --------------------------------------------------------------------------
# Wear & SPC (J3, J4)
# --------------------------------------------------------------------------

with tabs[2]:
    st.subheader("Tool degradation (J4)")
    picked = st.multiselect("Stations", V.STATIONS, default=V.STATIONS,
                            format_func=lambda s: f"{s} {V.STATION_NAMES[s]}")
    unit = st.radio("Y axis", ["% above fresh", "torque (Nm)"], horizontal=True)
    dd = D["degradation"][D["degradation"].station.isin(picked)]
    ycol, ytitle = ("rise_pct", "mean torque above fresh (%)") if unit.startswith("%") else ("mean_torque", "mean torque (Nm)")
    base = alt.Chart(dd).encode(
        x=alt.X("age_h:Q", title="time since last maintenance (h)"),
        y=alt.Y(f"{ycol}:Q", title=ytitle),
        color=alt.Color("station:N", scale=STATION_SCALE, legend=alt.Legend(orient="top", title=None)),
        tooltip=["station", alt.Tooltip("age_h:Q", title="age (h)"), alt.Tooltip("mean_torque:Q", format=".2f"),
                 alt.Tooltip("rise_pct:Q", format=".1f", title="% above fresh"), "n"],
    )
    layers = [base.mark_line(strokeWidth=2), base.mark_circle(size=40)]
    if ycol == "rise_pct":
        layers.append(alt.Chart(pd.DataFrame({"y": [15]})).mark_rule(color="#c3c2b7").encode(y="y:Q"))
    st.altair_chart(alt.layer(*layers).properties(height=340), width="stretch")
    st.caption(f"**{fastest}** degrades fastest ({S.slope_per_h.max():.2f} Nm/h). The grey rule is the "
               "maintenance target: service a tool once torque is 15% above fresh. The map-side join "
               "with the maintenance log is what gives each reading its age.")

    st.subheader("SPC control limits on torque (J3 mined vs baseline)")
    a, b = D["policy_a"]["stations"], D["policy_b"]["stations"]
    lim = pd.DataFrame(
        [{"station": s, "policy": "A · baseline", "lcl": a[s]["torque_lcl"], "ucl": a[s]["torque_ucl"]} for s in V.STATIONS] +
        [{"station": s, "policy": "B · mined", "lcl": b[s]["torque_lcl"], "ucl": b[s]["torque_ucl"]} for s in V.STATIONS])
    means = S[["station", "torque_mean"]]
    bars = alt.Chart(lim).mark_bar(size=10, cornerRadius=3).encode(
        y=alt.Y("station:N", sort=V.STATIONS, title=None), yOffset="policy:N",
        x=alt.X("lcl:Q", title="torque (Nm)", scale=alt.Scale(zero=False)), x2="ucl:Q",
        color=alt.Color("policy:N", scale=POLICY_SCALE, legend=alt.Legend(orient="top", title=None)),
        tooltip=["station", "policy", alt.Tooltip("lcl:Q", format=".2f"), alt.Tooltip("ucl:Q", format=".2f")])
    ticks = alt.Chart(means).mark_tick(color="#eda100", thickness=3, size=26).encode(
        y=alt.Y("station:N", sort=V.STATIONS), x="torque_mean:Q",
        tooltip=["station", alt.Tooltip("torque_mean:Q", format=".2f", title="mined mean")])
    st.altair_chart((bars + ticks).properties(height=320), width="stretch")
    st.caption("Yellow tick = mined mean torque. Baseline A is nameplate torque ± 4σ; mined B is mean ± 3σ "
               "of what each station actually runs at, from J3 (Chan's parallel variance, non-running "
               "samples excluded). S04's band is wider and higher because its tool drifts across a "
               "maintenance cycle. A breach of the UCL makes the edge controller derate the station.")


# --------------------------------------------------------------------------
# Policy
# --------------------------------------------------------------------------

with tabs[3]:
    st.subheader("What Hadoop changed")
    pt = V.policy_table(D["policy_a"], D["policy_b"])
    field = st.selectbox("Field", pt.field.unique(), index=3)
    sub = pt[pt.field == field]
    long = sub.melt(id_vars=["station"], value_vars=["A", "B"], var_name="p", value_name="value")
    long["policy"] = long.p.map({"A": "A · baseline", "B": "B · mined"})
    rule = alt.Chart(sub).mark_rule(color="#c3c2b7", strokeWidth=2).encode(
        y=alt.Y("station:N", sort=V.STATIONS, title=None), x="A:Q", x2="B:Q")
    dots = alt.Chart(long).mark_circle(size=140, opacity=1, stroke="white", strokeWidth=2).encode(
        y=alt.Y("station:N", sort=V.STATIONS), x=alt.X("value:Q", title=field, scale=(
            alt.Scale(type="log", domain=[1000, 15000]) if field == "pm_interval_cycles" else alt.Scale(zero=False))),
        color=alt.Color("policy:N", scale=POLICY_SCALE, legend=alt.Legend(orient="top", title=None)),
        tooltip=["station", "policy", "value"])
    st.altair_chart((rule + dots).properties(height=300), width="stretch")
    st.caption({
        "pm_interval_cycles": "From J4 slopes, converted from wall-clock hours to cycles through J2 utilisation. Log scale.",
        "torque_lcl": "From J3: mean − 3σ.", "torque_ucl": "From J3: mean + 3σ.",
        "buffer_capacity": f"From J2: buffers around the constraint ({constraint}) grow 1.5x.",
        "speed_setpoint": "Unchanged by design.",
    }[field])

    st.subheader("Full policy diff")
    def fmt(v):
        return f"{v:g}" if isinstance(v, float) else f"{v}"
    show = pt.assign(change=lambda d: d.apply(
        lambda r: f"{fmt(r.A)} → {fmt(r.B)}" if r.changed else fmt(r.A), axis=1)) \
        .pivot(index="station", columns="field", values="change")
    st.dataframe(show, width="stretch")
    st.download_button("Download policy_B.json", data=open(V.path("artifacts", "policy_B.json")).read(),
                       file_name="policy_B.json", mime="application/json")


# --------------------------------------------------------------------------
# Closed loop
# --------------------------------------------------------------------------

def ab_chart(t):
    t = t.copy()
    t["lo"], t["hi"] = t.delta_pct - t.ci_pct, t.delta_pct + t.ci_pct
    t["outcome"] = t.verdict.map({"B better": "better", "B WORSE": "worse", "no effect": "no effect"})
    scale = alt.Scale(domain=["better", "worse", "no effect"], range=[V.GOOD, V.CRITICAL, "#898781"])
    charts = []
    for _, r in t.iterrows():
        one = pd.DataFrame([r])
        span = max(abs(r.lo), abs(r.hi), 0.5) * 1.3
        x = alt.X("delta_pct:Q", title="change vs baseline (%)", scale=alt.Scale(domain=[-span, span]))
        err = alt.Chart(one).mark_rule(strokeWidth=3).encode(x=alt.X("lo:Q", scale=alt.Scale(domain=[-span, span])),
                                                              x2="hi:Q", color=alt.Color("outcome:N", scale=scale, legend=None))
        pt_ = alt.Chart(one).mark_circle(size=160, opacity=1).encode(
            x=x, color=alt.Color("outcome:N", scale=scale, legend=None),
            tooltip=["metric", alt.Tooltip("A:Q", format=",.3f"), alt.Tooltip("B:Q", format=",.3f"),
                     alt.Tooltip("delta_pct:Q", format="+.2f", title="change %"),
                     alt.Tooltip("ci_pct:Q", format=".2f", title="95% CI ±%"), "verdict"])
        zero = alt.Chart(pd.DataFrame({"x": [0]})).mark_rule(color="#c3c2b7").encode(x="x:Q")
        charts.append((zero + err + pt_).properties(title=r.metric, height=70, width=230))
    return alt.hconcat(*charts)


with tabs[4]:
    r = D["results"]
    st.subheader("Mined policy B vs static baseline A")
    st.caption(f"{r['seeds']} paired seeds, {r['days']} days, scored on held-out days "
               f"{r['split_day'] + 1}-{r['days']}. Same random seeds in both arms, so the only "
               "difference is the policy. Bars are 95% CI on the paired difference.")
    t = V.ab_table(r)
    st.altair_chart(ab_chart(t), width="content")
    st.dataframe(t[["metric", "A", "B", "delta", "ci95", "delta_pct", "verdict"]], hide_index=True,
                 width="stretch", column_config={"delta_pct": st.column_config.NumberColumn("change %", format="%+.2f")})
    st.info("OEE drops slightly while downtime falls 18%: longer intervals on slow-wearing stations save "
            "stop time but let wear accumulate, which inflates cycle time. A real trade-off, reported as-is.")

    st.divider()
    st.subheader("What-if: re-mine the policy with different settings")
    st.caption("Re-runs `build_policy.synthesise()` on the same Hadoop job output, then scores the new "
               "policy against the baseline on the simulator. Fewer seeds = faster but noisier.")
    c1, c2, c3 = st.columns(3)
    rise = c1.slider("Maintenance target: torque rise above fresh", 0.05, 0.30, BP.TARGET_TORQUE_RISE, 0.01,
                     format="%.2f", help="Lower = service tools sooner. The mined policy uses 0.15.")
    sigma = c2.slider("Control-limit width (σ)", 2.0, 4.0, 3.0, 0.25,
                      help="Narrower limits derate drifting stations sooner, costing throughput.")
    seeds = c3.slider("Paired seeds", 1, 10, 3)
    syn = BP.synthesise(D["policy_a"], V.MINED, sigma=sigma, target_rise=rise)
    pm = pd.DataFrame([{"station": s, "PM A": D["policy_a"]["stations"][s]["pm_interval_cycles"],
                        "PM mined (saved)": D["policy_b"]["stations"][s]["pm_interval_cycles"],
                        "PM what-if": syn["policy"]["stations"][s]["pm_interval_cycles"],
                        "UCL what-if": syn["policy"]["stations"][s]["torque_ucl"]} for s in V.STATIONS])
    st.dataframe(pm, hide_index=True, width="stretch")

    if st.button(f"Score it ({seeds} seed{'s' if seeds > 1 else ''}, ~{seeds * 12} s)", type="primary"):
        days, split = r["days"], r["split_day"]
        bar = st.progress(0.0, text="simulating ...")
        runs = []
        for i, seed in enumerate(range(1, seeds + 1), 1):
            ra = baseline_run(seed, days, split)
            rb = AB.evaluate(syn["policy"], seed, days, split)
            runs.append((ra, rb))
            bar.progress(i / seeds, text=f"seed {i}/{seeds}")
        bar.empty()
        rows = []
        for key, label, direction in AB.METRICS:
            a = [x[key] for x, _ in runs]
            d = [y[key] - x[key] for x, y in runs]
            ma = statistics.mean(a)
            md, hd = AB.ci95(d)
            better = (md > 0) if direction == "higher" else (md < 0)
            verdict = "no effect" if seeds > 1 and abs(md) <= hd else ("B better" if better else "B WORSE")
            rows.append({"metric": label, "A": ma, "B": ma + md, "delta": md, "ci95": hd,
                         "delta_pct": md / ma * 100 if ma else 0, "ci_pct": hd / ma * 100 if ma else 0,
                         "verdict": verdict})
        wt = pd.DataFrame(rows)
        st.altair_chart(ab_chart(wt), width="content")
        st.dataframe(wt[["metric", "A", "B", "delta", "ci95", "delta_pct", "verdict"]], hide_index=True,
                     width="stretch")
        if seeds == 1:
            st.warning("One seed has no confidence interval - a single run swings several percent on noise alone.")


# --------------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------------

with tabs[5]:
    st.subheader("Checked against ground truth planted in the plant model")
    gt = D["summary"]
    c = st.columns(2)
    c[0].metric("J2 line constraint", constraint, "planted: S03 · match" if constraint == "S03" else "planted: S03 · MISMATCH",
                delta_color="normal" if constraint == "S03" else "inverse")
    c[1].metric("J4 fastest degradation", fastest, "planted: S04 · match" if fastest == "S04" else "planted: S04 · MISMATCH",
                delta_color="normal" if fastest == "S04" else "inverse")
    if gt:
        rows = []
        for _, s_ in S.iterrows():
            true = gt["stations"][s_.station]
            for k in ("oee", "availability", "performance", "quality", "utilisation"):
                rows.append({"station": s_.station, "metric": k, "mined (Hadoop)": s_[k],
                             "simulator truth": true[k], "match": "✓" if abs(s_[k] - true[k]) < 5e-4 else "✗"})
        v = pd.DataFrame(rows)
        n_ok = (v.match == "✓").sum()
        if n_ok == len(v):
            st.success(f"{n_ok} of {len(v)} per-station values match `run_summary.json` exactly.")
        else:
            st.warning(f"{n_ok} of {len(v)} per-station values match `run_summary.json`.")
        st.dataframe(v, hide_index=True, width="stretch")
    else:
        st.info("run_summary.json not found (data/run=baseline/ or artifacts/) - generate the run to see the per-station check.")


# --------------------------------------------------------------------------
# Cluster (live)
# --------------------------------------------------------------------------

with tabs[6]:
    st.subheader("Live cluster")
    live = st.toggle("Auto-refresh every 5 s", value=False,
                     help="Turn on, then run .\\scripts\\failover.ps1 to watch a datanode die and HDFS heal.")

    @st.fragment(run_every=5 if live else None)
    def cluster_panel():
        fs = get_json(f"{NAMENODE}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState")
        nn = get_json(f"{NAMENODE}/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo")
        rm = get_json(f"{RESOURCEMANAGER}/ws/v1/cluster/metrics")
        if not fs or not fs.get("beans"):
            st.warning("Cluster not reachable at localhost:9870 / :8088. Start it with `.\\scripts\\up.ps1`.")
            return
        f = fs["beans"][0]
        cm = (rm or {}).get("clusterMetrics", {})
        c = st.columns(5)
        c[0].metric("Live datanodes", f.get("NumLiveDataNodes"), f"{f.get('NumDeadDataNodes', 0)} dead",
                    delta_color="inverse" if f.get("NumDeadDataNodes") else "off")
        c[1].metric("Under-replicated blocks", f.get("UnderReplicatedBlocks", 0))
        c[2].metric("Blocks", f.get("BlocksTotal"))
        c[3].metric("YARN nodes", cm.get("activeNodes", "-"), f"{cm.get('lostNodes', 0)} lost",
                    delta_color="inverse" if cm.get("lostNodes") else "off")
        c[4].metric("Apps running", cm.get("appsRunning", "-"), f"{cm.get('appsCompleted', 0)} completed",
                    delta_color="off")

        if nn and nn.get("beans"):
            import json as _json
            b = nn["beans"][0]
            rows = [{"datanode": k.split(":")[0], "state": "live", "used": v.get("usedSpace", 0),
                     "blocks": v.get("numBlocks")} for k, v in _json.loads(b.get("LiveNodes", "{}")).items()]
            rows += [{"datanode": k.split(":")[0], "state": "dead", "used": None, "blocks": None}
                     for k in _json.loads(b.get("DeadNodes", "{}"))]
            st.dataframe(pd.DataFrame(rows).sort_values("datanode"), hide_index=True, width="stretch",
                         column_config={"used": st.column_config.NumberColumn("used (bytes)", format="%d")})

        apps = get_json(f"{RESOURCEMANAGER}/ws/v1/cluster/apps?limit=200")
        if apps and apps.get("apps"):
            a = pd.DataFrame(apps["apps"]["app"])
            a = a.sort_values("startedTime", ascending=False).head(10)
            a["elapsed_s"] = (a.elapsedTime / 1000).round(1)
            st.markdown("**Recent YARN applications**")
            st.dataframe(a[["id", "name", "state", "finalStatus", "progress", "elapsed_s"]], hide_index=True,
                         width="stretch", column_config={
                             "progress": st.column_config.ProgressColumn(format="%.0f%%", min_value=0, max_value=100)})
        st.caption(f"NameNode UI {NAMENODE} · ResourceManager UI {RESOURCEMANAGER}")

    cluster_panel()
