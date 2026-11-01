#!/usr/bin/env python3
"""
Report figures from the pipeline's own output - no cluster needed.

  fig1_degradation.png   J4: torque rise above fresh vs time since maintenance
  fig2_time_split.png    J2: busy / blocked / starved share of each station's clock
  fig3_pm_intervals.png  policy A vs mined policy B maintenance intervals
  fig4_ab_results.png    closed-loop A/B: change in each metric with 95% CI
  fig5_oee.png           J1: availability, performance, quality per station

Usage:
  python sim/plots.py                 # -> artifacts/figures/
"""

import argparse
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import viz_data as V

INK, INK2, MUTED = "#0b0b0b", "#52514e", "#898781"
GRID, AXIS, SURFACE = "#e1e0d9", "#c3c2b7", "#fcfcfb"
SLOT = ["#2a78d6", "#eb6834", "#1baf7a"]

plt.rcParams.update({
    "font.family": ["Segoe UI", "DejaVu Sans", "sans-serif"],
    "font.size": 10,
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": AXIS, "axes.labelcolor": INK2, "axes.titlecolor": INK,
    "axes.titlesize": 12, "axes.titleweight": "semibold", "axes.titlelocation": "left",
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "axes.axisbelow": True, "grid.color": GRID, "grid.linewidth": 0.6,
    "xtick.color": MUTED, "ytick.color": MUTED, "xtick.labelcolor": INK2, "ytick.labelcolor": INK2,
    "legend.frameon": False, "legend.labelcolor": INK2,
})


def fig_degradation(out):
    d = V.degradation_points(fitted_only=True)
    st = V.station_table().set_index("station")
    fig, ax = plt.subplots(figsize=(8, 4.6))
    ends = []
    for sid in V.STATIONS:
        s = d[d.station == sid]
        hero = sid == "S04"
        ax.plot(s.age_h, s.rise_pct, color=V.STATION_COLORS[sid], lw=2.4 if hero else 1.6,
                alpha=1 if hero else 0.85, marker="o", ms=3.5, label=f"{sid} {V.STATION_NAMES[sid]}")
        ends.append((s.iloc[-1].rise_pct, s.iloc[-1].age_h, sid))
    # End labels, nudged apart vertically so close lines stay readable.
    placed = []
    for y, x, sid in sorted(ends):
        ly = max(y, placed[-1] + 1.4) if placed else y
        placed.append(ly)
        hero = sid == "S04"
        ax.annotate(f"{sid}  {st.loc[sid, 'slope_per_h']:.2f}/h", (x, y), xytext=(x + 0.25, ly),
                    va="center", fontsize=9, color=INK if hero else INK2,
                    fontweight="semibold" if hero else None)
    ax.axhline(15, color=AXIS, lw=1)
    ax.text(0.2, 15.6, "maintenance target: +15% above fresh", fontsize=8.5, color=INK2)
    ax.set_xlabel("time since last maintenance (h, wall clock)")
    ax.set_ylabel("mean torque above fresh (%)")
    rate = (st.slope_per_h / st.fresh_torque * 100)
    fast, slow = rate.idxmax(), rate.idxmin()
    ax.set_title(f"{fast} wears fastest: torque climbs {rate[fast]:.1f}% per hour, "
                 f"{rate[fast] / rate[slow]:.0f}x the slowest station ({slow})")
    ax.margins(x=0.12)
    ax.legend(ncol=3, fontsize=8.5, loc="upper left")
    fig.text(0.01, 0.005, "J4 degradation job, run=baseline (7 days, 1 Hz). Slope labels in Nm per hour; "
             "non-running samples excluded.", fontsize=8, color=MUTED)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(out, dpi=200)
    plt.close(fig)


def fig_time_split(out):
    t = V.time_split().pivot(index="station", columns="state", values="share").loc[V.STATIONS[::-1]]
    fig, ax = plt.subplots(figsize=(8, 3.8))
    left = [0.0] * len(t)
    for state, color in zip(("busy", "blocked", "starved"), SLOT):
        vals = t[state].values
        ax.barh(t.index, vals, left=left, color=color, height=0.62, label=state,
                edgecolor=SURFACE, linewidth=2)
        for i, (l, v) in enumerate(zip(left, vals)):
            if v > 0.09:
                ax.text(l + v / 2, i, f"{v:.0%}", ha="center", va="center", fontsize=8.5,
                        color="#ffffff")
        left = [l + v for l, v in zip(left, vals)]
    ax.set_xlim(0, 1)
    ax.xaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0))
    ax.grid(axis="y", visible=False)
    ax.set_yticks(range(len(t)), [f"{s}  {V.STATION_NAMES[s]}" for s in t.index])
    ax.get_yticklabels()[list(t.index).index("S03")].set_fontweight("bold")
    ax.set_title("S03 is the constraint: busy 96% of the time, so everyone waits on it")
    ax.legend(ncol=3, loc="upper center", bbox_to_anchor=(0.5, -0.1), fontsize=9)
    fig.text(0.01, 0.01, "J2 bottleneck job (secondary sort). Blocked = output buffer full; "
             "starved = input buffer empty.", fontsize=8, color=MUTED)
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    fig.savefig(out, dpi=200)
    plt.close(fig)


def fig_pm(out):
    a = V.load_json("policy_A.json"); b = V.load_json("artifacts", "policy_B.json")
    fig, ax = plt.subplots(figsize=(8, 3.8))
    ys = list(range(len(V.STATIONS)))[::-1]
    for y, sid in zip(ys, V.STATIONS):
        va = a["stations"][sid]["pm_interval_cycles"]; vb = b["stations"][sid]["pm_interval_cycles"]
        ax.plot([va, vb], [y, y], color=AXIS, lw=2, zorder=1)
        ax.scatter([va], [y], s=60, color=V.POLICY_COLORS["A · baseline"], zorder=2,
                   edgecolor=SURFACE, linewidth=2, label="A · baseline" if y == ys[0] else None)
        ax.scatter([vb], [y], s=70, color=V.POLICY_COLORS["B · mined"], zorder=3,
                   edgecolor=SURFACE, linewidth=2, label="B · mined" if y == ys[0] else None)
        ax.annotate(f"{vb:,}", (vb, y), xytext=(0, 9), textcoords="offset points",
                    ha="center", fontsize=8.5, color=INK2)
    ax.set_yticks(ys, [f"{s}  {V.STATION_NAMES[s]}" for s in V.STATIONS])
    ax.set_xscale("log")
    ax.set_xticks([1000, 2000, 3000, 5000, 10000], ["1,000", "2,000", "3,000", "5,000", "10,000"])
    ax.minorticks_off()
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("preventive-maintenance interval (cycles, log scale)")
    ax.set_title("Mined PM intervals span 8x; the baseline set every station to 3,000")
    ax.legend(loc="lower right", fontsize=9)
    ax.margins(y=0.12)
    fig.tight_layout()
    fig.savefig(out, dpi=200)
    plt.close(fig)


def fig_ab(out):
    r = V.load_json("artifacts", "results.json")
    t = V.ab_table(r)
    fig, axes = plt.subplots(1, len(t), figsize=(9, 3.2))
    for ax, (_, m) in zip(axes, t.iterrows()):
        better = m.verdict == "B better"
        color = V.GOOD if better else V.CRITICAL
        ax.axvline(0, color=AXIS, lw=1)
        ax.errorbar([m.delta_pct], [0], xerr=[m.ci_pct], fmt="o", ms=8, color=color,
                    ecolor=color, elinewidth=2, capsize=5)
        span = max(abs(m.delta_pct) + m.ci_pct, 0.5) * 1.35
        ax.set_xlim(-span, span)
        ax.set_ylim(-1, 1)
        ax.set_yticks([])
        ax.grid(axis="y", visible=False)
        ax.spines["left"].set_visible(False)
        ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:+.0f}%" if abs(v) >= 1 else f"{v:+.1f}%"))
        mark = "✓" if better else "✗"
        word = "better" if better else "worse"
        ax.set_title(m.metric, fontsize=10.5)
        ax.text(0.5, 0.86, f"{mark} {m.delta_pct:+.2f}%  ({word})", transform=ax.transAxes,
                ha="center", fontsize=10, color=INK, fontweight="semibold")
        fmt = "{:,.0f}" if abs(m.A) >= 100 else "{:.3f}"
        ax.text(0.5, 0.12, f"A {fmt.format(m.A)}  ->  B {fmt.format(m.B)}", transform=ax.transAxes,
                ha="center", fontsize=8.5, color=INK2)
    fig.suptitle("Closed loop: mined policy B vs static baseline A", x=0.01, ha="left",
                 fontsize=12, fontweight="semibold", color=INK)
    fig.text(0.01, 0.01, f"{r['seeds']} paired seeds, {r['days']} days, scored on held-out days "
             f"{r['split_day'] + 1}-{r['days']}. Bars are 95% CI on the paired difference.",
             fontsize=8, color=MUTED)
    fig.tight_layout(rect=(0, 0.05, 1, 0.93))
    fig.savefig(out, dpi=200)
    plt.close(fig)


def fig_oee(out):
    st = V.station_table()
    fig, ax = plt.subplots(figsize=(8, 3.8))
    w = 0.26
    xs = range(len(st))
    for k, (col, color) in enumerate(zip(("availability", "performance", "quality"), SLOT)):
        ax.bar([x + (k - 1) * w for x in xs], st[col], width=w - 0.03, color=color, label=col)
    for x, (_, row) in zip(xs, st.iterrows()):
        ax.text(x, 1.003, f"OEE {row.oee:.3f}", ha="center", fontsize=8.5,
                color=INK if row.station == "S04" else INK2,
                fontweight="semibold" if row.station == "S04" else None)
    ax.set_ylim(0.8, 1.02)
    ax.set_xticks(list(xs), [f"{s}\n{V.STATION_NAMES[s]}" for s in st.station])
    ax.grid(axis="x", visible=False)
    ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0))
    ax.set_title("OEE = availability x performance x quality; S04 is lowest on all three")
    ax.legend(ncol=3, loc="lower center", bbox_to_anchor=(0.5, -0.38), fontsize=9)
    fig.text(0.01, 0.01, "J1 OEE job. Y axis starts at 80% to separate close values.",
             fontsize=8, color=MUTED)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(out, dpi=200)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=V.path("artifacts", "figures"))
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    for name, fn in (("fig1_degradation.png", fig_degradation),
                     ("fig2_time_split.png", fig_time_split),
                     ("fig3_pm_intervals.png", fig_pm),
                     ("fig4_ab_results.png", fig_ab),
                     ("fig5_oee.png", fig_oee)):
        p = os.path.join(args.out, name)
        fn(p)
        print(f"-> {p}")


if __name__ == "__main__":
    main()
