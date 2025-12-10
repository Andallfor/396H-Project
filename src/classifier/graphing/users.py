from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import math
import matplotlib.units as munits
import datetime
from util import parse_bar_specs, read_concat_df, read_concat_series, getOrDefault

data = [
    f"Users:2024-09_r_users_per_text.csv,2024-10_r_users_per_text.csv,2024-11_r_users_per_text.csv,2024-12_r_users_per_text.csv,2025-01_r_users_per_text.csv,2025-02_r_users_per_text.csv,2025-03_r_users_per_text.csv,2025-04_r_users_per_text.csv,2025-05_r_users_per_text.csv,2025-06_r_users_per_text.csv,2025-07_r_users_per_text.csv,2025-08_r_users_per_text.csv,2025-09_r_users_per_text.csv",
    f"Moderators:2024-09_r_mods_per_text.csv,2024-10_r_mods_per_text.csv,2024-11_r_mods_per_text.csv,2024-12_r_mods_per_text.csv,2025-01_r_mods_per_text.csv,2025-02_r_mods_per_text.csv,2025-03_r_mods_per_text.csv,2025-04_r_mods_per_text.csv,2025-05_r_mods_per_text.csv,2025-06_r_mods_per_text.csv,2025-07_r_mods_per_text.csv,2025-08_r_mods_per_text.csv,2025-09_r_mods_per_text.csv"
]

bar_specs = parse_bar_specs(data)

plt.rcParams["font.family"] = "serif"
plt.rcParams["font.serif"] = ["Times New Roman"]

script_dir = Path(__file__).resolve().parent
finished_dir = script_dir.parent / "csvs" / "finished"
fig, (ax0, ax1) = plt.subplots(1, 2, sharey=True, width_ratios=[4.5, 1], layout = 'constrained')
fig.set_size_inches(10, 3)

ax0.set_ylabel("Authoritarian Score")

ax = ax0
locator = mdates.AutoDateLocator()
locator.intervald[mdates.MONTHLY] = [1]
formatter = mdates.ConciseDateFormatter(locator)
ax.xaxis.set_major_locator(locator)
ax.xaxis.set_major_formatter(formatter)
if True:
    TIME_COL = "created_utc"

    # we use equal-width buckets, so change this as needed for more/less precise graphs,
    # similarly to bins for the histogram.
    BUCKETS = 50

    # only show shaded spread region if there's two or less lines, otherwise it gets cluttered
    show_spread = (len(bar_specs) <= 2)

    # keep track of global min and max times
    global_min = None
    global_max = None

    # each label gets a distinct line and allows multiple CSVs
    cnt = 0
    for label, files in bar_specs:
        df = read_concat_df(finished_dir, files, [TIME_COL, 'auth'], 'all')
        cnt += df.count()

        # timestamps are unix seconds
        t = df[TIME_COL].astype("int64")
        v = df['auth']

        # determine the time range from the data, then bucket that range evenly
        t_min = int(t.min())
        t_max = int(t.max())

        den = (t_max - t_min)
        idx = ((t - t_min) * BUCKETS // den).astype("int64")
        idx = idx.clip(lower=0, upper=BUCKETS - 1)

        # compute mean times and value stats for each bucket
        t_mean = t.groupby(idx).mean()
        g = v.groupby(idx)
        v_mean = g.mean()
        v_std = g.std().fillna(0.0)

        # convert unix seconds to actual dates/times
        x_dt = pd.to_datetime(t_mean.values, unit="s", utc=True)

        ax.plot(x_dt, v_mean.values, label=label)

        # add shaded in spread regions if only one line
        if show_spread:
            lo = (v_mean - v_std).to_numpy()
            hi = (v_mean + v_std).to_numpy()
            ax.fill_between(x_dt, lo, hi, alpha=0.25)

        # update global min and max times
        cur_min = x_dt.min()
        cur_max = x_dt.max()
        global_min = cur_min if global_min is None else min(global_min, cur_min)
        global_max = cur_max if global_max is None else max(global_max, cur_max)


    ax.set_ylim(0.0, 1.0)
    ax.margins(0.025, 0, tight = False)

    handles, labels = ax.get_legend_handles_labels()

    max_rows = 8 # max rows of the legend for large line graphs, change as needed
    ncol = max(1, math.ceil(len(labels) / max_rows))

    ax.legend(handles, labels, ncol=ncol, fontsize=10)
    print(cnt)

ax = ax1
if True:
    bar_specs = parse_bar_specs(data)
    data = []
    labels = []

    # one violin per label, with each label allowing multiple CSVs
    positions = []
    for j, (label, files) in enumerate(bar_specs):
        s = read_concat_series(finished_dir, files, "auth").dropna()
        data.append(s.to_numpy())
        positions.append(0.6 * j + 1)
        labels.append(label)

    # Violin: distribution only (no mean/median/extrema lines)
    vp = ax.violinplot(
        data,
        positions=positions,
        showmeans=False,
        showmedians=False,
        showextrema=False,
    )

    # Make violins a bit less "bare" visually
    for body in vp["bodies"]:
        body.set_alpha(0.35)

    # boxplot inside of the violin, change settings as needed
    bp = ax.boxplot(
        data,
        positions=positions,
        widths=0.12,
        patch_artist=False,
        showmeans=True,
        meanline=False,
        showfliers=True,

        # turn alpha level of the box plot down
        boxprops=dict(linewidth=1.0, alpha=0.5),
        whiskerprops=dict(linewidth=1.0, alpha=0.5),
        capprops=dict(linewidth=1.0, alpha=0.5),
        medianprops=dict(color="black", linewidth=1.4, alpha=0.5),
        
        meanprops=dict(
            marker="o",
            markersize=3,
            alpha=0.5,
            markerfacecolor="black",
            markeredgecolor="black",
            markeredgewidth=0.6,
        ),

        # outliers are just small black dots
        flierprops=dict(
            marker=".",
            markersize=2.2,
            alpha=0.25,
            markerfacecolor="black",
            markeredgecolor="black",
        )
    )

    ax.set_xticks(positions)
    ax.set_xticklabels(labels)
    ax.set_ylim(0.0, 1.0)
    # ax.set_xlim(left = 0.75, right)
    ax.margins(-0.1, 0)

out_path = script_dir / "users.png"
# plt.tight_layout()
fig.savefig(out_path, dpi=300)
plt.close(fig)