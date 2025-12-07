#!/usr/bin/env python3

# Create bar chart, allows combining of multiple CSVs.
#
# Note all CSVs should be in ../csvs/finished
#
# Bar graph example:
# > python create_bar.py \
#   --bar "r_politics: politics_r_subreddit_per_text.csv" \
#   --bar "moderators: 2025-07_r_mods_per_text.csv, 2025-06_r_mods_per_text.csv"
#
# Histogram example:
# > python create_bar.py \
#   --graph hist \
#   --bar "moderators: 2025-07_r_mods_per_text.csv, 2025-06_r_mods_per_text.csv"
#
# Violin plot example:
# > python create_bar.py \
#   --graph violin \
#   --bar "r_politics: politics_r_subreddit_per_text.csv" \
#   --bar "mods: 2025-06_r_mods_per_text.csv, 2025-07_r_mods_per_text.csv"
#
# Line graph example:
# > python create_bar.py \
#   --graph line \
#   --bar "r_politics: politics_r_subreddit_per_text.csv" \
#   --bar "moderators: 2025-07_r_mods_per_text.csv, 2025-06_r_mods_per_text.csv"
#
# Scatter plot example:
# > python create_bar.py \
#   --graph scatter \
#   --x-col score \
#   --bar "moderators: 2025-07_r_mods_per_text.csv, 2025-06_r_mods_per_text.csv"

import argparse
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


# each spec is 'label:csv1,csv2,...'
# returns list of (label, [csv1, csv2, ...])
def parse_bar_specs(bar_specs):
    parsed = []
    for spec in bar_specs:
        label, files_str = spec.split(":", 1)
        files = [f.strip() for f in files_str.split(",") if f.strip()]
        parsed.append((label.strip(), files))
    return parsed

DIST_COL = "distinguished"

def read_concat_df(base_dir, files, usecols, dist="all"):
    # only read distinguished when needed
    if dist != "all" and DIST_COL not in usecols:
        usecols = list(usecols) + [DIST_COL]

    df_list = []
    for fname in files:
        path = base_dir / fname
        df_list.append(pd.read_csv(path, usecols=usecols))

    df = pd.concat(df_list, ignore_index=True)

    # simply return if we don't need to filter for distinguished
    if dist == "all":
        return df

    d = pd.to_numeric(df[DIST_COL], errors="coerce")
    if dist == "users":
        return df.loc[d == 1]
    else:  # elevated_users
        return df.loc[d > 1]

def read_concat_series(base_dir, files, col, dist="all"):
    if dist == "all":
        series_list = []
        for fname in files:
            path = base_dir / fname
            series_list.append(pd.read_csv(path, usecols=[col])[col])
        return pd.concat(series_list, ignore_index=True)
    
    # read df (col + distinguished), filter, then take the column
    else:
        df = read_concat_df(base_dir, files, [col], dist=dist)
        return df[col]

# command line arg handling
ap = argparse.ArgumentParser()
ap.add_argument(
    "--graph",
    default="bar",
    choices=["bar", "hist", "line", "scatter", "violin"],
    help="graph type (default: bar)",
)
ap.add_argument(
    "--bar",
    action="append",
    required=True,
    help="each bar is defined this way: 'label:csv1,csv2,...' (files in ../csvs/finished)",
)
ap.add_argument(
    "--value-col",
    default="auth",
    help="numeric column to aggregate / plot (default: 'auth')",
)
ap.add_argument(
    "--x-col",
    choices=["score", "num_sentences"],
    help="x-axis column for scatter (required if --graph scatter).",
)
ap.add_argument(
    "--agg",
    default="mean",
    choices=["mean", "median", "sum"],
    help="what type of aggregation we do on value-col (bar only)",
)
ap.add_argument(
    "--y-min",
    type=float,
    default=0.0,
    help="lower y-axis limit (bar only; default: 0.0 for auth scores)",
)
ap.add_argument(
    "--y-max",
    type=float,
    default=1.0,
    help="upper y-axis limit (bar only; default: 1.0 for auth scores)",
)
ap.add_argument(
    "--dist",
    default="all",
    choices=["all", "users", "elevated_users"],
    help="filter by distinguished column (default: all)",
)
args = ap.parse_args()

# get current directory and the csv directory, all CSVs should be in ../csvs/finished
script_dir = Path(__file__).resolve().parent
finished_dir = script_dir.parent / "csvs" / "finished"

bar_specs = parse_bar_specs(args.bar)

dist_tag = "" if args.dist == "all" else f"{args.dist}_"

# histogram only accepts one label with multiple CSVs allowed
if args.graph == "hist":

    if len(bar_specs) != 1:
        raise ValueError("histogram expects exactly one --bar")

    label, files = bar_specs[0]

    # load and concatenate the target value column across all CSVs for this label
    series = read_concat_series(finished_dir, files, args.value_col, args.dist)
    series = series.dropna()
    n = len(series)

    # create the matplotlib figure/axes for plotting
    fig, ax = plt.subplots()

    # number of bins we use, change as needed for more/less detail.
    # depending on what it's changed to you might need to change the ylim as well.
    bins = 50

    # density histogram instead of raw amounts
    densities, _, patches = ax.hist(
        series,
        bins=bins,
        range=(0.0, 1.0),
        density=True,
    )

    bin_width = 1.0 / bins

    # convert density heights to "percent of messages in bin"
    for d, p in zip(densities, patches):
        p.set_height(d * bin_width * 100.0)

    # set axes/labels/title/etc.
    ax.set_xlim(0.0, 1.0)
    ax.set_ylim(0.0, 15.0)   # y axis range, change as needed
    ax.set_xlabel(args.value_col)
    ax.set_ylabel(f"Percent of messages (N={n})")
    ax.set_title(f"Histogram of {args.value_col} ({label})")

    # output filename: hist_[<dist>_]<value-col>_<label>.png
    label_safe = label.replace(" ", "_")
    out_name = f"{args.graph}_{dist_tag}{args.value_col}_{label_safe}.png"

# violin plot of distributions
elif args.graph == "violin":
    data = []
    labels = []

    # one violin per label, with each label allowing multiple CSVs
    for label, files in bar_specs:
        s = read_concat_series(finished_dir, files, args.value_col, args.dist).dropna()
        data.append(s.to_numpy())
        labels.append(label)

    positions = list(range(1, len(data) + 1))

    fig, ax = plt.subplots()

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

    # configure axes/labels/title/etc.
    ax.set_ylabel(args.value_col)
    ax.set_xlabel("Group")
    ax.set_title(f"Violin plot of {args.value_col} across groups")
    ax.set_xticks(positions)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylim(0.0, 1.0)

    labels_safe = [label.replace(" ", "_") for label in labels]
    out_name = f"{args.graph}_{dist_tag}{args.value_col}_" + "_".join(labels_safe) + ".png"


# line graph for mean auth over time
elif args.graph == "line":
    TIME_COL = "created_utc"

    # we use equal-width buckets, so change this as needed for more/less precise graphs,
    # similarly to bins for the histogram.
    BUCKETS = 50

    # only show shaded spread region if there's only one line, otherwise it gets cluttered
    show_spread = (len(bar_specs) == 1)

    # create the matplotlib figure/axes for plotting
    fig, ax = plt.subplots()
    
    # keep track of global min and max times
    global_min = None
    global_max = None

    # each label gets a distinct line and allows multiple CSVs
    for label, files in bar_specs:
        df = read_concat_df(finished_dir, files, [TIME_COL, args.value_col], args.dist)

        # timestamps are unix seconds
        t = df[TIME_COL].astype("int64")
        v = df[args.value_col]

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


    # configure axes/labels/title/etc.
    ax.set_ylabel(f"mean of {args.value_col}")
    ax.set_title(f"mean {args.value_col} over time")
    ax.set_ylim(0.0, 1.0)
    ax.legend()

    # choose readable date ticks based on the full plotted time span
    if global_min is not None and global_max is not None:
        span_days = (global_max - global_min).days

        if span_days <= 60:
            ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))  # weekly
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        else:
            ax.xaxis.set_major_locator(mdates.MonthLocator())              # monthly
            if global_min.year == global_max.year:
                ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
            else:
                ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))

        fig.autofmt_xdate(rotation=45, ha="right")

        # update x label to show the time frame
        if global_min.year == global_max.year:
            ax.set_xlabel(f"Time (UTC) for {global_min.year}")
        else:
            ax.set_xlabel(f"Time (UTC) for {global_min.year} to {global_max.year}")

    # output filename: line_[<dist>_]<value-col>_<label1>_<label2>_...png
    labels_safe = [label.replace(" ", "_") for label, _ in bar_specs]
    out_name = f"{args.graph}_{dist_tag}{args.value_col}_" + "_".join(labels_safe) + ".png"

# scatter plot of auth vs (score or num_sentences)
elif args.graph == "scatter":

    # scatter plot needs an x axis value and only accepts one bar similar to histogram
    if not args.x_col:
        raise ValueError("--x-col is required when --graph scatter")
    elif len(bar_specs) != 1:
        raise ValueError("scatter plot expects exactly one --bar")

    # create the matplotlib figure/axes for plotting
    fig, ax = plt.subplots()

    label, files = bar_specs[0]

    # load and concatenate the target columns across all CSVs for this label
    df = read_concat_df(finished_dir, files, [args.x_col, args.value_col], args.dist)

    x_raw = df[args.x_col]
    y_raw = df[args.value_col]

    # get rid of NaNs
    mask = x_raw.notna() & y_raw.notna()
    x_raw = x_raw[mask]
    y = y_raw[mask].to_numpy()

    # percentile-scale x so equal widths have roughly equal point counts
    x = x_raw.rank(method="first", pct=True).to_numpy()  # in [0,1]

    # s = dot size, alpha = dot opacity, change as needed
    ax.scatter(x, y, s=4, alpha=0.5)

    # configure axes/labels/title/etc.
    x_name = args.x_col
    y_name = args.value_col

    ax.set_xlabel(f"{x_name} percentile rank")
    ax.set_ylabel(y_name)
    ax.set_title(f"{y_name} vs {x_name} ({label})")
    ax.set_xlim(0.0, 1.0)
    ax.set_ylim(0.0, 1.0)

    # tick positions are percentile ranks, tick labels show raw x_col quantiles.
    if args.x_col == "score":
        # score have a tail on both ends
        ticks = [0.0, 0.04, 0.07, 0.10, 0.25, 0.50, 0.75, 0.90, 0.93, 0.96, 1.0]
    else:  # num_sentences
        # num_sentences just has a tail on the right
        ticks = [0.0, 0.10, 0.25, 0.50, 0.75, 0.90, 0.93, 0.96, 1.0]

    qvals = x_raw.quantile(ticks).to_numpy()

    # get rid of duplicate tick labels, keeping the leftmost occurrence
    uniq_ticks = []
    uniq_labels = []
    seen = set()
    for t, q in zip(ticks, qvals):
        label_str = f"{q:g}"
        if label_str not in seen:
            seen.add(label_str)
            uniq_ticks.append(t)
            uniq_labels.append(label_str)

    ax.set_xticks(uniq_ticks)
    ax.set_xticklabels(uniq_labels, rotation=45, ha="center", fontsize=9)

    # output filename: scatter_[<dist>_]<x-col>_<value-col>_<label>.png
    label_safe = label.replace(" ", "_")
    out_name = f"{args.graph}_{dist_tag}{x_name}_{args.value_col}_{label_safe}.png"


# bar chart
else:
    labels = []
    values = []
    errs = []

    use_err = (args.value_col == "auth" and args.agg == "mean")

    # one bar per label, with each label allowing multiple CSVs
    for label, files in bar_specs:
        series = read_concat_series(finished_dir, files, args.value_col, args.dist).dropna()

        # bar height
        if args.agg == "mean":
            val = series.mean()
        elif args.agg == "median":
            val = series.median()
        else:
            val = series.sum()

        labels.append(label)
        values.append(val)

        # spread = std dev of sampled auth values
        if use_err:
            errs.append(series.std(ddof=1))

    # making the bar chart
    fig, ax = plt.subplots()
    if use_err:
        ax.bar(
            labels,
            values,
            yerr=errs,
            capsize=3,
            error_kw=dict(alpha=0.5, linewidth=1.0),
        )
    else:
        ax.bar(labels, values)
    ax.set_ylabel(f"{args.agg} of {args.value_col}")            # y-axis name, edit as needed
    ax.set_xlabel("Group")                                      # x-axis name, edit as needed
    ax.set_title(f"{args.agg} {args.value_col} across groups")  # graph title, edit as needed
    ax.set_ylim(args.y_min, args.y_max)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=45, ha="right")

    # output filename: bar_[<dist>_]<agg>_<value-col>_<label1>_<label2>_...png
    labels_safe = [label.replace(" ", "_") for label in labels]
    out_name = f"{args.graph}_{dist_tag}{args.agg}_{args.value_col}_" + "_".join(labels_safe) + ".png"


# save the figure to a PNG in the same folder as this script
out_path = script_dir / out_name
plt.tight_layout()
fig.savefig(out_path, dpi=300)
plt.close(fig)
print(f"Saved plot to {out_path}")