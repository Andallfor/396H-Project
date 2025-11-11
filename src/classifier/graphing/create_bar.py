#!/usr/bin/env python3

# Create bar chart, allows combining of multiple CSVs.
#
# Example usage:
# > python create_bar.py \
#   --value-col "auth" \
#   --bar "r_politics: r_politics_per_text.csv" \
#   --bar "moderators: 2025-07_r_mods_per_text.csv, 2025-06_r_mods_per_text.csv"
#
# The result of the above command is creating a bar graph of the mean auth values with
# two bars, "r_poltics" and "moderators", whose values are aggregated from the CSV files
# defined above.

import argparse
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

# each spec is 'label:csv1,csv2,...'
# returns list of (label, [csv1, csv2, ...])
def parse_bar_specs(bar_specs):
    parsed = []
    for spec in bar_specs:
        label, files_str = spec.split(":", 1)
        files = [f.strip() for f in files_str.split(",") if f.strip()]
        parsed.append((label.strip(), files))
    return parsed

# command line arg handling
ap = argparse.ArgumentParser()
ap.add_argument(
    "--bar",
    action="append",
    required=True,
    help="each bar is defined this way: 'label:csv1,csv2,...' (files in ../csvs/finished)",
)
ap.add_argument(
    "--value-col",
    default="auth",
    help="numeric column to aggregate (default: 'auth')",
)
ap.add_argument(
    "--agg",
    default="mean",
    choices=["mean", "median", "sum"],
    help="what type of aggregation we do on value-col",
)
ap.add_argument(
    "--y-min",
    type=float,
    default=0.0,
    help="lower y-axis limit (default: 0.0 for auth scores)",
)
ap.add_argument(
    "--y-max",
    type=float,
    default=1.0,
    help="upper y-axis limit (default: 1.0 for auth scores)",
)
args = ap.parse_args()

script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
finished_dir = project_root / "csvs" / "finished"

bar_specs = parse_bar_specs(args.bar)

labels = []
values = []

for label, files in bar_specs:
    dfs = []
    for fname in files:
        path = finished_dir / fname
        df = pd.read_csv(path, usecols=[args.value_col])
        dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)
    series = combined[args.value_col]

    if args.agg == "mean":
        val = series.mean()
    elif args.agg == "median":
        val = series.median()
    else:
        val = series.sum()

    labels.append(label)
    values.append(val)

# making the bar chart
fig, ax = plt.subplots()
ax.bar(labels, values)
ax.set_ylabel(f"{args.agg} of {args.value_col}")            # y-axis name, edit as needed
ax.set_xlabel("Group")                                      # x-axis name, edit as needed
ax.set_title(f"{args.agg} {args.value_col} across groups")  # graph title, edit as needed
ax.set_ylim(args.y_min, args.y_max)
plt.tight_layout()

# output filename: value-col_label1_label2_...png
labels_safe = [label.replace(" ", "_") for label in labels]
out_name = f"{args.value_col}_" + "_".join(labels_safe) + ".png"
out_path = script_dir / out_name

plt.savefig(out_path, dpi=300)
print(f"Saved plot to {out_path}")