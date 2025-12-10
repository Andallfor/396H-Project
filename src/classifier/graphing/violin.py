from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import math
from util import parse_bar_specs, read_concat_df, read_concat_series, getOrDefault

subreddits = [
    # control
    "r/MadeMeSmile:MadeMeSmile_r_subreddit_per_text.csv",
    "r/explainlikeimfive:explainlikeimfive_r_subreddit_per_text.csv",
    "r/aww:aww_r_subreddit_per_text.csv",
    "r/Damnthatsinteresting:Damnthatsinteresting_r_subreddit_per_text.csv",
    "r/HobbyDrama:HobbyDrama_r_subreddit_per_text.csv",
    ":",

    # drama
    "r/IAmA:IAmA_r_subreddit_per_text.csv",
    "r/AITAH:AITAH_r_subreddit_per_text.csv",
    "r/offmychest:offmychest_r_subreddit_per_text.csv",
    "r/changemyview:changemyview_r_subreddit_per_text.csv",
    "r/unpopularopinion:unpopularopinion_r_subreddit_per_text.csv",
    ":",

    # politics
    "r/politics:politics_r_subreddit_per_text.csv",
    "r/Conservative:Conservative_r_subreddit_per_text.csv",
    "r/democrats:democrats_r_subreddit_per_text.csv",
    "r/TheNewRight:TheNewRight_r_subreddit_per_text.csv",
    "r/dsa:dsa_r_subreddit_per_text.csv",
    ":",

    # controversial
    "r/The_Donald:The_Donald_r_subreddit_per_text.csv",
    "r/ChapoTrapHouse:ChapoTrapHouse_r_subreddit_per_text.csv",
    "r/MGTOW:MGTOW_r_subreddit_per_text.csv",
    "r/antiwork:antiwork_r_subreddit_per_text.csv",
    "r/KotakuInAction:KotakuInAction_r_subreddit_per_text.csv",
]

grouped = [
    "Control:MadeMeSmile_r_subreddit_per_text.csv,explainlikeimfive_r_subreddit_per_text.csv,aww_r_subreddit_per_text.csv,Damnthatsinteresting_r_subreddit_per_text.csv,HobbyDrama_r_subreddit_per_text.csv",
    "Drama:IAmA_r_subreddit_per_text.csv,AITAH_r_subreddit_per_text.csv,offmychest_r_subreddit_per_text.csv,changemyview_r_subreddit_per_text.csv,unpopularopinion_r_subreddit_per_text.csv",
    "Politics:politics_r_subreddit_per_text.csv,Conservative_r_subreddit_per_text.csv,democrats_r_subreddit_per_text.csv,TheNewRight_r_subreddit_per_text.csv,dsa_r_subreddit_per_text.csv",
    "Controversial:The_Donald_r_subreddit_per_text.csv,ChapoTrapHouse_r_subreddit_per_text.csv,MGTOW_r_subreddit_per_text.csv,antiwork_r_subreddit_per_text.csv,KotakuInAction_r_subreddit_per_text.csv"
]

plt.rcParams["font.family"] = "serif"
plt.rcParams["font.serif"] = ["Times New Roman"]

script_dir = Path(__file__).resolve().parent
finished_dir = script_dir.parent / "csvs" / "finished"
fig, (ax0, ax1) = plt.subplots(1, 2, sharey=True, width_ratios=[4.5, 1])
fig.set_size_inches(11, 3.5)

ax0.set_ylabel("Authoritarian Score")
# fig.suptitle("Distribution of Authoritarian Score Across Selected Subreddits")

for i, (ax, inp) in enumerate(zip([ax0, ax1], [subreddits, grouped])):
    bar_specs = parse_bar_specs(inp)
    data = []
    labels = []

    # one violin per label, with each label allowing multiple CSVs
    positions = []
    for j, (label, files) in enumerate(bar_specs):
        if len(files) != 0:
            s = read_concat_series(finished_dir, files, "auth").dropna()
            data.append(s.to_numpy())
            positions.append(0.75 * j + 1)
        else: # hack to separate groups
            data.append([-1])
            positions.append(-1)
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
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylim(0.0, 1.0)
    ax.set_xlim(left = 0.5)

out_path = script_dir / "violin.png"
plt.tight_layout()
fig.savefig(out_path, dpi=300)
plt.close(fig)