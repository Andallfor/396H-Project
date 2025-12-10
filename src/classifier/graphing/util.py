import pandas as pd

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

# a ? a : b
def getOrDefault(a, b):
    if type(a)() == a:
        return b
    return a