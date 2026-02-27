import re
import pandas as pd
from typing import List

# --------- helpers ---------
def normalize_key(x):
    """Normalize strings for matching keys."""
    if pd.isna(x):
        return ""
    s = str(x).replace("\u00A0", " ").strip()       # remove non-breaking spaces
    s = re.sub(r"\s+", " ", s)                      # collapse multiple spaces
    return s.upper()

def find_col(cols: List[str], preferred_exact: List[str], contains_any: List[str]):
    """Find column name in a DataFrame that matches preferred names."""
    lower = {c.lower(): c for c in cols}
    for name in preferred_exact:
        if name.lower() in lower:
            return lower[name.lower()]
    for c in cols:
        cl = c.lower()
        if any(pat.lower() in cl for pat in contains_any):
            return c
    raise KeyError(f"Could not find matching column in {cols}")

# --------- load data ---------
mrp_df = pd.read_csv("MRP_2025-08-14.csv")
td = pd.read_excel("target_days.xlsx")

# --------- identify columns in target_days ---------
item_col  = find_col(td.columns, ["ItemCode", "Item Code", "Item"], ["item"])
site_col  = find_col(td.columns, ["SiteCode", "Site Code", "Site"], ["site", "warehouse", "location"])
days_col  = find_col(td.columns, ["# of Days", "Days", "Target Days"], ["days"])

# normalize keys in both files
mrp_df["_ITEM_KEY"] = mrp_df["ItemCode"].map(normalize_key)
mrp_df["_SITE_KEY"] = mrp_df["SiteCode"].map(normalize_key)

td["_ITEM_KEY"] = td[item_col].map(normalize_key)
td["_SITE_KEY"] = td[site_col].map(normalize_key)

# reduce target_days to unique keys
td_reduced = (
    td[["_ITEM_KEY", "_SITE_KEY", days_col]]
    .dropna(subset=[days_col])
    .drop_duplicates(subset=["_ITEM_KEY", "_SITE_KEY"], keep="first")
)

# create lookup dict
target_map = td_reduced.set_index(["_ITEM_KEY", "_SITE_KEY"])[days_col].to_dict()

# month columns are everything after "Step"
month_columns = list(mrp_df.columns[mrp_df.columns.get_loc("Step") + 1:])

# --------- build final DataFrame ---------
final_parts = []
fallback_used = []  # collect missing matches

for (item_key, site_key), group in mrp_df.groupby(["_ITEM_KEY", "_SITE_KEY"], sort=False):
    final_parts.append(group)
    
    if (item_key, site_key) in target_map:
        days_value = target_map[(item_key, site_key)]
    else:
        days_value = 75
        fallback_used.append((group.iloc[0]["ItemCode"], group.iloc[0]["SiteCode"]))
    
    tdoc_row = {col: pd.NA for col in mrp_df.columns}
    head = group.iloc[0]
    tdoc_row["ProductCategory"] = head["ProductCategory"]
    tdoc_row["ItemCode"] = head["ItemCode"]
    tdoc_row["SiteCode"] = head["SiteCode"]
    tdoc_row["Step"] = "9 TDOC"
    for mc in month_columns:
        tdoc_row[mc] = days_value
    
    final_parts.append(pd.DataFrame([tdoc_row], columns=mrp_df.columns))

final_df = pd.concat(final_parts, ignore_index=True)

# remove helper columns
final_df = final_df.drop(columns=["_ITEM_KEY", "_SITE_KEY"])

# save file
final_df.to_csv("MRP with Target Days.csv", index=False)

# --------- reporting ---------
print(f"✅ File saved as 'MRP with Target Days.csv'")
if fallback_used:
    print(f"⚠️ {len(fallback_used)} combinations used fallback 75 because no match was found in target_days.xlsx:")
    for item, site in fallback_used:
        print(f"  - ItemCode: {item}, SiteCode: {site}")
else:
    print("🎉 All combinations matched target_days.xlsx without fallback.")
