"""
Merges with historical HQ data; 

This code can run in my own computer environment, but I have not worked to make it run in other machines

License: All rights reserved.
"""

import pandas as pd
from code_private.edgarform import get_funda_10k_df_linked, get_header_df

# 1. Load the historical HQ data and the FJC data (LiberalCourt, calculated by read_fjc_judges_csv.py)
header_df = get_header_df(years = range(1996, 2026), _edgar_root_dir="/mnt/text/edgar/")

FJC_OUTFILE = "data/liberalcourt_pnld_202603.csv"
circuit_month = pd.read_csv(FJC_OUTFILE, sep="|")

circuit_month["datadate"] = pd.to_datetime(circuit_month["datadate"])

# 2. Create the Year-Month joining keys
# We create a Period objects (Year-Month) to ensure the join ignores specific days
header_df['join_month'] = header_df['Conformed_Period_of_Report'].dt.to_period('M')
circuit_month['join_month'] = circuit_month['datadate'].dt.to_period('M')

# 3. Perform the merge
# We join on State (BUSIADDR_STATE <-> statecode) and the Year-Month key
merged_df = pd.merge(
    header_df[["CIK", "CompanyName", "FormType", "FileName", "Conformed_Period_of_Report", "BUSIADDR_STATE", "join_month"]], 
    circuit_month[["datadate", "CircuitNo", "statecode", "pctd", "pctda", "pnld", "join_month"]],
    left_on=['BUSIADDR_STATE', 'join_month'],
    right_on=['statecode', 'join_month'],
    how='inner'  # Use 'inner' if you only want rows that match in both
)

merged_df = merged_df.loc[merged_df.datadate.dt.year>=1996].copy()

merged_df.to_csv("data/merged_pnld_hist_hq_202603.csv", sep="|", index=False)


merged_df.groupby(["CircuitNo", merged_df.datadate.dt.year]).pnld.describe()