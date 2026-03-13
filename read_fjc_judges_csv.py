#!/usr/bin/env python3
"""
read_fjc_judges_csv.py
Author: Reeyarn Li <reeyarn@gmail.com> | Date: 2026-03-13

REPLICATION SPECIFICATION
-------------------------
Operationalizes the "LiberalCourt" (pnld) measure as defined in:
Huang, A., Hui, K. W., & Li, R. Z. (2019). Federal judge ideology: A new measure 
of ex ante litigation risk. Journal of Accounting Research, 57(2), 431-489.

CORE METHODOLOGY:
  1. DATA SOURCE: Federal Judicial Center (FJC.gov) biographical 'judges.csv'.
  2. UNIT OF OBS: Circuit-State-Month panel.
  3. IDEOLOGY: Proxied by Appointing President (Democrat=Liberal, Republican=Conservative).
  4. MEASURE (pnld): Probability of a 3-judge panel having ≥2 Democratic appointees,
     calculated via hypergeometric distribution (sampling without replacement).

OUTPUT DATA DICTIONARY (liberalcourt_pnld_YYYYMM.csv):
  - datadate:  Month-end date (YYYY-MM-DD).
  - CircuitNo: Circuit identifier (C1-C11, DC).
  - statecode: Two-letter US state abbreviation.
  - pctd:      Share of ALL judges (active + senior) appointed by Democrats.
  - pctda:     Share of ACTIVE (non-senior) judges appointed by Democrats.
  - pnld:      LiberalCourt measure (Probability of Democratic panel majority).

MIT License
-----------
Copyright (c) 2026 Reeyarn Li

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import sys
import pathlib
from io import StringIO

import requests
import pandas as pd

FJC_URL     = "https://www.fjc.gov/sites/default/files/history/judges.csv"
LOCAL_FILE  = pathlib.Path("data/judges.csv")
from datetime import datetime

# Get the current year
current_year = datetime.now().year
current_month = datetime.now().month

# Dynamically create the filename

import os
os.makedirs("data", exist_ok=True)
FJC_OUTFILE = f"data/liberalcourt_pnld_{current_year}{current_month:02d}.csv"


# ── 1. Load ────────────────────────────────────────────────────────────────────
def load_data(url: str, local: pathlib.Path) -> pd.DataFrame:
    """
    Load the data from the URL or local file.
    If local file exists and is no older than 30 days, use it.
    If older than 30 days or missing, download from URL and save locally.
    If download fails and a local file exists (even stale), fall back to it.
    If download fails and no local file, exit with error.
    """
    import time
    if local.exists() and local.stat().st_mtime > time.time() - 30 * 24 * 60 * 60:
        print(f"Using local file (fresh): {local.resolve()}")
        return pd.read_csv(local)
   
    # Need to download (missing or stale)
    reason = "does not exist" if not local.exists() else "is older than 30 days"
    print(f"Local file {reason}, downloading from {url} ...")
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        local.write_text(r.text, encoding="utf-8")
        print(f"Download succeeded, saved to {local.resolve()}")
        return pd.read_csv(StringIO(r.text))
    except Exception as e:
        print(f"Download failed: {e}")
        if local.exists():
            print(f"Falling back to stale local file: {local.resolve()}")
            return pd.read_csv(local)
        sys.exit(f"Error: no local file available. Exiting.")

df = load_data(FJC_URL, LOCAL_FILE)

# ── 2. True judge-level scalar columns ────────────────────────────────────────
judge_cols = [
    "jid", "Last Name", "First Name", "Middle Name", "Suffix",
    "Birth Year", "Gender", "Race or Ethnicity",
    "Professional Career", "Other Nominations/Recess Appointments",
]

# ── 3. Reshape appointment block (6 slots × 27 fields) ────────────────────────
appt_fields = [
    "Court Type", "Court Name", "Appointment Title",
    "Appointing President", "Party of Appointing President",
    "Reappointing President", "Party of Reappointing President",
    "ABA Rating", "Seat ID", "Statute Authorizing New Seat",
    "Recess Appointment Date", "Nomination Date", "Committee Referral Date",
    "Hearing Date", "Judiciary Committee Action", "Committee Action Date",
    "Senate Vote Type", "Ayes/Nays", "Confirmation Date", "Commission Date",
    "Service as Chief Judge, Begin", "Service as Chief Judge, End",
    "2nd Service as Chief Judge, Begin", "2nd Service as Chief Judge, End",
    "Senior Status Date", "Termination", "Termination Date",
]

df = df.rename(columns={f"{f} ({n})": f"{f}{n}"
                         for n in range(1, 7) for f in appt_fields})

appt_long = pd.wide_to_long(
    df, stubnames=appt_fields, i="jid", j="appointment_num",
    sep="", suffix=r"\d+",
).reset_index()

appt_long = appt_long.dropna(subset=["Court Name"])

# ── 4. Reshape education block (5 slots × 3 fields) ───────────────────────────
edu_fields = ["School", "Degree", "Degree Year"]

edu_wide = df[["jid"] + [f"{f} ({n})" for n in range(1, 6) for f in edu_fields]].copy()
edu_wide = edu_wide.rename(columns={f"{f} ({n})": f"{f}{n}"
                                     for n in range(1, 6) for f in edu_fields})

edu_long = pd.wide_to_long(
    edu_wide, stubnames=edu_fields, i="jid", j="edu_num",
    sep="", suffix=r"\d+",
).reset_index()

edu_long = edu_long.dropna(subset=["School"])

# Collapse education rows into a single string per judge: "School|Degree|Year; ..."
edu_summary = (
    edu_long
    .assign(edu_entry=lambda x: x["School"] + " | " + x["Degree"].fillna("") +
                                " | " + pd.to_numeric(x["Degree Year"], errors="coerce")
                                           .astype("Int64").astype(str).replace("<NA>", ""))
    .groupby("jid")["edu_entry"]
    .apply("; ".join)
    .reset_index()
    .rename(columns={"edu_entry": "Education"})
)

# ── 5. Collapse other federal service (4 slots) into a single string per judge ─
svc_cols = [f"Other Federal Judicial Service ({n})" for n in range(1, 5)]
svc_summary = (
    df[["jid"] + svc_cols]
    .set_index("jid")
    .stack()
    .reset_index(level=1, drop=True)
    .rename("Other Federal Judicial Service")
    .reset_index()
    .groupby("jid")["Other Federal Judicial Service"]
    .apply("; ".join)
    .reset_index()
)

# ── 6. Merge everything onto the appointment-level frame ──────────────────────
out = (
    appt_long[judge_cols + ["appointment_num"] + appt_fields]
    .merge(edu_summary,  on="jid", how="left")
    .merge(svc_summary,  on="jid", how="left")
    .sort_values(["jid", "appointment_num"])
    .reset_index(drop=True)
)

# ── Write appointment-level data ──────────────────────────────────────────────
out.to_csv(FJC_OUTFILE, sep="|", index=False)
print(f"Appointment-level data. {len(out)} judge-appointment rows -> {FJC_OUTFILE}")


# ── 7. Parse dates and derive key fields for panel composition ────────────────
from math import comb
 
date_cols = ["Commission Date", "Recess Appointment Date", "Senior Status Date",
             "Termination Date"]
for col in date_cols:
    out[col] = pd.to_datetime(out[col], errors="coerce")
 
# Party: D / R / na  (from appointing president's party)
def parse_party(s):
    if pd.isna(s): return "na"
    s = str(s)
    if "dem" in s.lower(): return "D"
    if "rep" in s.lower(): return "R"
    return "na"
 
out["Party"] = out["Party of Appointing President"].map(parse_party)
 
# Fill "na" party forward within judge (carry prior appointment's party)
out = out.sort_values(["jid", "appointment_num"]).reset_index(drop=True)
out["Party"] = out.groupby("jid")["Party"].transform(
    lambda s: s.replace("na", pd.NA).ffill().fillna("na")
)
 
# jdate1: Commission Date, overridden by Recess Appointment Date if earlier
out["jdate1"] = out["Commission Date"]
mask = (
    out["Recess Appointment Date"].notna() &
    (out["Recess Appointment Date"] < out["Commission Date"].fillna(pd.Timestamp.max))
)
out.loc[mask, "jdate1"] = out.loc[mask, "Recess Appointment Date"]
 
# jdate2: Senior Status Date, overridden by Termination Date;
#         if neither, assume still serving
out["jdate2"] = out["Senior Status Date"]
out.loc[out["Termination Date"].notna(), "jdate2"] = out.loc[out["Termination Date"].notna(), "Termination Date"]
still_serving = out["Termination Date"].isna() & out["Termination"].fillna("").str.strip().str.len().lt(2)
out.loc[still_serving, "jdate2"] = pd.Timestamp("2199-12-31")
 
# Circuit identifier from court name
def circuit_from_name(name):
    if pd.isna(name): return "na"
    n = name.lower()
    if "federal"  in n: return "CF"
    if "first"    in n: return "C1"
    if "second"   in n: return "C2"
    if "third"    in n: return "C3"
    if "fourth"   in n: return "C4"
    if "fifth"    in n: return "C5"
    if "sixth"    in n: return "C6"
    if "seventh"  in n: return "C7"
    if "eighth"   in n: return "C8"
    if "ninth"    in n: return "C9"
    if "tenth"    in n: return "C10"
    if "eleventh" in n: return "C11"
    if "columbia" in n: return "DC"
    return "na"
 
out["CircuitNo"] = out["Court Name"].map(circuit_from_name)
 
# Keep only U.S. Court of Appeals rows for panel composition calc
appeals = out[out["Court Type"].str.startswith("U.S. Court of Appeals", na=False)].copy()
 
# ── 8. Build monthly date spine and compute pctd / pnld ───────────────────────
 
def pnld_calc(numD, numA):
    """
    Calculates the probability of a 'Liberal' panel majority.
    
    Using a hypergeometric distribution (sampling without replacement), 
    calculates the probability that at least 2 out of 3 randomly drawn 
    judges are Democratic appointees.
    
    Args:
        numD (int): Number of active Democratic judges in the pool.
        numA (int): Total number of active judges in the pool.
        
    Returns:
        float: Probability (0.0 to 1.0). Returns 0.0 if numA < 3.
    """
    if numA < 3 or numD < 2:
        return 0.0
    total = comb(numA, 3)
    at_least_2 = comb(numD, 3) + comb(numD, 2) * comb(numA - numD, 1)
    return at_least_2 / total
 
def agg_circuit(g):
    """
    Aggregate the circuit-month data by circuit and year.
    pctd is the proportion of judges on the circuit who are Democrats.
    pctda is the proportion of active judges on the circuit who are Democrats.
    pnld (LiberalCourt in Huang et al. 2019) is the probability of having at least 2 Democrats on a random 3-judge panel drawn from numA judges, numD of whom are Democrats.
    """
    numD        = int((g["Party"] == "D").sum())
    numA        = len(g)
    numActive   = int((g["Senior"] == 0).sum())
    numD_active = int(((g["Party"] == "D") & (g["Senior"] == 0)).sum())
    pctd        = numD / numA if numA > 0 else float("nan")
    pctda       = numD_active / numActive if numActive > 0 else float("nan")
    return pd.Series({
        "numD": numD, "numA": numA,
        "numActive": numActive, "numD_active": numD_active,
        "pctd": pctd, "pctda": pctda,
        "pnld": pnld_calc(numD, numA),
    })
 
# Monthly date spine. it is safe to start calculating from 1980; since at least WWII there are just D vs R; but in 1890s there were other parties.
date_min = pd.Timestamp("1980-01-01")
date_max = pd.Timestamp.today().replace(day=1)
monthly  = pd.date_range(date_min, date_max, freq="ME")
 
print(f"Computing panel composition across {len(monthly)} months ...")

from tqdm import tqdm   
rows = []
for datadate in tqdm(monthly, desc="Computing panel composition"):
    active = appeals[
        appeals["jdate1"].notna() &
        (appeals["jdate1"] < datadate) &
        (appeals["jdate2"] > datadate)
    ][["jid", "CircuitNo", "Party", "Senior Status Date"]].copy()
     
    if active.empty:
        continue
     
    active["Senior"] = (
        active["Senior Status Date"].notna() &
        (datadate > active["Senior Status Date"])
    ).astype(int)
    active["datadate"] = datadate
    rows.append(active)


judge_dates = pd.concat(rows, ignore_index=True)
 
circuit_month = (
    judge_dates
    .groupby(["datadate", "CircuitNo"])
    .apply(agg_circuit, include_groups=False)
    .reset_index()
)
 



# ── Merge circuit_states ──────────────────────────────────────────────────────
circuit_states = pd.DataFrame([
    ("C1","MA"),("C1","ME"),("C1","NH"),("C1","PR"),("C1","RI"),
    ("C10","CO"),("C10","KS"),("C10","NM"),("C10","OK"),("C10","UT"),("C10","WY"),
    ("C11","AL"),("C11","FL"),("C11","GA"),
    ("C2","CT"),("C2","NY"),("C2","VT"),
    ("C3","DE"),("C3","NJ"),("C3","PA"),("C3","VI"),
    ("C4","MD"),("C4","NC"),("C4","SC"),("C4","VA"),("C4","WV"),
    ("C5","LA"),("C5","MS"),("C5","TX"),
    ("C6","KY"),("C6","MI"),("C6","OH"),("C6","TN"),
    ("C7","IL"),("C7","IN"),("C7","WI"),
    ("C8","AR"),("C8","IA"),("C8","MN"),("C8","MO"),("C8","ND"),("C8","NE"),("C8","SD"),
    ("C9","AK"),("C9","AZ"),("C9","CA"),("C9","HI"),("C9","ID"),
    ("C9","MT"),("C9","NV"),("C9","OR"),("C9","WA"),
    ("DC","DC"),
], columns=["CircuitNo", "statecode"])
 
circuit_month = circuit_month.merge(circuit_states, on="CircuitNo", how="left")
 
# ── Summary statistics of judge ideology in circuit courts ──────────────────
# LiberalCourt = yearly mean of monthly pctda (active non-senior dem share)
circuit_order = ["C1","C2","C3","C4","C5","C6","C7","C8","C9","C10","C11","DC"]
circuit_label = {"C1":"1st","C2":"2nd","C3":"3rd","C4":"4th","C5":"5th","C6":"6th",
                 "C7":"7th","C8":"8th","C9":"9th","C10":"10th","C11":"11th","DC":"D.C."}
 
cm = circuit_month.copy()
cm["year"] = cm["datadate"].dt.year
 
summ_circuityear_data = (
    cm[cm["year"].between(1996, 2014) & cm["CircuitNo"].isin(circuit_order)]
    .groupby(["year", "CircuitNo"])["pctda"]
    .mean()
    .reset_index()
)
 
# Pivot: rows = year, cols = circuit
summ_circuityear = (
    summ_circuityear_data
    .pivot(index="year", columns="CircuitNo", values="pctda")
    .reindex(columns=circuit_order)
    .rename(columns=circuit_label)
)
 
# Yearly mean and SD across circuits (each row)
summ_circuityear["Yearly Mean"] = summ_circuityear.mean(axis=1)
summ_circuityear["Yearly SD"]   = summ_circuityear.std(axis=1)
 
# Footer rows: circuit mean and SD across years
circuit_mean = summ_circuityear[list(circuit_label.values())].mean(axis=0).rename("Circuit mean")
circuit_sd   = summ_circuityear[list(circuit_label.values())].std(axis=0).rename("Circuit SD")
 
summ_circuityear = pd.concat([summ_circuityear, circuit_mean.to_frame().T, circuit_sd.to_frame().T])
summ_circuityear.index.name = "Year"
 
print("\nSummary Statistics of Judge Ideology in Circuit Courts")
print(summ_circuityear.round(3).to_string())
# This is different from Table 1 in Huang et al. (2019) because their Table 1 is after merging with Compustat 
 
# ── 11. Write out ──────────────────────────────────────────────────────────────
#out.to_csv(FJC_OUTFILE, sep="|", index=False)
circuit_month[["datadate", "CircuitNo", "statecode",  "pctd", "pctda", "pnld"]].to_csv(FJC_OUTFILE, sep="|", index=False)


print(f"      {len(circuit_month)} circuit-month rows    -> circuit_pctd_pnld_{current_year}{current_month:02d}.csv")



if False:   #check
    circuit_month["jyear"] = circuit_month.datadate.dt.year 
    df1 = circuit_month.groupby(["CircuitNo", "jyear"]).pctd.mean().reset_index()
    df1.loc[df1.jyear==1996]
    #Expected output:
    """>>> df1.loc[df1.jyear==1996]
    CircuitNo  jyear      pctd
16         C1   1996  0.300000
63        C10   1996  0.475490
109       C11   1996  0.490079
156        C2   1996  0.373538
203        C3   1996  0.256536
250        C4   1996  0.375000
297        C5   1996  0.428571
344        C6   1996  0.400000
391        C7   1996  0.294118
438        C8   1996  0.388889
485        C9   1996  0.484353
530        CF   1996  0.336091
577        DC   1996  0.416667"""