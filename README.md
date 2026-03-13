# LiberalCourt
Builds the **LiberalCourt (pnld)** measure of federal judge ideology at the circuit–state–month level, following Huang, Hui, and Li (2019).

- **`read_fjc_judges_csv.py`**: Python script that downloads FJC `judges.csv`, parses appointments and party, and computes pctd, pctda, and pnld (LiberalCourt) by circuit–month, then expands to circuit–state–month.
- **`data/liberalcourt_pnld_202603.csv`**: LiberalCourt (pnld) and related measures at the circuit–state–month level (e.g. `circuit_pctd_pnld_YYYYMM.csv` from the script).


## Reference

**Huang, A., Hui, K. W., & Li, R. Z.** (2019). Federal judge ideology: A new measure of ex ante litigation risk. *Journal of Accounting Research*, 57(2), 431–489.

**Original JAR materials:** Allen Huang, Kai Wai Hui, and Reeyarn Zhiyang Li, [Federal Judge Ideology: A New Measure of Ex-Ante Litigation Risk](https://onlinelibrary.wiley.com/doi/abs/10.1111/1475-679X.12260) ([online appendix](https://www.chicagobooth.edu/-/media/research/arc/docs/journal/online-supplements/hhl-online-appendix.pdf)) ([datasheet and code](https://www.chicagobooth.edu/-/media/research/arc/docs/journal/online-supplements/hhl-datasheet-and-code.zip)).


---
The script replicates the construction of the *LiberalCourt* variable used in that paper to measure ex ante securities litigation risk from the composition of U.S. Courts of Appeals.


## What it does

1. **Download**  
   Fetches `judges.csv` from the [Federal Judicial Center](https://www.fjc.gov/sites/default/files/history/judges.csv). If a local `judges.csv` exists and is less than 30 days old, it is used instead; otherwise the file is re-downloaded and saved.

2. **Reshape**  
   Converts FJC’s wide-format judge file into a long panel:
   - One row per **appointment** (up to 6 per judge), with court, president, party, and dates.
   - Education (up to 5 entries) and other federal service (up to 4) are collapsed into single fields per judge.

3. **Parse & restrict**  
   - Party of appointing president → **D** (Democrat/Liberal) or **R** (Republican/Conservative).  
   - Commission / recess start → `jdate1`; senior status or termination → `jdate2`.  
   - Circuit is inferred from court name (C1–C11, DC; Federal Circuit CF is excluded from panel composition).  
   - Only **U.S. Court of Appeals** rows are used for the ideology panel.

4. **Monthly panel composition**  
   For each month from 1980 through the current month:
   - Determines which judges are **serving** (`jdate1` &lt; month-end &lt; `jdate2`).
   - Flags **senior status** (on or after Senior Status Date).
   - By **(datadate, CircuitNo)** computes:
     - **pctd**: share of all serving judges (active + senior) appointed by Democrats.
     - **pctda**: share of **active** (non-senior) judges appointed by Democrats.
     - **pnld**: probability that a random 3-judge panel has ≥2 Democratic appointees (hypergeometric, sampling without replacement).

5. **Circuit–state expansion**  
   Each circuit is merged to its states (e.g., C9 → AK, AZ, CA, HI, ID, MT, NV, OR, WA), so the final panel is at **circuit–state–month** (one row per circuit–state–month).

6. **Outputs**  
   - **Appointment-level**: `history_judiciary_1789_YYYY.csv` (pipe-separated).  
   - **LiberalCourt panel**: `circuit_pctd_pnld_YYYYMM.csv` (see [Output variables](#output-variables)).  
   - Console: summary statistics of judge ideology by circuit and year (1996–2014), in the spirit of Huang et al. Table 1.

---

## Output variables

File: `circuit_pctd_pnld_YYYYMM.csv`

| Variable    | Description |
|------------|-------------|
| `datadate` | Month-end date (YYYY-MM-DD). |
| `CircuitNo`| Circuit (C1–C11, DC). |
| `statecode`| Two-letter U.S. state (or DC, PR, VI). |
| `pctd`     | Share of *all* judges (active + senior) in the circuit that month appointed by Democrats. |
| `pctda`    | Share of *active* (non-senior) judges appointed by Democrats. |
| `pnld`     | **LiberalCourt**: probability that a random 3-judge panel has ≥2 Democratic appointees (0–1). |

---

## pnld (LiberalCourt) formula

For a circuit-month with `numA` serving judges and `numD` Democratic appointees, **pnld** is the probability that at least 2 of 3 randomly chosen judges are Democrats, under sampling **without replacement** (hypergeometric):

- Total 3-judge panels: `C(numA, 3)`  
- Panels with ≥2 Democrats: `C(numD, 3) + C(numD, 2) * C(numA - numD, 1)`  
- **pnld** = (panels with ≥2 D) / (total panels)  

If `numA < 3` or `numD < 2`, **pnld** is set to 0.

---

## Circuit–state mapping

| Circuit | States |
|---------|--------|
| C1  | MA, ME, NH, PR, RI |
| C2  | CT, NY, VT |
| C3  | DE, NJ, PA, VI |
| C4  | MD, NC, SC, VA, WV |
| C5  | LA, MS, TX |
| C6  | KY, MI, OH, TN |
| C7  | IL, IN, WI |
| C8  | AR, IA, MN, MO, ND, NE, SD |
| C9  | AK, AZ, CA, HI, ID, MT, NV, OR, WA |
| C10 | CO, KS, NM, OK, UT, WY |
| C11 | AL, FL, GA |
| DC  | DC |

---

## Requirements

- Python 3  
- `pandas`, `requests`, `tqdm`

Install:

```bash
pip install pandas requests tqdm
```

---

## Usage

Run from the directory where you want `judges.csv` and the output CSVs written (or set paths inside the script):

```bash
python read_fjc_judges_csv.py
```

No arguments. Output filenames use the current year and month (e.g. `circuit_pctd_pnld_202603.csv`).

---
## License for `read_fjc_judges_csv.py`

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

## License for other files

**ALL RIGHTS RESERVED.** All other files in this repository (including documentation, data processing scripts, and research assets) are the intellectual property of the author. No permission is granted for reproduction, distribution, or modification of these files without explicit written consent.