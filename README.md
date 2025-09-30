# 🏠 RentRPA — Technical Documentation & Operations Manual

> Version: 2.x • Updated: 2025-09 • Scope: Gmail → Google Sheets rent tracking for NCBA/M‑Pesa emails

# 📊 Rental Income Record keeping Automation Bot 🏠

<img src="images/rent%20bot.jpg" alt="Rent bot" width="400"/>

***
Automate your apartment rent payment tracking using this Streamlit-based RPA bot. The bot fetches MPESA Paybill payment notifications from Gmail and updates a Google Sheet in real-time—no manual logging, no spreadsheet headaches!

## 🚀 Features

* 🔑 Secure Google OAuth 2.0 Login – No need to store passwords.

* 📩 Fetches MPESA Payment Emails – Automatically pulls bank transaction notifications.

* 📄 Parses Payment Details – Extracts Amount, Payer Name, Reference Code, etc.

* 📊 Updates Google Sheets in Real-Time – Adds new payment entries for each tenant.

* 🛡 Deduplication Logic – Never logs the same transaction twice.

* 🖥 Streamlit Web Interface – Simple, user-friendly dashboard.

***

- Business rules:

 **Rent due = 5th**; **Penalty KES 3000** if **Date Paid ≥ (Due + 2 days)** **and** **net‑after ≤ 0**.
- **Prepayments auto‑consumed**: positive balances create future month rows with **Paid = 0** to consume over time.
- **First‑payment override removed** (no B3:B5 zeroing logic).
- Defensive formulas (no `#VALUE!`), quota‑friendly writes, MonthKey for sorting.
- UI includes **Migration/Repair** & **MonthKey backfill** tools.

---

## 🗂 Project Structure

```bash

/RentRPA-bot/
│
├── data
├── images
├── docs
├── tests
├── CONTRIBUTING.md
├── LICENSE
├── RentRPA.ipynb
├── README.md                # This file
├── requirements.txt         # Python dependencies
├── prototype.py             # Prototype program(Local running)
├── streamlit_app.py         # Main Streamlit UI file
└── bot_logic.py         # Backend logic for email parsing & Sheets updating

```

## 1) System Overview

### 1.1 Components
- **`app/streamlit_app.py`** — Streamlit UI, Gmail + Sheets OAuth, runs ingestion, maintenance tools, portfolio metrics.
- **`app/bot_logic.py`** — Core logic: parse email → find/create month row → write values/formulas → auto‑prepay carry → set MonthKey.
- **`tests/`** — Offline test kit (mock gspread), harness and pytest suite.
- **GitHub Actions** — CI to run offline tests on push/PR.

### 1.2 Data flow
1. UI searches Gmail with a query (e.g., `PAYLEMAIYAN subject:"NCBA TRANSACTIONS STATUS UPDATE" newer_than:365d`).
2. For each message: extract plain text → **`parse_email`** → structured payment.
3. **Deduplicate** by MPesa Ref (sheet `ProcessedRefs`).
4. For each payment: locate/create tenant tab → update the month row → write History + mark Gmail as read (optional).

---

## 2) Business Rules (Authoritative)

- **Due date**: 5th of the target month (column **Date Due**).
- **Penalty**:  **3000** when **(Date Paid ≥ Date Due + 2 days) AND (net_after ≤ 0)**.
  - `net_after = previous_balance + amount_paid − amount_due`.
  - Defensive coercions ensure text/blank cells don’t cause `#VALUE!`.
- **Prepayments**:
  - If resulting balance **≥ one month’s rent**, automatically create **future month rows**.
  - Future rows are set **Amount Paid = 0** and **Date Paid blank** with comment **“Auto prepayment applied”**.
  - Each created row consumes one month’s due in the rolling balance; penalties remain **0** (no Date Paid).
- **First‑payment rule**: **removed** — first rows use the same balance formula as others.

---

## 3) Google Sheet Model

### 3.1 Tenant tabs (one per account)
**Canonical headers** (detected; typically row 7):
```
Month | Date Due | Amount Due | Amount Paid | Date Paid | REF Number | Comments | Prepayment/Arrears | Penalties | (auto) MonthKey
```

- **Month**: free‑format display (e.g., `Sep-2025`); **MonthKey** stores `YYYY‑MM` for sorting.
- **Comments**: never overwritten by the bot unless auto‑prepayment comment on new future rows.

### 3.2 Meta sheets
- **PaymentHistory**: `Date Paid, Amount Paid, REF Number, Payer, Phone, Comments, AccountCode, TenantSheet, Month`.
- **ProcessedRefs**: first column `Refs` (uppercase REF for dedupe).

---

## 4) Installation & Setup

### 4.1 Requirements
- Python **3.10+**
- Google Cloud project with **OAuth (Web Application)**
- A **Google Sheet** (converted from Excel for best compatibility)

### 4.2 Secrets (`.streamlit/secrets.toml`)
```toml
ENV = "local" # or "prod"

[google_oauth]
client_id = "YOUR_CLIENT_ID.apps.googleusercontent.com"
client_secret = "YOUR_CLIENT_SECRET"
redirect_uri_local = "http://localhost:8501/"
redirect_uri_prod  = "https://your-app.streamlit.app/"
```

### 4.3 Local run
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt
streamlit run app/streamlit_app.py
```

Open the app, sign in with Google, paste your **Google Sheet URL**, set **Gmail query**, and click **Run Bot Now**.

---

## 5) Streamlit UI — Operator Guide

### 5.1 Inputs
- **Google Sheet URL** — target workbook.
- **Gmail search query** — narrow scope to control volume/quotas.
- Options: Mark as read, throttle delay, max results, auto‑create tabs, verbose debug.

### 5.2 Run
- Click **▶️ Run Bot Now**.
- The app parses emails, skips already processed REFs, writes tenant rows, appends to PaymentHistory, and logs actions.

### 5.3 Maintenance Tools

#### A) 🛠 Backfill MonthKey (maintenance)
- Safely fills missing `MonthKey` for all tenant sheets.
- Batches + delay to avoid 429 write‑quota errors.

#### B) 🔧 Repair Formulas (migration)
- Rewrites **Penalties** and **Prepayment/Arrears** formulas for **all existing rows** using the latest defensive logic.
- Optional checkbox to also **backfill MonthKey** in the same run.
- Configure chunk size + delay for quota friendliness.

> **Tip:** Run **Repair Formulas** once after deploying a new version to normalize legacy sheets.

---

## 6) Calculation Details (Formulas)

> Notation below uses A1 references for the current row; the app writes these expressions with the correct addresses.

### 6.1 Defensive coercions (used in all formulas)
- `paid_num  = IFERROR(VALUE(AmountPaid), N(AmountPaid))`
- `due_num   = IFERROR(VALUE(AmountDue),  N(AmountDue))`
- `prev_bal  = IFERROR(VALUE(PrevBalanceCell), N(PrevBalanceCell))`  ← fixes first‑row header issue
- `dpaid     = IFERROR(DATEVALUE(TO_TEXT(DatePaid)), DatePaid)`
- `ddue      = IFERROR(DATEVALUE(TO_TEXT(DateDue)),  DateDue)`
- Presence checks: `LEN(TO_TEXT(DatePaid))>0`, `LEN(TO_TEXT(DateDue))>0`

### 6.2 Penalty
```
=IF(AND( LEN(TO_TEXT(DatePaid))>0,
         LEN(TO_TEXT(DateDue))>0,
         (prev_bal + paid_num - due_num) <= 0,
         dpaid >= ddue + 2 ),
    3000, 0)
```

### 6.3 Prepayment/Arrears (rolling balance)
- First data row: `= paid_num - due_num - Penalties`
- Subsequent rows: `= prev_bal + paid_num - due_num - Penalties`

### 6.4 Auto‑prepayment carry
- After posting a payment, if estimated **balance ≥ monthly rent**, create future rows:
  - `Amount Due = monthly rent`
  - `Amount Paid = 0`
  - `Date Paid = ""` (blank)
  - `Comments = "Auto prepayment applied"`
  - Formulas as above; penalty evaluates to **0** (no Date Paid)
  - Repeat until remaining balance **< monthly rent** (cap: 24 months)

---

## 7) Testing & CI

### 7.1 Offline tests (no Google APIs)
```bash
pip install -r requirements-dev.txt  # or: pip install pytest pandas numpy openpyxl
pytest -q
```
- Covers: parser, penalty rule (≥ due+2 & net_after ≤ 0), defensive formulas, prepayment auto‑carry.

### 7.2 Harness (manual)
```bash
python -m tests.run_harness
```
Shows example runs and prints the generated formulas/rows.

### 7.3 GitHub Actions
- Workflow: `.github/workflows/tests.yml`
- Matrix Python 3.10 / 3.11; caches pip; uploads test artifacts.

---

## 8) Operations SOP

### 8.1 Daily/weekly processing
1. Open the app → verify **Sheet URL** and **Gmail query**.
2. Click **Run Bot Now**.
3. Review **Run Log** and **Metrics**.

### 8.2 Monthly close
1. Run **Repair Formulas (migration)** if any sheet structure changed that month.
2. Run **Backfill MonthKey** if new tenant tabs were added manually.
3. Export or archive PaymentHistory if desired.

### 8.3 Quota hygiene
- Keep Gmail query narrow; use `max_results`.
- Prefer maintenance tools with conservative **chunk sizes** and **delays**.
- The app uses exponential backoff for 429s.

---

## 9) Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| `#VALUE!` in Balance/Penalties | Legacy text values or header reference in previous balance | Run **Repair Formulas**; new formulas coerce with `VALUE`/`N` and handle blanks. |
| 429 quota errors | Too many write calls/minute | Increase delay, reduce chunk size, re‑run. |
| 500 on `ws.sort` | Transient Sheets backend | Sorting is best‑effort; it’s safe to ignore. |
| OAuth callback error | Redirect URI mismatch | Ensure exact URI (including trailing slash) is configured in GCP and secrets. |
| No tenant tab match | Mismatched tab names | Enable **Auto‑create tenant tabs** or rename tab to start with `AccountCode`. |

---

## 10) Dummy Emails (for live testing)

Paste these to your Gmail (self‑email) and run the bot.

```
Your M-Pesa payment of KES 12,000.00 for account: PAYLEMAIYAN #b3 has been received from rama mwangi 071****111 on 12/09/2025 09:30 PM. M-Pesa Ref: ABCD123456. NCBA, Go for it.

Your M-Pesa payment of KES 12,000.00 for account: PAYLEMAIYAN e4 has been received from john doe 072****222 on 06/09/2025 08:15 PM. M-Pesa Ref: XYZ9876543. NCBA, Go for it.

your m-pesa payment of kes 36000.00 for account: paylemaiyan #E5 has been received from catherine gathoni 070****117 on 05/08/2025 10:47 PM. m-pesa ref: th543v6hdy. NCBA, go for it.
```

Variations covered:
- Case‑insensitive, account with/without `#`, lowercase names, refs normalized.

---

## 11) Security
- Google OAuth; tokens live only in the Streamlit session.
- Do not commit secrets; use Streamlit Secrets.
- App touches only your specified spreadsheet and Gmail messages via explicit scopes.

---

## 12) Change Log (major)
- **v2.4** — Auto‑prepayment rows set **Paid=0**, blank Date Paid; stronger prev‑balance coercion (fixes first penalty cell).
- **v2.3** — Added **Repair Formulas (migration)**; MonthKey backfill tool.
- **v2.2** — Prepayment auto‑carry (future months); penalty rule `≥ due+2` & `net_after ≤ 0`.
- **v2.1** — Removed first‑payment (B3:B5) zeroing logic; unified balance formula.
- **v2.0** — Defensive formulas; write‑quota reductions; sorting best‑effort.

---

## 13) FAQ
**Q:** Why create future months with Paid=0?
**A:** To consume positive balances automatically month‑by‑month while keeping penalties at 0 (no Date Paid).

**Q:** Can I disable auto‑prepayment?
**A:** Yes — add a small toggle in the UI (feature flag). Default is ON.

**Q:** What if my Month column uses a different format?
**A:** The bot derives MonthKey from several formats; Month display is preserved.

---

## 14) Glossary
- **MonthKey** — `YYYY‑MM` helper for sorting/filtering.
- **Net After** — `prev + paid − due` for penalty evaluation.
- **Carry row** — auto‑created future row to consume prepayment with Paid=0.

---

## 15) Contact & Ownership


Eugene Maina |
Data Scientist | RPA Developer

* [LinkedIn](https://www.linkedin.com/in/eugene-maina-4a8b9a128/) | [GitHub](https://github.com/eugene-maina72) | [Email](mailto:eugenemaina72@gmail.com)
