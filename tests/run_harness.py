# tests/run_harness.py
# Offline E2E check on your real workbook with synthetic emails

import pandas as pd
from pathlib import Path
import re
from mock_gspread import MockWorksheet
import test_logic as bl

WB = Path("RENT TRACKING-Lemaiyan Heights test.xlsx")

def load_ws(sheet_name: str):
    df = pd.read_excel(WB, sheet_name=sheet_name, header=None)
    return MockWorksheet(sheet_name, df.fillna("").values.tolist())

def email(amount, code, payer, phone, dt_str, ref, with_hash=True, lowercase=False):
    code2 = code.lower() if lowercase else code
    payer2 = payer.lower() if lowercase else payer
    h = f"#{code2}" if with_hash else f" {code2}"
    return (f"Your M-Pesa payment of KES {amount:,.2f} for account: PAYLEMAIYAN {h} "
            f"has been received from {payer2} {phone} on {dt_str}. "
            f"M-Pesa Ref: {ref}. NCBA, Go for it.")

CASES = [
    ("E5 - Catherine", email(12000, "E5", "CATHERINE GATHONI", "070****117", "05/08/2025 10:47 PM", "TH543V6HDY", True, False)),
    ("B3 - Rama",      email(12000, "B3", "rama mwangi",      "071****111", "12/09/2025 09:30 PM", "ABCD123456", True, True)),
    ("E4 - Benjamin",  email(12000, "E4", "john doe",         "072****222", "06/09/2025 08:15 PM", "XYZ9876543", False, True)),
]

def _detect(vals): return bl._detect_header_row(vals)
def _colmap(h):    return bl._header_colmap(h)

def snapshot(ws, month=None):
    vals = ws.get_all_values()
    h0 = _detect(vals); header = vals[h0]
    cmap = _colmap(header); rows = vals[h0+1:]
    tgt = None
    for i, r in enumerate(rows):
        if len(r) > cmap["month"] and r[cmap["month"]].strip() == (month or ""):
            tgt = i; break
    if tgt is None:
        for i in range(len(rows)-1, -1, -1):
            if any(c.strip() for c in rows[i]): tgt = i; break
    r = rows[tgt] if tgt is not None else [""]*len(header)
    def g(k): 
        idx = cmap[k]; 
        return r[idx] if idx < len(r) else ""
    return {
        "Month": g("month"),
        "Amount Paid": g("amount_paid"),
        "Date Paid": g("date_paid"),
        "REF Number": g("ref"),
        "Date Due": g("date_due"),
        "Penalties": g("penalties"),
        "Prepayment/Arrears": g("prepay_arrears"),
    }, h0+1+(tgt+1 if tgt is not None else 1), header, cmap

def run():
    results = []
    for sheet, text in CASES:
        ws = load_ws(sheet)
        pay = bl.parse_email(text)
        before, *_ = snapshot(ws)
        info = bl.update_tenant_month_row(ws, pay, debug=[])
        after, row_abs, header, cmap = snapshot(ws, month=info["month"])
        rowvals = ws.get_all_values()[row_abs-1]
        penalty_formula = rowvals[cmap["penalties"]]
        balance_formula = rowvals[cmap["prepay_arrears"]]
        results.append((sheet, info, before, after, penalty_formula, balance_formula))
    return results

if __name__ == "__main__":
    out = run()
    for sheet, info, before, after, pen, bal in out:
        print(f"== {sheet} | Month {info['month']} | Row {info['row']}")
        print(f"Paid {info['paid_before']} -> {info['paid_after']}")
        print("Before:", before)
        print("After :", after)
        print("Penalty formula:", pen)
        print("Balance formula:", bal)
        print()
