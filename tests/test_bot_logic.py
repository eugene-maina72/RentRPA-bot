# tests/test_bot_logic.py
# Pytest suite to guard parsing, header alignment, and the due+2 penalty rule.

import re
import pandas as pd
from pathlib import Path
from mock_gspread import MockWorksheet
import test_logic as bl

WB = Path("tests/RENT TRACKING-Lemaiyan Heights test.xlsx")

def load_ws(name):
    df = pd.read_excel(WB, sheet_name=name, header=None)
    return MockWorksheet(name, df.fillna("").values.tolist())

def email(amount, code, payer, phone, dt_str, ref, with_hash=True, lowercase=False):
    code2 = code.lower() if lowercase else code
    payer2 = payer.lower() if lowercase else payer
    h = f"#{code2}" if with_hash else f" {code2}"
    return (f"Your M-Pesa payment of KES {amount:,.2f} for account: PAYLEMAIYAN {h} "
            f"has been received from {payer2} {phone} on {dt_str}. "
            f"M-Pesa Ref: {ref}. NCBA, Go for it.")

def _detect(vals): return bl._detect_header_row(vals)
def _colmap(h):    return bl._header_colmap(h)

def _penalty_uses_plus2(formula: str) -> bool:
    return re.search(r"\+\s*2\)", formula) is not None

def test_parse_variations():
    cases = [
        email(12000, "E5", "CATHERINE GATHONI", "070****117", "05/08/2025 10:47 PM", "TH543V6HDY", True, False),
        email(12000, "b3", "rama mwangi",       "071****111", "12/09/2025 09:30 PM", "ABCD123456", True, True),
        email(12000, "e4", "john doe",          "072****222", "06/09/2025 08:15 PM", "XYZ9876543", False, True),
    ]
    for t in cases:
        p = bl.parse_email(t)
        assert p["REF Number"]
        assert p["AccountCode"] in {"E5","B3","E4"}

def test_history_schema_first6_match_workbook():
    # Workbook: ['Date Paid','Amount Paid','REF Number','Payer','Phone','Comments']
    df = pd.read_excel(WB, sheet_name="PaymentHistory", nrows=0)
    book_first6 = list(df.columns)[:6]
    assert book_first6 == ['Date Paid','Amount Paid','REF Number','Payer','Phone','Comments']
    assert bl.PAYMENT_COLS[:6] == book_first6  # <-- will FAIL until you change Payment Mode -> Comments

def test_penalty_rule_is_due_plus_2():
    ws = load_ws("B3 - Rama")
    t = email(12000, "b3", "rama mwangi", "071****111", "12/09/2025 09:30 PM", "ABCD123456", True, True)
    p = bl.parse_email(t)
    info = bl.update_tenant_month_row(ws, p, debug=[])
    vals = ws.get_all_values()
    h0 = _detect(vals); header = vals[h0]; cmap = _colmap(header)
    pen_formula = vals[info["row"]-1][cmap["penalties"]]
    assert _penalty_uses_plus2(pen_formula), f"Penalty formula must use due+2, got: {pen_formula}"
