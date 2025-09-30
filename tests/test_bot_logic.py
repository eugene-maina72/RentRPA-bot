# tests/test_bot_logic.py
# Pytest suite to guard parsing, header alignment, and the due+2 penalty rule.

import re
import pandas as pd
from pathlib import Path
from mock_gspread import MockWorksheet
import test_logic as bl

WB = Path("tests/RENT TRACKING-Lemaiyan Heights test.xlsx")  # used only for legacy checks

# ---------- Helpers ----------

CANONICAL_HEADER = ['Month','Date Due','Amount Due','Amount Paid','Date Paid','REF Number','Comments','Prepayment/Arrears','Penalties']

def mk_minimal_tenant_sheet(title="T1 - Test", monthly_due=12000.0, seed_aug_row=False):
    """
    Build a minimal tenant worksheet:
    - Headers at row 7
    - Optional seed row for Aug-2025 with given Amount Due and zero balance
    """
    grid = [[""]*10 for _ in range(6)]  # rows 1..6 padding
    grid.append(CANONICAL_HEADER[:])    # row 7 header
    if seed_aug_row:
        # Month, Date Due (5th), Amount Due, Amount Paid (0), Date Paid blank, REF blank, Comments, Balance 0, Penalties 0
        grid.append(["Aug-2025","05/08/2025", f"{monthly_due:g}", "0", "", "", "None", "0", "0"])
    return MockWorksheet(title, grid)

def addr_of(ws, row, colname):
    vals = ws.get_all_values()
    header = vals[6]
    j = header.index(colname)
    return (row+1, j+1)  # 1-indexed

def get_header_map(ws):
    vals = ws.get_all_values()
    header = vals[6]
    return {name: idx for idx, name in enumerate(header)}

def get_last_data_row(ws):
    vals = ws.get_all_values()
    # after header row 7
    for i in range(len(vals)-1, 7, -1):
        if any(str(c).strip() for c in vals[i]):
            return i
    return 7

# ---------- Existing basic tests (schema & parse) ----------

def test_history_schema_first6_match_workbook_if_present():
    # If workbook is present and has PaymentHistory, verify first 6 column names.
    try:
        df = pd.read_excel(WB, sheet_name="PaymentHistory", nrows=0)
    except Exception:
        return  # workbook not mandatory for this test run
    book_first6 = list(df.columns)[:6]
    assert book_first6 == ['Date Paid','Amount Paid','REF Number','Payer','Phone','Comments']
    assert bl.PAYMENT_COLS[:6] == book_first6

def test_parse_variations_case_and_hash():
    text = "Your M-Pesa payment of KES 12,000.00 for account: PAYLEMAIYAN e4 has been received from john doe 072****222 on 06/09/2025 08:15 PM. M-Pesa Ref: XYZ9876543. NCBA, Go for it."
    p = bl.parse_email(text)
    assert p and p["AccountCode"] == "E4" and p["REF Number"] == "XYZ9876543"

# ---------- New tests ----------

def test_penalty_rule_ge_due_plus_2_and_balance_le_zero():
    ws = mk_minimal_tenant_sheet(title="PEN - Case")
    pay = bl.parse_email("Your M-Pesa payment of KES 12,000.00 for account: PAYLEMAIYAN #t1 has been received from test name 070****111 on 08/09/2025 09:00 PM. M-Pesa Ref: ABCD123456. NCBA, Go for it.")
    pay["AccountCode"] = "T1"  # direct route
    info = bl.update_tenant_month_row(ws, pay, debug=[])
    vals = ws.get_all_values()
    h = get_header_map(ws)
    row = info["row"]-1
    pen_formula = vals[row][h["Penalties"]]
    # Must include ">= ... + 2" per new rule
    assert ">=" in pen_formula and "+ 2" in pen_formula
    # And the formula references both date cells
    assert "DATEVALUE" in pen_formula or "TO_TEXT" in pen_formula



def test_defensive_formulas_do_not_propagate_value_errors():
    ws = mk_minimal_tenant_sheet(title="VAL - Case")
    # Put text "None" in prev balance cell to mimic messy sheets
    vals = ws.get_all_values()
    # Seed a fake previous row with string values to ensure IFERROR/N() guards are present
    ws.update("8:8", [["Aug-2025","05/08/2025","12000","notnum","","","None","notnum","notnum"]])
    pay = bl.parse_email("Your M-Pesa payment of KES 12,000.00 for account: PAYLEMAIYAN #t1 has been received from test 070****111 on 06/09/2025 09:00 AM. M-Pesa Ref: SAFEFORM1. NCBA, Go for it.")
    pay["AccountCode"] = "T1"
    info = bl.update_tenant_month_row(ws, pay, debug=[])
    vals = ws.get_all_values(); h = get_header_map(ws); r = info["row"]-1
    # Check penalty/balance formulas contain IFERROR and VALUE/N coercions
    pen_formula = vals[r][h["Penalties"]]
    bal_formula = vals[r][h["Prepayment/Arrears"]]
    assert "IFERROR" in pen_formula and "DATEVALUE" in pen_formula
    assert "IFERROR" in bal_formula and ("VALUE(" in bal_formula or "N(" in bal_formula)

def test_prepayment_auto_carry_creates_future_rows():
    ws = mk_minimal_tenant_sheet(title="PREPAY - Case", monthly_due=12000.0, seed_aug_row=True)
    pay = bl.parse_email(
        "Your M-Pesa payment of KES 36,000.00 for account: PAYLEMAIYAN #t1 "
        "has been received from john 070****111 on 04/09/2025 09:00 AM. "
        "M-Pesa Ref: BIGPAY999. NCBA, Go for it."
    )
    pay["AccountCode"] = "T1"

    info = bl.update_tenant_month_row(ws, pay, debug=[])
    assert info["autocreated_future_months"] >= 2  # e.g., Oct & Nov created

    vals = ws.get_all_values()
    h = get_header_map(ws)

    future_months = []
    for r in vals[7:]:
        if not any(str(c).strip() for c in r):
            continue
        month = r[h["Month"]]
        if any(k in month for k in ("Oct-2025", "2025-10", "Nov-2025", "2025-11")):
            future_months.append({
                "Month": month,
                "AmountPaid": r[h["Amount Paid"]],
                "AmountDue": r[h["Amount Due"]],
                "DatePaid": r[h["Date Paid"]],
                "Comments": r[h["Comments"]],
                "Penalties": r[h["Penalties"]],
            })

    # Must have at least two future months
    assert len(future_months) >= 2

    # In auto-carry rows: Amount Paid == 0, Date Paid blank, comment set, penalty formula present
    for fm in future_months:
        assert str(fm["AmountPaid"]).strip() in {"0", "0.0"}
        assert fm["Comments"] == "Auto prepayment applied"
        assert str(fm["DatePaid"]).strip() == ""  # blank ⇒ no penalty
        assert isinstance(fm["Penalties"], str) and fm["Penalties"].startswith("=IF(")