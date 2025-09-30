# tests/run_harness.py
# Offline E2E check on your real workbook with synthetic emails

from pathlib import Path
from mock_gspread import MockWorksheet
import test_logic as bl

CANONICAL_HEADER = ['Month','Date Due','Amount Due','Amount Paid','Date Paid','REF Number','Comments','Prepayment/Arrears','Penalties']

def mk_minimal_tenant_sheet(title="HARNESS - T1", monthly_due=12000.0, seed_aug_row=False):
    grid = [[""]*10 for _ in range(6)]
    grid.append(CANONICAL_HEADER[:])
    if seed_aug_row:
        grid.append(["Aug-2025","05/08/2025", f"{monthly_due:g}", "0", "", "", "None", "0", "0"])
    return MockWorksheet(title, grid)

def case_penalty():
    ws = mk_minimal_tenant_sheet(title="HARNESS - PEN")
    text = "Your M-Pesa payment of KES 12,000.00 for account: PAYLEMAIYAN #t1 has been received from test name 070****111 on 08/09/2025 09:00 PM. M-Pesa Ref: ABCD123456. NCBA, Go for it."
    p = bl.parse_email(text); p["AccountCode"] = "T1"
    info = bl.update_tenant_month_row(ws, p, debug=[])
    vals = ws.get_all_values()
    print("== PENALTY CASE ==")
    print("Row:", info["row"], "Month:", info["month"])
    print("Penalty formula:", vals[info["row"]-1][vals[6].index("Penalties")])
    print()


def case_prepayment_carry():
    ws = mk_minimal_tenant_sheet(title="HARNESS - PREPAY", seed_aug_row=True)
    text = "Your M-Pesa payment of KES 36,000.00 for account: PAYLEMAIYAN #t1 has been received from john 070****111 on 04/09/2025 09:00 AM. M-Pesa Ref: BIGPAY999. NCBA, Go for it."
    p = bl.parse_email(text); p["AccountCode"] = "T1"
    info = bl.update_tenant_month_row(ws, p, debug=[])
    vals = ws.get_all_values()
    months = [r[vals[6].index("Month")] for r in vals[7:] if any(str(c).strip() for c in r)]
    print("== PREPAYMENT CARRY ==")
    print("Base row:", info["row"], "Created future months:", info["autocreated_future_months"])
    print("Months present:", months)
    print()

if __name__ == "__main__":
    case_penalty()
    
    case_prepayment_carry()