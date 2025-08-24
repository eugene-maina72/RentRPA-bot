
# bot_logic_merged.py
# This is a merged version of bot_logic.py with bugfixes.
# and adds exponential backoff. Re-exports PATTERN, PAYMENT_COLS, parse_email.

from datetime import datetime
import time
from gspread.exceptions import APIError
from gspread.utils import ValueInputOption

# Pull your original parsing bits from the existing module
from bot_logic import PATTERN, PAYMENT_COLS, parse_email  # noqa: F401

__all__ = ["PATTERN", "PAYMENT_COLS", "parse_email", "update_tenant_month_row"]

def _with_backoff(fn, *args, **kwargs):
    delay = 1.0
    for _ in range(6):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            # Respect per-minute limits
            if hasattr(e, "response") and getattr(e.response, "status_code", None) == 429:
                time.sleep(delay)
                delay *= 2
                continue
            raise
    # final attempt (surface any error)
    return fn(*args, **kwargs)

def _find_month_row(ws, month_str, search_rows=2000):
    # Read only column A where Month is stored; avoid full-sheet reads.
    rng = f"A2:A{search_rows}"
    colA = _with_backoff(ws.get, rng) or []
    # Flatten like [["2025-08"], ["2025-09"], ...] -> ["2025-08", "2025-09", ...]
    colA = [r[0] if r else "" for r in colA]
    for i, v in enumerate(colA, start=2):
        if v.strip() == month_str:
            return i
    # Not found -> next empty row after the last non-empty cell
    return len(colA) + 2

def update_tenant_month_row(tenant_ws, payment_dict):
    """Quota-friendly sheet update.
    Writes only the cells we need for the given month row using a single batch update.
    Returns a summary dict used by the Streamlit app for logging.
    Expected columns: A:Month, C:Amount paid, D:Date paid, E:REF Number
    """
    # month key like '2025-08'
    dt = datetime.strptime(payment_dict["Date Paid"], "%d/%m/%Y %I:%M %p")
    month_key = dt.strftime("%Y-%m")

    row = _find_month_row(tenant_ws, month_key)

    # Try to read previous paid value for logging (single-cell read)
    try:
        paid_before = _with_backoff(tenant_ws.acell, f"C{row}").value
    except Exception:
        paid_before = ""

    updates = [
        (f"A{row}", month_key),
        (f"C{row}", payment_dict["Amount Paid"]),
        (f"D{row}", payment_dict["Date Paid"]),
        (f"E{row}", payment_dict["REF Number"]),
    ]

    body = [{"range": rng, "values": [[val]]} for rng, val in updates]
    # One network call, user-entered so numbers/formatting behave like manual entry.
    _with_backoff(
        tenant_ws.spreadsheet.values_batch_update,
        body,
        value_input_option=ValueInputOption.user_entered,
    )

    return {
        "sheet": tenant_ws.title,
        "month_row": row,
        "paid_before": paid_before or "",
        "paid_after": payment_dict["Amount Paid"],
        "ref_added": payment_dict["REF Number"],
        "formulas_set": False,
    }
