# -*- coding: utf-8 -*-
"""
bot_logic.py — RentRPA core logic (HEAVILY COMMENTED)

Purpose
-------
Update a single tenant worksheet in Google Sheets to reflect a parsed payment
coming from Gmail (e.g., NCBA/M-Pesa alerts). This module is designed to be:
- **Header-row aware** (many tabs use row 7 for headers)
- **Non-destructive** (no renaming/deleting user columns; we append missing canonicals to the far right)
- **Placement-correct** (writes inside the existing data block—not to the physical bottom of the sheet)
- **Quota-friendly** (caches header/rows mapping in memory during a run; uses backoff on 429s)
- **Business-rules aligned** (Date due = 5th; Penalty = 3000 KES if paid > due+2 days; rolling Prepayment/Arrears)
- **User-friendly** (never overwrites Comments; provides verbose debug notes)

How it works (high level)
-------------------------
1) We read a tenant worksheet once (per run) and detect which row holds the header.
2) We build a column map by matching existing headers against aliases (no renames).
3) If any canonical columns are missing (e.g., "Prepayment/Arrears"), we append them to the far right.
4) Given a payment (Amount, Date Paid, Ref, etc.), we find the target month row by parsing the Month column.
   If not found, we create a new row exactly under the last data row (not at sheet bottom).
5) We set values and formulas with A1 ranges **relative to this worksheet** (no "'Sheet'!" prefix).
6) We ensure the grid (rows/cols) is big enough before writing to avoid "exceeds grid limits".
7) We apply conditional formatting (once per sheet per run) to highlight arrears/penalties.

You can pass a `debug` list to `update_tenant_month_row(ws, payment, debug)`
to capture step-by-step breadcrumbs for troubleshooting.
"""

from __future__ import annotations
from copy import deepcopy
import re
import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from gspread.utils import rowcol_to_a1
import re as _re

# ---------------------------------------------------------------------------
# 0) PAYMENT PARSER — extract payment fields from an email body
# ---------------------------------------------------------------------------
# Pattern assumes messages like:
# "payment of KES 15,000.00 for account: PAYLEMAIYAN # B3 has been received from John Doe 0712XXXXXX
#  on 14/08/2025 12:04 PM. M-Pesa Ref: ABC123XYZ9"
#
# Tweak this regex if your bank format changes.
PATTERN = re.compile(
    r'payment of KES ([\d,]+\.\d{2}) '
    r'for account: PAYLEMAIYAN\s*#?\s*([A-Za-z]\d{1,2}) '
    r'has been received from (.+?) (.{1,13}) '
    r'on (\d{2}/\d{2}/\d{4} \d{1,2}:\d{2} [APM]{2})\. '
    r'M-Pesa Ref: ([A-Z0-9]{10})',
    re.I
)

# Columns written to the PaymentHistory meta sheet (Streamlit app uses this)
PAYMENT_COLS = ['Date Paid','Amount Paid','REF Number','Payer','Phone','Payment Mode']

def parse_email(text: str):
    """
    Attempt to parse an NCBA/M-Pesa message text into a dict.
    Returns None if not matched (so the caller can skip it).
    """
    m = PATTERN.search(text or "")
    if not m:
        return None
    amt, code, payer, phone, dt, ref = m.groups()
    return {
        'Date Paid': dt.strip(),
        'Amount Paid': float(amt.replace(',', '')),
        'REF Number': ref.upper(),
        'Payer': payer.strip(),
        'Phone': phone.strip(),
        'Payment Mode': 'MPESA Payment',
        'AccountCode': code.upper(),
    }

# ---------------------------------------------------------------------------
# 1) HEADER/SCHEMA DEFINITIONS — canonical columns + alias matching
# ---------------------------------------------------------------------------

# Canonical columns used by the bot (order is not enforced on the sheet)
ALIASES = {
    "month": {"month","month/period","period","rent month","billing month"},
    "amount_due": {"amount due","rent due","due","monthly rent","rent","amount due kes","rent kes"},
    "amount_paid": {"amount paid","paid","amt paid","paid kes","amountpaid"},
    "date_paid": {"date paid","paid date","payment date","datepaid","date of payment"},
    "ref": {"ref number","ref","reference","ref no","reference no","mpesa ref","mpesa reference","receipt","receipt no"},
    "date_due": {"date due","due date","rent due date","datedue"},
    "prepay_arrears": {"prepayment/arrears","prepayment","arrears","balance","bal","prepayment arrears","carry forward","cf"},
    "penalties": {"penalties","penalty","late fee","late fees","fine","fines"},
    "comments": {"comments","comment","remarks","notes","note"},
}
REQUIRED_KEYS = ["month","amount_due","amount_paid","date_paid","ref","date_due","prepay_arrears","penalties","comments"]
CANONICAL_NAME = {
    "month": "Month",
    "amount_due": "Amount Due",
    "amount_paid": "Amount paid",
    "date_paid": "Date paid",
    "ref": "REF Number",
    "date_due": "Date due",
    "prepay_arrears": "Prepayment/Arrears",
    "penalties": "Penalties",
    "comments": "Comments",
}
CANONICAL_SET = set(CANONICAL_NAME.values())

def _norm_header(s: str) -> str:
    """Normalize a header cell for alias matching: lowercase, strip, remove punctuation, collapse spaces."""
    import re as _re
    s = str(s or "").replace("\xa0", " ").strip().lower()
    s = _re.sub(r"[^\w\s/]+", "", s)
    s = _re.sub(r"\s+", " ", s)
    return s

def _alias_key_for(normalized_header: str) -> Optional[str]:
    """Return the canonical key if this normalized header matches any alias set."""
    for key, aliases in ALIASES.items():
        if normalized_header in aliases:
            return key
    return None

def _header_colmap(header: List[str]) -> Dict[str, int]:
    """
    Build a dict mapping canonical keys -> column index for the given header list.
    IMPORTANT: We do **not** rename any header. We only *map* to existing names/aliases.
    """
    colmap: Dict[str, int] = {}
    seen = set()
    for idx, name in enumerate(header):
        name_str = str(name or "")
        norm = _norm_header(name_str)
        if name_str in CANONICAL_SET:
            key = next(k for k, v in CANONICAL_NAME.items() if v == name_str)
        else:
            key = _alias_key_for(norm)
        if key and key not in seen:
            colmap[key] = idx
            seen.add(key)
    return colmap

# ---------------------------------------------------------------------------
# 2) UTILITIES — backoff, grid safety, header detection, month parsing
# ---------------------------------------------------------------------------

def _with_backoff(fn, *args, **kwargs):
    """
    Wrap a Google API call and retry on common quota/rate-limit signals.
    This prevents transient 429s from killing the run.
    """
    delay = 1.0
    for _ in range(6):  # exponential backoff up to ~63s
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            status = getattr(getattr(e, "resp", None), "status", None)
            if status == 429 or "quota" in str(e).lower() or "Rate Limit" in str(e):
                time.sleep(delay); delay *= 2; continue
            raise

def _with_backoff_factory(fn_factory, *, max_tries=6):
    """
    Retry a ZERO-ARG callable factory that returns a fresh operation each attempt.
    Use this when the underlying client MUTATES the input (like gspread Worksheet.batch_update
    prefixing ranges with the sheet name). We rebuild the body every try via the factory.
    """
    delay = 1.0
    for _ in range(max_tries):
        try:
            return fn_factory()   # build fresh call each attempt
        except Exception as e:
            status = getattr(getattr(e, "resp", None), "status", None)
            if status == 429 or "quota" in str(e).lower() or "Rate Limit" in str(e):
                time.sleep(delay); delay *= 2; continue
            raise



def _strip_ws_prefix(a1: str) -> str:
    """
    Remove any leading 'Sheet'! prefix (repeated or nested) from an A1 string.
    We always pass pure A1 to Worksheet.batch_update; gspread will add the sheet title.
    """
    s = str(a1 or "")
    while True:
        m = _re.match(r"^'[^']+'!(.+)$", s)
        if not m: break
        s = m.group(1)
    return s


def _ensure_grid_size(ws, need_rows: int = None, need_cols: int = None):
    """
    Make sure the worksheet has enough rows/cols before writing.
    - Avoids errors like "Range exceeds grid limits".
    - Uses add_rows/add_cols with backoff.
    """
    try:
        cur_rows = ws.row_count
        cur_cols = ws.col_count
        if need_rows is not None and need_rows > cur_rows:
            _with_backoff(ws.add_rows, need_rows - cur_rows)
        if need_cols is not None and need_cols > cur_cols:
            _with_backoff(ws.add_cols, need_cols - cur_cols)
    except Exception:
        # If we can't query row/col_count reliably, we still attempt the write — Sheets often auto-expands.
        pass

def _detect_header_row(all_vals, scan_rows: int = 30) -> int:
    """
    Heuristically detect which row contains the header by scanning the first few rows.
    Returns a 0-based index. In your layout, visible row 7 corresponds to index 6.
    """
    def score(row):
        s = 0
        for cell in row:
            if not cell: continue
            txt = str(cell).strip()
            if txt in CANONICAL_SET or _alias_key_for(_norm_header(txt)):
                s += 1
        return s

    best_i, best_score = 0, 0
    limit = min(len(all_vals), max(1, scan_rows))
    for i in range(limit):
        sc = score(all_vals[i])
        if sc > best_score:
            best_i, best_score = i, sc
    return best_i if best_score >= 3 else 0

def _parse_month_cell(s: str) -> Optional[Tuple[int,int]]:
    """
    Parse a Month cell into (year, month) if possible.
    Accepts formats like 'Aug-2025', 'August 2025', '2025-08', '08/2025'.
    """
    if not s: return None
    s = str(s).strip()
    m = re.match(r"^([A-Za-z]{3,9})[- ](\d{4})$", s)
    if m:
        name, year = m.group(1), int(m.group(2))
        for fmt in ("%b", "%B"):
            try:
                dt = datetime.strptime(f"01 {name} {year}", f"%d {fmt} %Y")
                return (dt.year, dt.month)
            except ValueError:
                pass
    m = re.match(r"^(\d{4})[-/](\d{1,2})$", s)
    if m: return (int(m.group(1)), int(m.group(2)))
    m = re.match(r"^(\d{1,2})[-/](\d{4})$", s)
    if m: return (int(m.group(2)), int(m.group(1)))
    return None

def _choose_month_display(existing_samples: List[str], dt: datetime) -> str:
    """
    Pick a Month display that matches the current sheet style.
    Fallback to 'Mon-YYYY' if we can't infer a style.
    """
    for v in existing_samples:
        t = (v or "").strip()
        if not t: continue
        if re.fullmatch(r"\d{4}[-/]\d{2}", t):
            return f"{dt.year}-{dt.month:02d}"
        if re.fullmatch(r"[A-Za-z]{3,9}[- ]\d{4}", t):
            return dt.strftime("%b-%Y")
        if re.fullmatch(r"\d{2}[-/]\d{4}", t):
            return f"{dt.month:02d}-{dt.year}"
    return dt.strftime("%b-%Y")

# ---------------------------------------------------------------------------
# 3) CONDITIONAL FORMATTING — highlight arrears/penalties
# ---------------------------------------------------------------------------

def _ensure_conditional_formatting(ws, header_row0: int, colmap: Dict[str,int], cache: Dict, debug: Optional[List[str]] = None):
    """
    Adds two rules once per sheet per run:
      - Arrears < 0 → light red background
      - Penalties > 0 → light yellow background
    """
    if cache.get("cf_applied"):  # already applied this run
        return
    try:
        sheet_id = ws.id
    except Exception:
        return

    start_row = header_row0 + 1  # first data row (0-based header)
    requests = [
        {   # Arrears < 0
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{
                        "sheetId": sheet_id,
                        "startRowIndex": start_row,
                        "startColumnIndex": colmap["prepay_arrears"],
                        "endColumnIndex": colmap["prepay_arrears"] + 1
                    }],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]},
                        "format": {"backgroundColor": {"red": 1.0, "green": 0.84, "blue": 0.84}}
                    }
                },
                "index": 0
            }
        },
        {   # Penalties > 0
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{
                        "sheetId": sheet_id,
                        "startRowIndex": start_row,
                        "startColumnIndex": colmap["penalties"],
                        "endColumnIndex": colmap["penalties"] + 1
                    }],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]},
                        "format": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 0.6}}
                    }
                },
                "index": 1
            }
        },
    ]
    try:
        ws.spreadsheet.batch_update({"requests": requests})
        cache["cf_applied"] = True
        if debug is not None:
            debug.append("Applied conditional formatting (arrears<0, penalties>0).")
    except Exception as e:
        if debug is not None:
            debug.append(f"Conditional formatting error/skip: {e}")

# ---------------------------------------------------------------------------
# 4) PER-SHEET CACHE — avoids repeated reads during one run
# ---------------------------------------------------------------------------

_sheet_cache: Dict[str, Dict] = {}

def clear_cache():
    """Clear in-memory cache. Call at the start of a run to avoid stale header/col counts."""
    _sheet_cache.clear()

# ---------------------------------------------------------------------------
# 5) MAIN FUNCTION — update a tenant tab for a given payment
# ---------------------------------------------------------------------------

def update_tenant_month_row(ws, payment: Dict, debug: Optional[List[str]] = None) -> Dict:
    """
    Update a single tenant sheet with one payment.

    Args:
        ws      : gspread.Worksheet representing the tenant tab
        payment : dict from parse_email(text)
        debug   : optional list to collect verbose debug notes

    Returns:
        dict: details about the write: sheet, row, month, paid_before, paid_after, date_due, etc.
    """
    title = ws.title
    if debug is not None:
        debug.append(f"[{title}] Start → REF={payment.get('REF Number')} Amount={payment.get('Amount Paid')} DatePaid={payment.get('Date Paid')}")

    # ---- 1) Prime cache (read once per sheet per run) ----
    if title not in _sheet_cache:
        all_vals = _with_backoff(ws.get_all_values)  # ONE READ
        header_row0 = _detect_header_row(all_vals)   # 0-based (visible row7 => index=6)
        header = list(all_vals[header_row0]) if len(all_vals) > header_row0 else []
        rows   = [list(r) for r in all_vals[header_row0+1:]] if len(all_vals) > header_row0+1 else []
        colmap = _header_colmap(header)

        # Append missing canonical columns to FAR RIGHT (no renames)
        added_any = False
        for key in REQUIRED_KEYS:
            if key not in colmap:
                header.append(CANONICAL_NAME[key])
                for r in rows:
                    r.append("None" if key == "comments" else "")
                added_any = True

        # Ensure grid and write header back to the *actual* header row only
        _ensure_grid_size(ws, need_rows=header_row0+1, need_cols=len(header))
        _with_backoff(ws.update, f"{header_row0+1}:{header_row0+1}", [header], value_input_option='USER_ENTERED')

        cache = {
            "header_row0": header_row0,
            "header": header,
            "rows": rows,
            "colmap": _header_colmap(header),
            "cf_applied": False,
        }
        _sheet_cache[title] = cache
        if debug is not None:
            debug.append(f"[{title}] Header at visible row {header_row0+1}; columns={len(header)}; added_any={added_any}")
    else:
        cache = _sheet_cache[title]

    header_row0  = cache["header_row0"]
    header       = cache["header"]
    rows         = cache["rows"]
    colmap       = cache["colmap"]

    # ---- 2) Resolve the target month from payment date ----
    dt_paid = datetime.strptime(payment['Date Paid'], '%d/%m/%Y %I:%M %p')
    target_y, target_m = dt_paid.year, dt_paid.month
    existing_month_samples = [r[colmap['month']] for r in rows if len(r) > colmap['month'] and r[colmap['month']]]
    month_display = _choose_month_display(existing_month_samples, dt_paid)

    # ---- 3) Find (or create) the month row inside the data block ----
    row_abs = None    # 1-based absolute row in sheet
    row_idx = None    # 0-based index into rows[]
    for idx_in_rows, r in enumerate(rows, start=1):  # first data row is +1 after header
        if len(r) <= colmap['month']:
            continue
        ym = _parse_month_cell(r[colmap['month']])
        if ym and ym == (target_y, target_m):
            row_idx = idx_in_rows - 1
            row_abs = header_row0 + 1 + idx_in_rows
            break

    if row_abs is None:
        # Not found → create a new row at the next line under header (in the data block)
        last_due = "0"
        for r in reversed(rows):
            if len(r) > colmap['amount_due'] and str(r[colmap['amount_due']]).strip():
                last_due = r[colmap['amount_due']]
                break

        new_row = [""] * len(header)
        new_row[colmap['month']]      = month_display
        new_row[colmap['amount_due']] = last_due
        if "comments" in colmap:
            new_row[colmap['comments']] = "None"

        rows.append(new_row)
        row_idx = len(rows) - 1
        row_abs = header_row0 + 1 + (row_idx + 1)

        _ensure_grid_size(ws, need_rows=row_abs, need_cols=len(header))
        _with_backoff(ws.update, f"{row_abs}:{row_abs}", [new_row], value_input_option='USER_ENTERED')
        if debug is not None:
            debug.append(f"[{title}] Created month row at R{row_abs}; carry-forward Amount Due = {last_due}")

    # ---- 4) Set values + formulas for that row ----
    row_vals = rows[row_idx]
    while len(row_vals) < len(header):
        row_vals.append("")

    def _num(v):
        try: return float(str(v).replace(",", "").strip() or 0)
        except Exception: return 0.0

    paid_before = _num(row_vals[colmap['amount_paid']])
    paid_after  = paid_before + float(payment['Amount Paid'])
    prev_ref    = row_vals[colmap['ref']] or ""
    ref_new     = payment['REF Number'] if not prev_ref else f"{prev_ref}, {payment['REF Number']}"
    due_str     = datetime(target_y, target_m, 5).strftime("%d/%m/%Y")

    # Update our cached copy for this row (the sheet write is below)
    row_vals[colmap['month']]       = month_display
    row_vals[colmap['amount_paid']] = str(paid_after)
    row_vals[colmap['date_paid']]   = payment['Date Paid']
    row_vals[colmap['ref']]         = ref_new
    row_vals[colmap['date_due']]    = due_str

    # Build A1 references for formulas
    amt_paid_addr  = rowcol_to_a1(row_abs, colmap['amount_paid']+1)
    amt_due_addr   = rowcol_to_a1(row_abs, colmap['amount_due']+1)
    date_paid_addr = rowcol_to_a1(row_abs, colmap['date_paid']+1)
    date_due_addr  = rowcol_to_a1(row_abs, colmap['date_due']+1)
    pen_addr       = rowcol_to_a1(row_abs, colmap['penalties']+1)
    bal_addr       = rowcol_to_a1(row_abs, colmap['prepay_arrears']+1)
    prev_bal_addr  = rowcol_to_a1(row_abs-1, colmap['prepay_arrears']+1)

    # Coerce text dates to real dates when needed.
    # DATEVALUE(LEFT(DatePaid,10)) expects dd/mm/YYYY when user types text in the cell.
    dpaid_expr  = f"IF(ISTEXT({date_paid_addr}), DATEVALUE(LEFT({date_paid_addr},10)), {date_paid_addr})"
    ddue_expr   = f"IF(ISTEXT({date_due_addr}),  DATEVALUE({date_due_addr}),           {date_due_addr})"
    pen_formula = f"=IF(AND(ISNUMBER({dpaid_expr}), ISNUMBER({ddue_expr}), {dpaid_expr}>{ddue_expr}+2),3000,0)"

    if row_abs == header_row0 + 2:
        # First data row → no previous balance available
        bal_formula = f"=N({amt_paid_addr})-N({amt_due_addr})-N({pen_addr})"
    else:
        bal_formula = f"=N({prev_bal_addr})+N({amt_paid_addr})-N({amt_due_addr})-N({pen_addr})"

    # Ensure grid wide enough before batch update
    _target_cols = [
        colmap['month']+1, colmap['amount_paid']+1, colmap['date_paid']+1,
        colmap['ref']+1, colmap['date_due']+1, colmap['penalties']+1, colmap['prepay_arrears']+1
    ]
    _ensure_grid_size(ws, need_rows=row_abs, need_cols=max(_target_cols))

    # IMPORTANT: Ranges are plain A1 (e.g., "B21") — do NOT prefix with "'Sheet'!"
    updates = [
        {"range": _strip_ws_prefix(rowcol_to_a1(row_abs, colmap['month']+1)),          "values": [[month_display]]},
        {"range": _strip_ws_prefix(rowcol_to_a1(row_abs, colmap['amount_paid']+1)),    "values": [[paid_after]]},
        {"range": _strip_ws_prefix(rowcol_to_a1(row_abs, colmap['date_paid']+1)),      "values": [[payment['Date Paid']]]},
        {"range": _strip_ws_prefix(rowcol_to_a1(row_abs, colmap['ref']+1)),            "values": [[ref_new]]},
        {"range": _strip_ws_prefix(rowcol_to_a1(row_abs, colmap['date_due']+1)),       "values": [[due_str]]},
        {"range": _strip_ws_prefix(rowcol_to_a1(row_abs, colmap['penalties']+1)),      "values": [[pen_formula]]},
        {"range": _strip_ws_prefix(rowcol_to_a1(row_abs, colmap['prepay_arrears']+1)), "values": [[bal_formula]]},
    ]
    _with_backoff_factory(lambda: ws.batch_update(deepcopy(updates), value_input_option='USER_ENTERED'))
        
    if debug is not None:
        debug.append(f"[{title}] Wrote row R{row_abs}: paid {paid_before}→{paid_after}, due={due_str}")

    # Apply CF once per sheet per run
    _ensure_conditional_formatting(ws, header_row0, colmap, cache, debug)

    return {
        "sheet": ws.title,
        "row": row_abs,
        "month": month_display,
        "paid_before": paid_before,
        "paid_after": paid_after,
        "date_due": due_str,
        # legacy-friendly keys:
        "month_row": row_abs,
        "ref_added": payment.get("REF Number"),
        "formulas_set": {"balance": True, "penalties": True},
    }
