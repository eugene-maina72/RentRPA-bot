# -*- coding: utf-8 -*-
"""
bot_logic.py — quota-friendly, non-destructive updater for RentRAP

What it does:
- NORMALIZES headers by renaming recognized columns; never deletes user data.
- APPENDS any missing canonical columns at the far right (including "Comments").
- NEW rows: carry forward last "Amount Due"; always set "Date due" to 5th; default Comments="None".
- Robust "Penalties" (late > 2 days -> 3000) & rolling "Prepayment/Arrears" formulas (work with text or true date cells).
- Adds conditional formatting: arrears<0 (light red), penalties>0 (light yellow).
- Per-sheet in-memory cache + batch writes to minimize API reads.
- Returns keys compatible with older streamlit code: month_row, ref_added, formulas_set, etc.
"""

from __future__ import annotations
import re, time
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from googleapiclient.errors import HttpError
from gspread.utils import rowcol_to_a1

# ---------------- Parsing (M-Pesa emails) ----------------

MAX_PHONE_LEN = 13
REF_LEN = 10

PATTERN = re.compile(
    rf'payment of KES ([\d,]+\.\d{{2}}) '
    rf'for account: PAYLEMAIYAN\s*#?\s*([A-Za-z]\d{{1,2}})'
    rf' has been received from (.+?) '
    rf'(.{{1,{MAX_PHONE_LEN}}}) '
    rf'on (\d{{2}}/\d{{2}}/\d{{4}} \d{{1,2}}:\d{{2}} [APM]{{2}})\. '
    rf'M-Pesa Ref: ([A-Z0-9]{{{REF_LEN}}})',
    flags=re.IGNORECASE
)

PAYMENT_COLS = ['Date Paid','Amount Paid','REF Number','Payer','Phone','Payment Mode']

def parse_email(text: str):
    m = PATTERN.search(text or "")
    if not m: return None
    amt, code, payer, phone, dt, ref = m.groups()
    return {
        'Date Paid': dt.strip(),
        'Amount Paid': float(amt.replace(',', '')),
        'REF Number': ref.upper(),
        'Payer': payer.strip(),
        'Phone': phone.strip(),
        'Payment Mode':'MPESA Payment',
        'AccountCode': code.upper(),
    }

# ---------------- Header/schema helpers ----------------

ALIASES = {
    "month": {"month","month/period","period","rent month","billing month"},
    "amount_due": {"amount due","rent due","due","monthly rent","rent","amount due kes","rent kes"},
    "amount_paid": {"amount paid","paid","amt paid","paid kes","amountpaid"},
    "date_paid": {"date paid","paid date","payment date","datepaid"},
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
CANONICAL_SET = {CANONICAL_NAME[k] for k in REQUIRED_KEYS}

def _norm_header(s: str) -> str:
    s = str(s or "").replace("\xa0", " ").strip().lower()
    s = re.sub(r"[^\w\s/]+", "", s)
    s = re.sub(r"\s+", " ", s)
    return s

def _alias_key_for(normalized_header: str) -> Optional[str]:
    for key, aliases in ALIASES.items():
        if normalized_header in aliases:
            return key
    return None

def _header_colmap(header: List[str]) -> Dict[str, int]:
    """Map canonical keys → column index from a concrete header row."""
    colmap: Dict[str,int] = {}
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

def _with_backoff(fn, *args, **kwargs):
    delay = 1.0
    for _ in range(6):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            status = getattr(getattr(e, "resp", None), "status", None)
            if status == 429 or "quota" in str(e).lower():
                time.sleep(delay); delay *= 2; continue
            raise

# --- Header row detection -----------------------------------------------------

def _detect_header_row(all_vals, scan_rows: int = 30) -> int:
    """
    Return the 0-based index of the header row by scoring the first `scan_rows`
    rows for known/canonical/aliased column names. Falls back to row 0.
    """
    def score(row):
        s = 0
        for cell in row:
            txt = str(cell or "").strip()
            if not txt:
                continue
            if txt in CANONICAL_SET:
                s += 1
            else:
                if _alias_key_for(_norm_header(txt)):
                    s += 1
        return s

    best_i, best_score = 0, 0
    limit = min(len(all_vals), max(1, scan_rows))
    for i in range(limit):
        sc = score(all_vals[i])
        if sc > best_score:
            best_i, best_score = i, sc
    # require at least a few matches to accept; otherwise assume first row
    return best_i if best_score >= 3 else 0


def _ensure_grid_size(ws, need_rows: int = None, need_cols: int = None):
    """Make sure the worksheet has at least the requested rows/cols before writes."""
    try:
        # Refresh counts (these can be stale on long-lived objects)
        cur_rows = ws.row_count
        cur_cols = ws.col_count

        if need_rows is not None and need_rows > cur_rows:
            _with_backoff(ws.add_rows, need_rows - cur_rows)
        if need_cols is not None and need_cols > cur_cols:
            _with_backoff(ws.add_cols, need_cols - cur_cols)
    except Exception:
        # As a fallback, attempt again with fresh counts
        try:
            cur_rows = ws.row_count
            cur_cols = ws.col_count
            if need_rows is not None and need_rows > cur_rows:
                ws.add_rows(need_rows - cur_rows)
            if need_cols is not None and need_cols > cur_cols:
                ws.add_cols(need_cols - cur_cols)
        except Exception:
            pass


# ---------------- Month parsing/formatting ----------------

def _parse_month_cell(s: str) -> Optional[Tuple[int,int]]:
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
    for v in existing_samples:
        t = (v or "").strip()
        if not t: continue
        if re.fullmatch(r"\d{4}[-/]\d{1,2}", t):
            delim = '-' if '-' in t else '/'
            return f"{dt.year}{delim}{dt.month:02d}"
        if re.fullmatch(r"[A-Za-z]{3,9}[- ]\d{4}", t):
            delim = '-' if '-' in t else ' '
            token = t.split(delim)[0]
            fmt = '%b' if len(token) == 3 else '%B'
            return f"{dt.strftime(fmt)}{delim}{dt.year}"
        if re.fullmatch(r"\d{1,2}[-/]\d{4}", t):
            delim = '-' if '-' in t else '/'
            return f"{dt.month:02d}{delim}{dt.year}"
    return dt.strftime("%b-%Y")

# ---------------- Conditional formatting ----------------

def _ensure_conditional_formatting(ws, header_row0: int, colmap: Dict[str,int], cache: Dict):
    if cache.get("cf_applied"): return
    try:
        sheet_id = ws.id
    except Exception:
        return
    start_row = header_row0 + 1
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
    except Exception:
        pass

# ---------------- Non-destructive header normalization ----------------

def _normalize_header_non_destructive(ws, header: List[str], rows: List[List[str]]) -> Tuple[List[str], List[List[str]], Dict[str,int], bool]:
    """
    - Renames recognized headers to canonical names (no deletions).
    - Appends missing canonical columns at the FAR RIGHT.
    - Returns (header, rows, colmap, comments_was_added_now).
    """
    comments_added_now = False

    # rename recognized headers in-place
    for i, name in enumerate(header):
        if name in CANONICAL_SET:
            continue
        alias = _alias_key_for(_norm_header(name))
        if alias:
            header[i] = CANONICAL_NAME[alias]

    # append missing canonical columns on the RIGHT
    present = set(header)
    for key in REQUIRED_KEYS:
        canon = CANONICAL_NAME[key]
        if canon not in present:
            header.append(canon)
            present.add(canon)
            if key == "comments":
                comments_added_now = True
                # DO NOT backfill existing rows here (we keep existing data untouched)
                # Only new rows will default to "None"
            # pad existing rows length if needed when we later write/appends

    # make sure sheet has enough columns to display header (no-op if already enough)
    try:
        if ws.col_count < len(header):
            ws.add_cols(len(header) - ws.col_count)
    except Exception:
        pass

    colmap = _header_colmap(header)
    return header, rows, colmap, comments_added_now

# ---------------- Cache ----------------

_sheet_cache: Dict[str, Dict] = {}

def clear_cache():
    _sheet_cache.clear()

# ---------------- Main updater ----------------

def update_tenant_month_row(ws, payment: Dict) -> Dict:
    """
    Update a tenant worksheet for a payment dict.

    Non-destructive + quota-friendly:
      - Read whole sheet once (first time per sheet per run) → cache {header, rows, colmap}.
      - Normalize header ONLY by renaming and appending; NO deletions of user columns.
      - Find-or-create month row; update values; write with a single batch_update.
      - Apply CF rules once per run per sheet.
    """
    title = ws.title

    # Prime cache (read once per sheet/run)
    if title not in _sheet_cache:
        all_vals = _with_backoff(ws.get_all_values)  # one read
        header_row0 = _detect_header_row(all_vals)   # <-- auto-detect (row 6 for row 7 headers)
        header = list(all_vals[header_row0]) if len(all_vals) > header_row0 else []
        rows   = [list(r) for r in all_vals[header_row0+1:]] if len(all_vals) > header_row0+1 else []

        # Non-destructive normalization: rename recognized headers, append missing canonicals at FAR RIGHT
        header, rows, colmap, comments_added_now = _normalize_header_non_destructive(ws, header, rows)

        # Make sure grid can fit that header row
        _ensure_grid_size(ws, need_rows=header_row0+1, need_cols=len(header))

        # Write back header to *that* row only (not row 1!)
        header_a1 = f"{header_row0+1}:{header_row0+1}"
        _with_backoff(ws.update, header_a1, [header])

        # Cache
        _sheet_cache[title] = {
            "header_row0": header_row0,
            "header": header,
            "rows": rows,
            "colmap": colmap,
            "cf_applied": False,
        }

    cache        = _sheet_cache[title]
    header_row0  = cache["header_row0"]
    header       = cache["header"]
    rows         = cache["rows"]
    colmap       = cache["colmap"]


    # Parse payment date → target month
    dt_paid = datetime.strptime(payment['Date Paid'], '%d/%m/%Y %I:%M %p')
    target_y, target_m = dt_paid.year, dt_paid.month
    existing_month_samples = [r[colmap['month']] for r in rows if len(r) > colmap['month'] and r[colmap['month']]]
    month_display = _choose_month_display(existing_month_samples, dt_paid)

    # Find month row by parsing month cells (handles mixed formats)
    row_abs = None
    for i, r in enumerate(rows, start=2):
        if len(r) <= colmap['month']: continue
        ym = _parse_month_cell(r[colmap['month']])
        if ym and ym == (target_y, target_m):
            row_abs = i; break

    # If not found, create new row (default Comments = "None")
    if row_abs is None:
        # carry forward Amount Due from last non-empty row
        last_due = "0"
        for r in reversed(rows):
            if len(r) > colmap['amount_due'] and str(r[colmap['amount_due']]).strip():
                last_due = r[colmap['amount_due']]; break
        new_row = [""] * len(header)
        new_row[colmap['month']] = month_display
        new_row[colmap['amount_due']] = last_due
        if "comments" in colmap:
            new_row[colmap['comments']] = "None"
        rows.append(new_row)

        # Ensure we have enough columns to hold the entire row we’re appending
        _ensure_grid_size(ws, need_rows=ws.row_count + 1, need_cols=len(header))

        _with_backoff(ws.append_row, new_row, value_input_option='USER_ENTERED')
        row_abs = len(rows) + 1  # header + rows


    # Work with cached row
    row_idx = row_abs - 2
    if row_idx >= len(rows):
        rows.extend([[""]*len(header) for _ in range(row_idx - len(rows) + 1)])
    row_vals = rows[row_idx]
    while len(row_vals) < len(header): row_vals.append("")

    def _num(v):
        try: return float(str(v).replace(",", "").strip() or 0)
        except Exception: return 0.0

    paid_before = _num(row_vals[colmap['amount_paid']])
    paid_after  = paid_before + float(payment['Amount Paid'])
    prev_ref    = row_vals[colmap['ref']] or ""
    ref_new     = payment['REF Number'] if not prev_ref else f"{prev_ref}, {payment['REF Number']}"
    due_str     = datetime(target_y, target_m, 5).strftime("%d/%m/%Y")

    # Update cached row (do NOT touch Comments if user has one)
    row_vals[colmap['month']]       = month_display
    row_vals[colmap['amount_paid']] = str(paid_after)
    row_vals[colmap['date_paid']]   = payment['Date Paid']
    row_vals[colmap['ref']]         = ref_new
    row_vals[colmap['date_due']]    = due_str

    # Build formulas (robust to text/serial dates)
    amt_paid_addr  = rowcol_to_a1(row_abs, colmap['amount_paid']+1)
    amt_due_addr   = rowcol_to_a1(row_abs, colmap['amount_due']+1)
    date_paid_addr = rowcol_to_a1(row_abs, colmap['date_paid']+1)
    date_due_addr  = rowcol_to_a1(row_abs, colmap['date_due']+1)
    pen_addr       = rowcol_to_a1(row_abs, colmap['penalties']+1)
    bal_addr       = rowcol_to_a1(row_abs, colmap['prepay_arrears']+1)
    prev_bal_addr  = rowcol_to_a1(row_abs-1, colmap['prepay_arrears']+1)

    dpaid_expr  = f"IF(ISTEXT({date_paid_addr}), DATEVALUE(LEFT({date_paid_addr},10)), {date_paid_addr})"
    ddue_expr   = f"IF(ISTEXT({date_due_addr}),  DATEVALUE({date_due_addr}),           {date_due_addr})"
    pen_formula = f"=IF(AND(ISNUMBER({dpaid_expr}), ISNUMBER({ddue_expr}), {dpaid_expr}>{ddue_expr}+2),3000,0)"

    if row_abs == cache["header_row0"] + 2:
        bal_formula = f"=N({amt_paid_addr})-N({amt_due_addr})-N({pen_addr})"
    else:
        bal_formula = f"=N({prev_bal_addr})+N({amt_paid_addr})-N({amt_due_addr})-N({pen_addr})"

    # Single batch write for values + formulas
    updates = [
        {"range": rowcol_to_a1(row_abs, colmap['month']+1),         "values": [[month_display]]},
        {"range": rowcol_to_a1(row_abs, colmap['amount_paid']+1),   "values": [[paid_after]]},
        {"range": rowcol_to_a1(row_abs, colmap['date_paid']+1),     "values": [[payment['Date Paid']]]},
        {"range": rowcol_to_a1(row_abs, colmap['ref']+1),           "values": [[ref_new]]},
        {"range": rowcol_to_a1(row_abs, colmap['date_due']+1),      "values": [[due_str]]},
        {"range": rowcol_to_a1(row_abs, colmap['penalties']+1),     "values": [[pen_formula]]},
        {"range": rowcol_to_a1(row_abs, colmap['prepay_arrears']+1),"values": [[bal_formula]]},
    ]
    # Max column index we’re about to touch (1-based)
    _target_cols = [
        colmap['month']+1,
        colmap['amount_paid']+1,
        colmap['date_paid']+1,
        colmap['ref']+1,
        colmap['date_due']+1,
        colmap['penalties']+1,
        colmap['prepay_arrears']+1,
        ]
    _ensure_grid_size(ws, need_rows=row_abs, need_cols=max(_target_cols))


    _with_backoff(ws.batch_update, updates, value_input_option='USER_ENTERED')

    # Ensure CF rules exist (once per run per sheet)
    _ensure_conditional_formatting(ws, cache["header_row0"], colmap, cache)

    return {
        "sheet": ws.title,
        "row": row_abs,                  # for new code
        "month": month_display,          # for new code
        "paid_before": paid_before,
        "paid_after": paid_after,
        "date_due": due_str,
        # Backwards-compat fields expected by older streamlit_app.py:
        "month_row": row_abs,
        "ref_added": payment.get("REF Number"),
        "formulas_set": {"balance": True, "penalties": True},
    }
