# -*- coding: utf-8 -*-
"""
bot_logic.py — non-destructive, header-row aware updater for RentRPA

What it does:
- Detects the actual header row per sheet (row 7 in your layout, or any other).
- Maps existing headers via aliases (NO renaming, NO deletions).
- Appends missing canonical columns at the far right (Comments included).
- Writes inside the existing data block (right under your history), not at sheet bottom.
- Carries forward "Amount Due"; Date due = 5th (value).
- Robust formulas for Penalties + Prepayment/Arrears (work with text or serial dates).
- Conditional formatting: arrears<0 light red; penalties>0 light yellow.
- Expands grid before writes to avoid "exceeds grid limits".
- Per-sheet in-memory cache to reduce API reads.
- Return keys include both new + backward-compat fields.
"""

from __future__ import annotations
import re, time
from typing import Dict, List, Optional, Tuple
from datetime import datetime
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
    """Map canonical keys → column index from a concrete header row (no renames)."""
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

def _ensure_grid_size(ws, need_rows: int = None, need_cols: int = None):
    """Make sure the worksheet has at least the requested rows/cols before writes."""
    try:
        cur_rows = ws.row_count
        cur_cols = ws.col_count
        if need_rows is not None and need_rows > cur_rows:
            _with_backoff(ws.add_rows, need_rows - cur_rows)
        if need_cols is not None and need_cols > cur_cols:
            _with_backoff(ws.add_cols, need_cols - cur_cols)
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
    start_row = header_row0 + 1  # 0-based: first data row
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

# ---------------- Header row detection & normalization ----------------

def _detect_header_row(all_vals, scan_rows: int = 30) -> int:
    """Return 0-based index of the header row by scoring the first `scan_rows` rows."""
    def score(row):
        s = 0
        for cell in row:
            txt = str(cell or "").strip()
            if not txt:
                continue
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

def _normalize_header_non_destructive(ws, header: List[str], rows: List[List[str]]):
    """
    - DO NOT rename existing headers.
    - Map via aliases; only append truly missing canonical columns at FAR RIGHT.
    - Returns (header, rows, colmap, comments_added_now).
    """
    comments_added_now = False
    colmap_existing = _header_colmap(header)

    added_any = False
    for key in REQUIRED_KEYS:
        if key not in colmap_existing:
            header.append(CANONICAL_NAME[key])
            for r in rows:
                r.append("None" if key == "comments" else "")
            added_any = True
            if key == "comments":
                comments_added_now = True

    try:
        if added_any and ws.col_count < len(header):
            _with_backoff(ws.add_cols, len(header) - ws.col_count)
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
    Update a tenant worksheet for a payment dict, writing inside the existing data block.

    Steps:
      - Read once (per sheet/run), detect header row, non-destructive header normalization.
      - Find existing month row; else create a new row at the next data line (NOT bottom-of-sheet).
      - Update values and formulas; ensure grid; apply CF once per run.
    """
    title = ws.title

    # Prime cache (read once per sheet/run)
    if title not in _sheet_cache:
        all_vals = _with_backoff(ws.get_all_values)  # one read
        header_row0 = _detect_header_row(all_vals)   # 0-based (6 for visible row 7)
        header = list(all_vals[header_row0]) if len(all_vals) > header_row0 else []
        rows   = [list(r) for r in all_vals[header_row0+1:]] if len(all_vals) > header_row0+1 else []

        header, rows, colmap, _ = _normalize_header_non_destructive(ws, header, rows)

        # Ensure grid & write header back to the ACTUAL header row (not row 1)
        _ensure_grid_size(ws, need_rows=header_row0+1, need_cols=len(header))
        header_a1 = f"{header_row0+1}:{header_row0+1}"
        _with_backoff(ws.update, header_a1, [header])

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

    # Target month from payment date
    dt_paid = datetime.strptime(payment['Date Paid'], '%d/%m/%Y %I:%M %p')
    target_y, target_m = dt_paid.year, dt_paid.month
    existing_month_samples = [r[colmap['month']] for r in rows if len(r) > colmap['month'] and r[colmap['month']]]
    month_display = _choose_month_display(existing_month_samples, dt_paid)

    # 1) Find existing month row within the data block
    row_abs = None
    row_idx = None  # 0-based within rows[]
    for idx_in_rows, r in enumerate(rows, start=1):  # first data row is +1 after header
        if len(r) <= colmap['month']:
            continue
        ym = _parse_month_cell(r[colmap['month']])
        if ym and ym == (target_y, target_m):
            row_idx = idx_in_rows - 1
            row_abs = header_row0 + 1 + idx_in_rows  # absolute 1-based row in the sheet
            break

    # 2) If not found, create a NEW row at the next line of the data block (NOT bottom of sheet)
    if row_abs is None:
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

        # in-memory append
        rows.append(new_row)
        row_idx = len(rows) - 1
        row_abs = header_row0 + 1 + (row_idx + 1)

        # ensure row exists and is wide enough, then write the full row in place
        _ensure_grid_size(ws, need_rows=row_abs, need_cols=len(header))
        _with_backoff(ws.update, f"{row_abs}:{row_abs}", [new_row], value_input_option='USER_ENTERED')

    # 3) Update values (do NOT touch Comments if user set one)
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

    row_vals[colmap['month']]       = month_display
    row_vals[colmap['amount_paid']] = str(paid_after)
    row_vals[colmap['date_paid']]   = payment['Date Paid']
    row_vals[colmap['ref']]         = ref_new
    row_vals[colmap['date_due']]    = due_str

    # 4) Formulas (robust to text/serial dates)
    amt_paid_addr  = rowcol_to_a1(row_abs, colmap['amount_paid']+1)
    amt_due_addr   = rowcol_to_a1(row_abs, colmap['amount_due']+1)
    date_paid_addr = rowcol_to_a1(row_abs, colmap['date_paid']+1)
    date_due_addr  = rowcol_to_a1(row_abs, colmap['date_due']+1)
    pen_addr       = rowcol_to_a1(row_abs, colmap['penalties']+1)
    prev_bal_addr  = rowcol_to_a1(row_abs-1, colmap['prepay_arrears']+1)

    dpaid_expr  = f"IF(ISTEXT({date_paid_addr}), DATEVALUE(LEFT({date_paid_addr},10)), {date_paid_addr})"
    ddue_expr   = f"IF(ISTEXT({date_due_addr}),  DATEVALUE({date_due_addr}),           {date_due_addr})"
    pen_formula = f"=IF(AND(ISNUMBER({dpaid_expr}), ISNUMBER({ddue_expr}), {dpaid_expr}>{ddue_expr}+2),3000,0)"

    # Rolling balance (Prepayment/Arrears)
    if row_abs == _sheet_cache[title]["header_row0"] + 2:
        bal_formula = f"=N({amt_paid_addr})-N({amt_due_addr})-N({pen_addr})"
    else:
        bal_formula = f"=N({prev_bal_addr})+N({amt_paid_addr})-N({amt_due_addr})-N({pen_addr})"

    # 5) Single batch write to the exact row/cols (ensure grid first)
    _target_cols = [
        colmap['month']+1, colmap['amount_paid']+1, colmap['date_paid']+1,
        colmap['ref']+1, colmap['date_due']+1, colmap['penalties']+1, colmap['prepay_arrears']+1
    ]
    _ensure_grid_size(ws, need_rows=row_abs, need_cols=max(_target_cols))
    updates = [
        {"range": rowcol_to_a1(row_abs, colmap['month']+1),         "values": [[month_display]]},
        {"range": rowcol_to_a1(row_abs, colmap['amount_paid']+1),   "values": [[paid_after]]},
        {"range": rowcol_to_a1(row_abs, colmap['date_paid']+1),     "values": [[payment['Date Paid']]]},
        {"range": rowcol_to_a1(row_abs, colmap['ref']+1),           "values": [[ref_new]]},
        {"range": rowcol_to_a1(row_abs, colmap['date_due']+1),      "values": [[due_str]]},
        {"range": rowcol_to_a1(row_abs, colmap['penalties']+1),     "values": [[pen_formula]]},
        {"range": rowcol_to_a1(row_abs, colmap['prepay_arrears']+1),"values": [[bal_formula]]},
    ]
    _with_backoff(ws.batch_update, updates, value_input_option='USER_ENTERED')

    # 6) Conditional formatting (once per run per sheet)
    _ensure_conditional_formatting(ws, _sheet_cache[title]["header_row0"], colmap, _sheet_cache[title])

    return {
        "sheet": ws.title,
        "row": row_abs,
        "month": month_display,
        "paid_before": paid_before,
        "paid_after": paid_after,
        "date_due": due_str,
        # Back-compat
        "month_row": row_abs,
        "ref_added": payment.get("REF Number"),
        "formulas_set": {"balance": True, "penalties": True},
    }
