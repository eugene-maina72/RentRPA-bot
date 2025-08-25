
# -*- coding: utf-8 -*-
"""
bot_logic.py — quota-friendly, production-hardened updater for RentRAP

Features:
- Canonical headers only (drops junk like "Column 1/2/3…" and unknown headers)
- Adds missing canonical columns (including "Comments")
- Carry-forward "Amount Due" (if user changes rent in the sheet, that becomes baseline going forward)
- "Date due" always = 5th of the month (value, not a formula)
- Robust "Penalties" & "Prepayment/Arrears" formulas (work with text or true date cells)
- Conditional formatting: arrears<0 (light red), penalties>0 (light yellow)
- Quota-friendly: one get_all_values per sheet per run; in-memory cache; batch writes
"""

from __future__ import annotations
import re
import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from googleapiclient.errors import HttpError
from gspread.utils import rowcol_to_a1

# =========================
# Parsing (M-Pesa emails)
# =========================

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
    """Parse raw email text into a structured payment dict. Returns None if no match."""
    m = PATTERN.search(text or "")
    if not m:
        return None
    amt, code, payer, phone, dt, ref = m.groups()
    return {
        'Date Paid':   dt.strip(),                      # dd/mm/YYYY hh:mm AM/PM
        'Amount Paid': float(amt.replace(',', '')),
        'REF Number':  ref.upper(),
        'Payer':       payer.strip(),
        'Phone':       phone.strip(),
        'Payment Mode':'MPESA Payment',
        'AccountCode': code.upper(),
    }

# =========================
# Header + schema helpers
# =========================

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
    """Normalize header text (lowercase, strip punctuation/extra spaces)."""
    s = str(s or "").replace("\xa0", " ").strip().lower()
    s = re.sub(r"[^\w\s/]+", "", s)
    s = re.sub(r"\s+", " ", s)
    return s

def _with_backoff(fn, *args, **kwargs):
    """Retry wrapper for Sheets API calls (handles 429 throttling)."""
    delay = 1.0
    for _ in range(6):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            status = getattr(getattr(e, "resp", None), "status", None)
            if status == 429 or "quota" in str(e).lower():
                time.sleep(delay); delay *= 2; continue
            raise

def _alias_key_for(normalized_header: str) -> Optional[str]:
    """Return the canonical key if header matches an alias, else None."""
    for key, aliases in ALIASES.items():
        if normalized_header in aliases:
            return key
    return None

def _header_colmap(header: List[str]) -> Dict[str, int]:
    """Map canonical keys → column index for a given header row."""
    colmap: Dict[str,int] = {}
    seen_keys = set()
    for idx, name in enumerate(header):
        name_str = str(name or "")
        norm = _norm_header(name_str)
        if name_str in CANONICAL_SET:
            key = next(k for k, v in CANONICAL_NAME.items() if v == name_str)
        else:
            key = _alias_key_for(norm)
        if key and key not in seen_keys:
            colmap[key] = idx
            seen_keys.add(key)
    return colmap

# =========================
# Month parsing / formatting
# =========================

def _parse_month_cell(s: str) -> Optional[Tuple[int,int]]:
    """Parse Month like 'Aug-2025', 'August 2025', '2025-08', '08/2025' → (year, month)."""
    if not s:
        return None
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
    if m:
        return (int(m.group(1)), int(m.group(2)))
    m = re.match(r"^(\d{1,2})[-/](\d{4})$", s)
    if m:
        return (int(m.group(2)), int(m.group(1)))
    return None

def _choose_month_display(existing_month_samples: List[str], dt: datetime) -> str:
    """Emit Month string in an existing style if we find one, else 'Mon-YYYY'."""
    for v in existing_month_samples:
        t = (v or "").strip()
        if not t:
            continue
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

# =========================
# Conditional formatting
# =========================

def _ensure_conditional_formatting(ws, header_row0: int, colmap: Dict[str,int], cache: Dict):
    """Add CF rules for arrears (<0) and penalties (>0). Guard with cache flag to avoid duplicates within a run."""
    if cache.get("cf_applied"):
        return
    try:
        sheet_id = ws.id
    except Exception:
        return
    start_row = header_row0 + 1  # 0-based index for first data row

    requests = [
        {   # Arrears < 0 (Prepayment/Arrears column)
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
        {   # Penalties > 0 (Penalties column)
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

# =========================
# Header canonicalization
# =========================

def _canonicalize_header_and_rows(ws, header: List[str], rows: List[List[str]]) -> Tuple[List[str], List[List[str]], Dict[str,int], bool]:
    """
    - Normalize header labels to canonical names for any recognized alias.
    - Keep only canonical/aliased columns (drop “Column N” and any unknown headers) — deletes columns in the sheet.
    - Append any missing canonical columns at the end (adds cols in the sheet).
    - If 'Comments' is newly added, prefill 'None' for all existing rows.
    - Return (new_header, new_rows, colmap, comments_added_now).
    """
    comments_added_now = False

    # 1) Identify unknown/duplicate columns to delete from the actual sheet
    to_delete = []
    keep_mask = []
    seen_keys = set()
    for idx, name in enumerate(header):
        name_str = str(name or "")
        norm = _norm_header(name_str)
        key = None
        if name_str in CANONICAL_SET:
            key = next(k for k, v in CANONICAL_NAME.items() if v == name_str)
        else:
            key = _alias_key_for(norm)
        if key and key not in seen_keys:
            keep_mask.append(True)
            seen_keys.add(key)
        else:
            # delete "Column N" or unknowns/duplicates
            if re.fullmatch(r"(?i)column\s+\d+", name_str) or key is None or key in seen_keys:
                to_delete.append(idx)
                keep_mask.append(False)
            else:
                keep_mask.append(True)

    # Physically delete columns on the sheet (right->left)
    if to_delete:
        try:
            sheet_id = ws.id
            requests = []
            for idx in sorted(to_delete, reverse=True):
                requests.append({
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": idx,
                            "endIndex": idx+1
                        }
                    }
                })
            ws.spreadsheet.batch_update({"requests": requests})
        except Exception:
            pass

    # Apply deletions in-memory
    for idx in sorted(to_delete, reverse=True):
        del header[idx]
        for r in rows:
            if idx < len(r):
                del r[idx]

    # 2) Rename kept columns to canonical names
    for i, name in enumerate(header):
        norm = _norm_header(name)
        if name in CANONICAL_SET:
            continue
        key = _alias_key_for(norm)
        if key:
            header[i] = CANONICAL_NAME[key]

    # 3) Append any missing canonical columns (also ensure grid has enough cols)
    present = set(header)
    for key in REQUIRED_KEYS:
        canon = CANONICAL_NAME[key]
        if canon not in present:
            header.append(canon)
            for r in rows:
                r.append("None" if key == "comments" else "")
            present.add(canon)
            if key == "comments":
                comments_added_now = True
    try:
        if ws.col_count < len(header):
            ws.add_cols(len(header) - ws.col_count)
    except Exception:
        pass

    # 4) Rebuild colmap
    colmap = _header_colmap(header)
    return header, rows, colmap, comments_added_now

# =========================
# Cache
# =========================

_sheet_cache: Dict[str, Dict] = {}

def clear_cache():
    """Clear in-memory sheet cache."""
    _sheet_cache.clear()

# =========================
# Main updater
# =========================

def update_tenant_month_row(ws, payment: Dict) -> Dict:
    """
    Update a tenant worksheet for a payment dict.

    Fast path by design:
      - One read of entire sheet (first time per sheet per run).
      - In-memory scan/mutate of cached rows.
      - One batch write to push values + formulas.
      - One-time conditional formatting apply per sheet per run.
    """
    title = ws.title
    header_row0 = 0  # header at row 1

    # 0) Prime cache: read everything once
    if title not in _sheet_cache:
        all_vals = _with_backoff(ws.get_all_values)  # ONE READ
        header = list(all_vals[0]) if all_vals else []
        rows   = [list(r) for r in all_vals[1:]] if len(all_vals) > 1 else []

        # Canonicalize header + rows (in memory, and delete junk columns in sheet)
        header, rows, colmap, comments_added_now = _canonicalize_header_and_rows(ws, header, rows)

        # Write back normalized header (single row update)
        _with_backoff(ws.update, "1:1", [header])

        # If we just added Comments, prefill "None" for all existing rows (single column update)
        if comments_added_now and rows:
            comments_col_1based = colmap["comments"] + 1
            start_addr = rowcol_to_a1(header_row0 + 2, comments_col_1based)             # e.g., H2
            end_addr   = rowcol_to_a1(header_row0 + 1 + len(rows), comments_col_1based) # e.g., H{last}
            rng = f"{start_addr}:{end_addr}"
            _with_backoff(ws.update, rng, [[ "None" ] for _ in rows], value_input_option="USER_ENTERED")

        # Cache sheet state
        _sheet_cache[title] = {
            "header_row0": header_row0,
            "header": header,
            "rows": rows,
            "colmap": colmap,
            "cf_applied": False,
        }

    header = _sheet_cache[title]["header"]
    rows   = _sheet_cache[title]["rows"]
    colmap = _sheet_cache[title]["colmap"]
    header_row0 = _sheet_cache[title]["header_row0"]

    # 1) Date calculations
    dt_paid = datetime.strptime(payment['Date Paid'], '%d/%m/%Y %I:%M %p')
    target_y, target_m = dt_paid.year, dt_paid.month
    existing_month_samples = [r[colmap['month']] for r in rows if len(r) > colmap['month'] and r[colmap['month']]]
    month_display = _choose_month_display(existing_month_samples, dt_paid)

    # 2) Find existing month row (compare parsed (year,month))
    row_abs = None
    for i, r in enumerate(rows, start=2):  # row 1 is header
        if len(r) <= colmap['month']:
            continue
        ym = _parse_month_cell(r[colmap['month']])
        if ym and ym == (target_y, target_m):
            row_abs = i
            break

    # 3) If not found, append new row with carried-forward Amount Due (rent) and default Comments
    if row_abs is None:
        last_due = "0"
        for r in reversed(rows):
            if len(r) > colmap['amount_due'] and str(r[colmap['amount_due']]).strip():
                last_due = r[colmap['amount_due']]
                break
        new_row = [""] * len(header)
        new_row[colmap['month']] = month_display
        new_row[colmap['amount_due']] = last_due
        new_row[colmap['comments']] = "None"  # default so landlord/caretaker can later replace
        rows.append(new_row)
        _with_backoff(ws.append_row, new_row, value_input_option='USER_ENTERED')
        row_abs = len(rows) + 1  # account for header row

    # 4) Mutate the cached row in memory (do not overwrite existing Comments)
    row_idx = row_abs - 2
    while len(rows[row_idx]) < len(header):
        rows[row_idx].append("")

    def _num(v):
        try:
            return float(str(v).replace(",", "").strip() or 0)
        except Exception:
            return 0.0

    paid_before = _num(rows[row_idx][colmap['amount_paid']])
    paid_after  = paid_before + float(payment['Amount Paid'])
    prev_ref    = rows[row_idx][colmap['ref']] or ""
    ref_new     = payment['REF Number'] if not prev_ref else f"{prev_ref}, {payment['REF Number']}"
    due_str     = datetime(target_y, target_m, 5).strftime("%d/%m/%Y")  # ALWAYS 5th

    # Update in-memory row values (leave 'Comments' as-is if user added anything)
    rows[row_idx][colmap['month']]       = month_display
    rows[row_idx][colmap['amount_paid']] = str(paid_after)
    rows[row_idx][colmap['date_paid']]   = payment['Date Paid']
    rows[row_idx][colmap['ref']]         = ref_new
    rows[row_idx][colmap['date_due']]    = due_str

    # 5) Build robust formulas (text or serial dates both work)
    amt_paid_addr  = rowcol_to_a1(row_abs, colmap['amount_paid']+1)
    amt_due_addr   = rowcol_to_a1(row_abs, colmap['amount_due']+1)
    date_paid_addr = rowcol_to_a1(row_abs, colmap['date_paid']+1)
    date_due_addr  = rowcol_to_a1(row_abs, colmap['date_due']+1)
    pen_addr       = rowcol_to_a1(row_abs, colmap['penalties']+1)
    bal_addr       = rowcol_to_a1(row_abs, colmap['prepay_arrears']+1)
    prev_bal_addr  = rowcol_to_a1(row_abs-1, colmap['prepay_arrears']+1)

    # Penalty formula:
    # =IF(AND(ISNUMBER(DatePaid), ISNUMBER(DateDue), DatePaid > DateDue + 2), 3000, 0)
    # Coerces text/numeric dates:
    dpaid_expr = f"IF(ISTEXT({date_paid_addr}), DATEVALUE(LEFT({date_paid_addr},10)), {date_paid_addr})"
    ddue_expr  = f"IF(ISTEXT({date_due_addr}), DATEVALUE({date_due_addr}), {date_due_addr})"
    pen_formula = f"=IF(AND(ISNUMBER({dpaid_expr}), ISNUMBER({ddue_expr}), {dpaid_expr}>{ddue_expr}+2),3000,0)"

    # Rolling balance (Prepayment/Arrears)
    if row_abs == header_row0 + 2:
        bal_formula = f"=N({amt_paid_addr})-N({amt_due_addr})-N({pen_addr})"
    else:
        bal_formula = f"=N({prev_bal_addr})+N({amt_paid_addr})-N({amt_due_addr})-N({pen_addr})"

    # 6) Single batch write for all changes
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

    # 7) Ensure conditional formatting exists (only once per run per sheet)
    _ensure_conditional_formatting(ws, header_row0, colmap, _sheet_cache[title])

    return {
        "sheet": ws.title,
        "row": row_abs,
        "month": month_display,
        "paid_before": paid_before,
        "paid_after": paid_after,
        "date_due": due_str,
    }

