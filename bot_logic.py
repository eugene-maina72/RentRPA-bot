# /bot_logic.py
# -*- coding: utf-8 -*-
"""
Core sheet-update logic for Rent RPA.

Key rules:
- Rent due on the 5th.
- Penalty (KES 3000) if payment date is on/after (due + 2 days) AND net balance <= 0.
- First-ever payment (B3:B5): if B4 equals monthly rent due, first-month balance = 0.
- Defensive formulas to prevent #VALUE!.
- NEW: Auto-consume prepayments — create future months where Amount Paid = Amount Due
  until remaining prepayment < one month's rent.
"""

from __future__ import annotations
from copy import deepcopy
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import re
import time
from gspread.utils import rowcol_to_a1

# History schema (matches workbook)
PAYMENT_COLS = ['Date Paid', 'Amount Paid', 'REF Number', 'Payer', 'Phone', 'Comments']

# --- NCBA email parser (tolerant to case, '#', date formats) -----------------

DATE_PAT = (
    r'(?:'
    r'\d{2}/\d{2}/\d{4}\s+\d{1,2}:\d{2}\s+[APMapm]{2}'
    r'|'
    r'\d{4}[-/]\d{2}[-/]\d{2}[ T]\d{1,2}:\d{2}(?:\s?[APMapm]{2})?'
    r'|'
    r'\d{2}[-/]\d{2}[-/]\d{4}\s+\d{1,2}:\d{2}(?:\s?[APMapm]{2})?'
    r')'
)

PATTERN = re.compile(
    rf'payment of KES ([\d,]+\.\d{{2}})\s*'
    rf'for account:\s*PAYLEMAIYAN\s*#?\s*([A-Za-z]\d{{1,2}})\s*'
    rf'has been received from\s+(.+?)\s+(.{{1,13}})\s+'
    rf'on\s+({DATE_PAT})\.?\s+'
    rf'M-?Pesa Ref:\s*([A-Za-z0-9\-\s]{{6,32}})',
    flags=re.IGNORECASE
)

def _normalize_ref(ref_raw: str, min_len: int = 8, max_len: int = 16) -> str | None:
    core = re.sub(r'[^A-Za-z0-9]', '', (ref_raw or '')).upper()
    return core if min_len <= len(core) <= max_len else None  # why: reject bad refs

def _normalize_payer(name: str) -> str:
    n = (name or "").strip()
    return " ".join(part.capitalize() for part in re.split(r"\s+", n) if part)

def _normalize_date_ddmmyyyy(dt_str: str) -> str:
    dt_str = (dt_str or "").strip()
    fmts = [
        "%d/%m/%Y %I:%M %p", "%d/%m/%Y %H:%M",
        "%d-%m-%Y %I:%M %p", "%d-%m-%Y %H:%M",
        "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M",
        "%Y-%m-%d %I:%M %p", "%Y/%m/%d %I:%M %p",
        "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d",
    ]
    for f in fmts:
        try:
            dt = datetime.strptime(dt_str, f)
            return dt.strftime("%d/%m/%Y")
        except ValueError:
            continue
    return dt_str

def parse_email(text: str) -> Optional[Dict]:
    m = PATTERN.search(text or "")
    if not m:
        return None
    amt, code, payer, phone, dt_str, ref_raw = m.groups()
    ref = _normalize_ref(ref_raw)
    if not ref:
        return None
    return {
        'Date Paid':   _normalize_date_ddmmyyyy(dt_str),
        'Amount Paid': float((amt or "0").replace(',', '')) if amt else 0.0,
        'REF Number':  ref,
        'Payer':       _normalize_payer(payer),
        'Phone':       (phone or "").strip(),
        'AccountCode': (code or "").upper(),
        'Comments':    "",
    }

# --- Canonical headers & helpers --------------------------------------------

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
    "amount_paid": "Amount Paid",
    "date_paid": "Date Paid",
    "ref": "REF Number",
    "date_due": "Date Due",
    "prepay_arrears": "Prepayment/Arrears",
    "penalties": "Penalties",
    "comments": "Comments",
}
CANONICAL_SET = set(CANONICAL_NAME.values())

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

# --- Utilities ---------------------------------------------------------------

def _with_backoff(fn, *args, **kwargs):
    delay = 1.0
    for _ in range(6):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            status = getattr(getattr(e, "resp", None), "status", None)
            if status == 429 or "quota" in str(e).lower() or "rate limit" in str(e).lower():
                time.sleep(delay); delay *= 2; continue
            raise

def _with_backoff_factory(fn_factory, *, max_tries=6):
    delay = 1.0
    for _ in range(max_tries):
        try:
            return fn_factory()
        except Exception as e:
            status = getattr(getattr(e, "resp", None), "status", None)
            if status == 429 or "quota" in str(e).lower() or "rate limit" in str(e).lower():
                time.sleep(delay); delay *= 2; continue
            raise

def _strip_ws_prefix(a1: str) -> str:
    s = str(a1 or "")
    while True:
        m = re.match(r"^'[^']+'!(.+)$", s)
        if not m: break
        s = m.group(1)
    return s

def _ensure_grid_size(ws, need_rows: Optional[int] = None, need_cols: Optional[int] = None):
    try:
        cur_rows = ws.row_count
        cur_cols = ws.col_count
        if need_rows is not None and need_rows > cur_rows:
            _with_backoff(ws.add_rows, need_rows - cur_rows)
        if need_cols is not None and need_cols > cur_cols:
            _with_backoff(ws.add_cols, need_cols - cur_cols)
    except Exception:
        pass

def _detect_header_row(all_vals, scan_rows: int = 30) -> int:
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
        if re.fullmatch(r"\d{4}[-/]\d{2}", t):
            return f"{dt.year}-{dt.month:02d}"
        if re.fullmatch(r"[A-Za-z]{3,9}[- ]\d{4}", t):
            return dt.strftime("%b-%Y")
        if re.fullmatch(r"\d{2}[-/]\d{4}", t):
            return f"{dt.month:02d}-{dt.year}"
    return dt.strftime("%b-%Y")

def _ensure_monthkey_header(ws, header_row0: int, header: list[str], colmap: dict):
    norm = [h.strip().lower() for h in header]
    if "monthkey" not in norm:
        header.append("MonthKey")
        ws.update(f"{header_row0+1}:{header_row0+1}", [header], value_input_option="USER_ENTERED")
        colmap.clear()
        colmap.update(_header_colmap(header))

def _sort_by_monthkey(ws, header_row0: int, header: list[str], colmap: dict):
    try:
        mk_idx = [h.strip().lower() for h in header].index("monthkey")
    except ValueError:
        return
    try:
        ws.sort((mk_idx+1, 'asc'))  # best-effort
    except Exception:
        pass

# --- Conditional formatting --------------------------------------------------

def _ensure_conditional_formatting(ws, header_row0: int, colmap: Dict[str,int], cache: Dict, debug: Optional[List[str]] = None):
    if cache.get("cf_applied"): return
    try:
        sheet_id = ws.id
    except Exception:
        return
    start_row = header_row0 + 1
    requests = [
        {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": sheet_id, "startRowIndex": start_row, "startColumnIndex": colmap["prepay_arrears"], "endColumnIndex": colmap["prepay_arrears"] + 1}],
                    "booleanRule": {"condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]}, "format": {"backgroundColor": {"red": 1.0, "green": 0.84, "blue": 0.84}}},
                },
                "index": 0,
            }
        },
        {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": sheet_id, "startRowIndex": start_row, "startColumnIndex": colmap["penalties"], "endColumnIndex": colmap["penalties"] + 1}],
                    "booleanRule": {"condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]}, "format": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 0.6}}},
                },
                "index": 1,
            }
        },
    ]
    try:
        ws.spreadsheet.batch_update({"requests": requests})
        cache["cf_applied"] = True
        if debug is not None: debug.append("Applied conditional formatting.")
    except Exception as e:
        if debug is not None: debug.append(f"CF skipped: {e}")

# --- Cache -------------------------------------------------------------------

_sheet_cache: Dict[str, Dict] = {}
def clear_cache(): _sheet_cache.clear()

def _inc_month(y: int, m: int) -> Tuple[int,int]:
    return (y + (1 if m == 12 else 0), 1 if m == 12 else m + 1)

# --- Main --------------------------------------------------------------------

def update_tenant_month_row(ws, payment: Dict, debug: Optional[List[str]] = None) -> Dict:
    title = ws.title
    if debug is not None:
        debug.append(f"[{title}] Start REF={payment.get('REF Number')} Amt={payment.get('Amount Paid')} DatePaid={payment.get('Date Paid')}")

    # Prime cache
    if title not in _sheet_cache:
        all_vals = _with_backoff(ws.get_all_values)
        header_row0 = _detect_header_row(all_vals)
        header = list(all_vals[header_row0]) if len(all_vals) > header_row0 else []
        rows   = [list(r) for r in all_vals[header_row0+1:]] if len(all_vals) > header_row0+1 else []
        colmap = _header_colmap(header)

        added_any = False
        for key in REQUIRED_KEYS:
            if key not in colmap:
                header.append(CANONICAL_NAME[key])
                for r in rows: r.append("None" if key == "comments" else "")
                added_any = True

        _ensure_grid_size(ws, need_rows=header_row0+1, need_cols=len(header))
        _with_backoff(ws.update, f"{header_row0+1}:{header_row0+1}", [header], value_input_option='USER_ENTERED')

        cache = {"header_row0": header_row0, "header": header, "rows": rows, "colmap": _header_colmap(header), "cf_applied": False}
        _sheet_cache[title] = cache
        if debug is not None:
            debug.append(f"[{title}] Header row {header_row0+1}, added_missing={added_any}")
    else:
        cache = _sheet_cache[title]

    header_row0, header, rows, colmap = cache["header_row0"], cache["header"], cache["rows"], cache["colmap"]

    dt_paid = datetime.strptime(payment['Date Paid'], '%d/%m/%Y')
    target_y, target_m = dt_paid.year, dt_paid.month
    existing_month_samples = [r[colmap['month']] for r in rows if len(r) > colmap['month'] and r[colmap['month']]]
    month_display = _choose_month_display(existing_month_samples, dt_paid)

    # Find or create target month row
    row_abs = None; row_idx = None
    for idx_in_rows, r in enumerate(rows, start=1):
        if len(r) <= colmap['month']: continue
        ym = _parse_month_cell(r[colmap['month']])
        if ym and ym == (target_y, target_m):
            row_idx = idx_in_rows - 1
            row_abs = header_row0 + 1 + idx_in_rows
            break

    if row_abs is None:
        last_due = "0"
        for r in reversed(rows):
            if len(r) > colmap['amount_due'] and str(r[colmap['amount_due']]).strip():
                last_due = r[colmap['amount_due']]
                break
        new_row = [""] * len(header)
        new_row[colmap['month']]      = month_display
        new_row[colmap['amount_due']] = last_due
        if "comments" in colmap: new_row[colmap['comments']] = "None"
        rows.append(new_row)
        row_idx = len(rows) - 1
        row_abs = header_row0 + 1 + (row_idx + 1)
        _ensure_grid_size(ws, need_rows=row_abs, need_cols=len(header))
        _with_backoff(ws.update, f"{row_abs}:{row_abs}", [new_row], value_input_option='USER_ENTERED')
        if debug is not None:
            debug.append(f"[{title}] Created month row R{row_abs}; carry Amount Due={last_due}")

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

    # Numeric due for estimation & for auto-prepay loop
    current_due_num = _num(row_vals[colmap['amount_due']]) if row_vals[colmap['amount_due']] else \
                      (_num(rows[row_idx-1][colmap['amount_due']]) if row_idx > 0 else 0.0)

    # Estimate new balance (no penalty when prepaying)
    prev_bal_num = _num(rows[row_idx-1][colmap['prepay_arrears']]) if row_idx > 0 and len(rows[row_idx-1]) > colmap['prepay_arrears'] else 0.0
    est_balance_after = prev_bal_num + float(payment['Amount Paid']) - current_due_num

    row_vals[colmap['month']]       = month_display
    row_vals[colmap['amount_paid']] = str(paid_after)
    row_vals[colmap['date_paid']]   = payment['Date Paid']
    row_vals[colmap['ref']]         = ref_new
    row_vals[colmap['date_due']]    = due_str

    # A1s for formulas/values
    amt_paid_addr  = rowcol_to_a1(row_abs, colmap['amount_paid']+1)
    amt_due_addr   = rowcol_to_a1(row_abs, colmap['amount_due']+1)
    date_paid_addr = rowcol_to_a1(row_abs, colmap['date_paid']+1)
    date_due_addr  = rowcol_to_a1(row_abs, colmap['date_due']+1)
    pen_addr       = rowcol_to_a1(row_abs, colmap['penalties']+1)
    bal_addr       = rowcol_to_a1(row_abs, colmap['prepay_arrears']+1)
    prev_bal_addr  = rowcol_to_a1(row_abs-1, colmap['prepay_arrears']+1)

    # Safe coercions (avoid #VALUE!)
    paid_num   = f"IFERROR(VALUE({amt_paid_addr}), N({amt_paid_addr}))"
    due_num    = f"IFERROR(VALUE({amt_due_addr}),  N({amt_due_addr}))"
    prev_bal   = f"IFERROR({prev_bal_addr}, 0)"
    pen_num    = f"IFERROR({pen_addr}, 0)"
    dpaid_expr = f"IFERROR(DATEVALUE(TO_TEXT({date_paid_addr})), {date_paid_addr})"
    ddue_expr  = f"IFERROR(DATEVALUE(TO_TEXT({date_due_addr})),  {date_due_addr})"
    has_paid   = f"LEN(TO_TEXT({date_paid_addr}))>0"
    has_due    = f"LEN(TO_TEXT({date_due_addr}))>0"
    net_after  = f"({prev_bal} + {paid_num} - {due_num})"

    # Updated penalty rule: >= due+2 AND net_after <= 0
    pen_formula = f"=IF(AND({has_paid}, {has_due}, {net_after} <= 0, {dpaid_expr} >= {ddue_expr} + 2), 3000, 0)"

    # Balance formula (uniform for all rows, no first-payment override)
    if row_abs == header_row0 + 2:
        bal_formula = f"=({paid_num})-({due_num})-({pen_num})"
    else:
        bal_formula = f"=({prev_bal})+({paid_num})-({due_num})-({pen_num})"

    # Write this row
    _ensure_grid_size(ws, need_rows=row_abs, need_cols=len(header))
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
        debug.append(f"[{title}] R{row_abs}: paid {paid_before}→{paid_after}, due={due_str}")

    # Header/formatting
    _ensure_conditional_formatting(ws, header_row0, colmap, cache, debug)
    _ensure_monthkey_header(ws, header_row0, header, colmap)
    # MonthKey for this row (quota-friendly single cell)
    try:
        mk_idx = [h.strip().lower() for h in header].index("monthkey")
        mk_addr = rowcol_to_a1(row_abs, mk_idx + 1)
        mk_value = f"{target_y:04d}-{target_m:02d}"
        _with_backoff(ws.update, mk_addr, [[mk_value]], value_input_option="USER_ENTERED")
    except Exception:
        pass

    # --- Auto-consume prepayments into future months -------------------------
    monthly_due = current_due_num if current_due_num > 0 else 0.0
    safety_max_months = 24  # avoid accidental loops
    carry_ref_note = f"{payment['REF Number']}(carry)"
    y, m = target_y, target_m
    added = 0

    while monthly_due > 0 and est_balance_after >= monthly_due and added < safety_max_months:
        # next month
        y, m = _inc_month(y, m)
        next_due_str = datetime(y, m, 5).strftime("%d/%m/%Y")
        dt_next = datetime(y, m, 1)
        next_month_display = _choose_month_display(existing_month_samples or [month_display], dt_next)

        # create/app row
        new_row = [""] * len(header)
        new_row[colmap['month']]       = next_month_display
        new_row[colmap['amount_due']]  = f"{monthly_due:g}"
        new_row[colmap['amount_paid']] = f"{monthly_due:g}"
        new_row[colmap['date_paid']]   = payment['Date Paid']          # same date → no penalty
        new_row[colmap['ref']]         = carry_ref_note
        new_row[colmap['date_due']]    = next_due_str
        if "comments" in colmap:
            new_row[colmap['comments']] = "Auto prepayment applied"

        rows.append(new_row)
        row_idx2 = len(rows) - 1
        row_abs2 = header_row0 + 1 + (row_idx2 + 1)

        # addresses
        ap2 = rowcol_to_a1(row_abs2, colmap['amount_paid']+1)
        ad2 = rowcol_to_a1(row_abs2, colmap['amount_due']+1)
        dp2 = rowcol_to_a1(row_abs2, colmap['date_paid']+1)
        dd2 = rowcol_to_a1(row_abs2, colmap['date_due']+1)
        pe2 = rowcol_to_a1(row_abs2, colmap['penalties']+1)
        ba2 = rowcol_to_a1(row_abs2, colmap['prepay_arrears']+1)
        prev_ba2 = rowcol_to_a1(row_abs2-1, colmap['prepay_arrears']+1)

        # safe formulas
        paid_num2   = f"IFERROR(VALUE({ap2}), N({ap2}))"
        due_num2    = f"IFERROR(VALUE({ad2}), N({ad2}))"
        prev_bal2   = f"IFERROR({prev_ba2}, 0)"
        pen_num2    = f"IFERROR({pe2}, 0)"
        dpaid2      = f"IFERROR(DATEVALUE(TO_TEXT({dp2})), {dp2})"
        ddue2       = f"IFERROR(DATEVALUE(TO_TEXT({dd2})), {dd2})"
        has_paid2   = f"LEN(TO_TEXT({dp2}))>0"
        has_due2    = f"LEN(TO_TEXT({dd2}))>0"
        net_after2  = f"({prev_bal2} + {paid_num2} - {due_num2})"
        pen_formula2 = f"=IF(AND({has_paid2}, {has_due2}, {net_after2} <= 0, {dpaid2} >= {ddue2} + 2), 3000, 0)"
        bal_formula2 = f"=({prev_bal2})+({paid_num2})-({due_num2})-({pen_num2})"

        _ensure_grid_size(ws, need_rows=row_abs2, need_cols=len(header))
        upd2 = [
            {"range": _strip_ws_prefix(rowcol_to_a1(row_abs2, colmap['month']+1)),          "values": [[next_month_display]]},
            {"range": _strip_ws_prefix(rowcol_to_a1(row_abs2, colmap['amount_due']+1)),     "values": [[monthly_due]]},
            {"range": _strip_ws_prefix(rowcol_to_a1(row_abs2, colmap['amount_paid']+1)),    "values": [[monthly_due]]},
            {"range": _strip_ws_prefix(rowcol_to_a1(row_abs2, colmap['date_paid']+1)),      "values": [[payment['Date Paid']]]},
            {"range": _strip_ws_prefix(rowcol_to_a1(row_abs2, colmap['ref']+1)),            "values": [[carry_ref_note]]},
            {"range": _strip_ws_prefix(rowcol_to_a1(row_abs2, colmap['date_due']+1)),       "values": [[next_due_str]]},
            {"range": _strip_ws_prefix(rowcol_to_a1(row_abs2, colmap['comments']+1)),       "values": [["Auto prepayment applied"]] if "comments" in colmap else [[""]]},
            {"range": _strip_ws_prefix(rowcol_to_a1(row_abs2, colmap['penalties']+1)),      "values": [[pen_formula2]]},
            {"range": _strip_ws_prefix(rowcol_to_a1(row_abs2, colmap['prepay_arrears']+1)), "values": [[bal_formula2]]},
        ]
        _with_backoff_factory(lambda: ws.batch_update(deepcopy(upd2), value_input_option='USER_ENTERED'))

        # MonthKey for this row
        try:
            mk_idx = [h.strip().lower() for h in header].index("monthkey")
            mk_addr2 = rowcol_to_a1(row_abs2, mk_idx + 1)
            mk_value2 = f"{y:04d}-{m:02d}"
            _with_backoff(ws.update, mk_addr2, [[mk_value2]], value_input_option="USER_ENTERED")
        except Exception:
            pass

        est_balance_after -= monthly_due
        added += 1
        if debug is not None:
            debug.append(f"[{title}] Auto prepayment applied → created {next_month_display} with paid={monthly_due:g}")

    # final sort once
    _sort_by_monthkey(ws, header_row0, header, colmap)

    return {
        "sheet": ws.title,
        "row": row_abs,
        "month": month_display,
        "paid_before": paid_before,
        "paid_after": paid_after,
        "date_due": due_str,
        "autocreated_future_months": added,
        "ref_added": payment.get("REF Number"),
        "formulas_set": {"balance": True, "penalties": True},
    }