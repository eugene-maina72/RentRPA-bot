import re
import time
from datetime import datetime, timedelta
from typing import Dict
from googleapiclient.errors import HttpError
from gspread.utils import rowcol_to_a1
from dateutil.relativedelta import relativedelta

# ------------------- Parsing -------------------
MAX_PHONE_LEN = 13
REF_LEN = 10

# Optional '#' before account code; 10-char alphanumeric ref
PATTERN = re.compile(
    rf'payment of KES ([\d,]+\.\d{{2}}) '
    rf'for account: PAYLEMAIYAN\s*#?\s*([A-Za-z]\d{{1,2}})'
    rf' has been received from (.+?) '
    rf'(.{{1,{MAX_PHONE_LEN}}}) '
    rf'on (\d{{2}}/\d{{2}}/\d{{4}} \d{{1,2}}:\d{{2}} [APM]{{2}})\. '
    rf'M-Pesa Ref: ([A-Z0-9]{{{REF_LEN}}})',
    flags=re.IGNORECASE
)

# Unified event schema (aligns app + notebook)
PAYMENT_COLS = ['Date Paid','Amount Paid','REF Number','Payer','Phone','Payment Mode']

def parse_email(text: str):
    """Return dict in PAYMENT_COLS + AccountCode or None if no match."""
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

# ------------------- Tenant sheet updater (realtime formulas) -------------------
_PUNCT = re.compile(r"[^\w\s/]+", re.UNICODE)
def _norm(s):
    if s is None: return ""
    s = str(s).replace("\xa0", " ")
    s = s.strip().lower()
    s = _PUNCT.sub("", s)
    s = re.sub(r"\s+", " ", s)
    return s

ALIASES = {
    "month": {"month","month/period","period","rent month","billing month"},
    "amount_due": {"amount due","rent due","due","amountdue","monthly rent","rent","amount due kes","rent kes"},
    "amount_paid": {"amount paid","paid","amt paid","paid kes","amountpaid"},
    "date_paid": {"date paid","paid date","payment date","datepaid"},
    "ref": {"ref number","ref","reference","ref no","reference no","mpesa ref","mpesa reference","receipt","receipt no"},
    "date_due": {"date due","due date","rent due date","datedue"},
    "prepay_arrears": {"prepayment/arrears","prepayment","arrears","balance","bal","prepayment arrears","carry forward","cf"},
    "penalties": {"penalties","penalty","late fee","late fees","fine","fines"},
}
REQUIRED_KEYS = ["month","amount_due","amount_paid","date_paid","ref","date_due","prepay_arrears","penalties"]

def _header_map_from_row(row):
    row_norm = [_norm(c) for c in row]
    colmap = {}
    for key, aliases in ALIASES.items():
        for i, token in enumerate(row_norm):
            if token in aliases:
                colmap[key] = i
                break
    return colmap

def _detect_or_create_header(ws):
    """Probe A1:T10 for a header. If none matches (>=4), insert canonical header at row 1."""
    end_a1 = rowcol_to_a1(10, 20)  # T10
    try:
        window = ws.get(f"A1:{end_a1}")
    except Exception:
        window = []

    best_idx, best_map, best_hits = None, None, -1
    for idx, row in enumerate(window):
        cmap = _header_map_from_row(row)
        hits = len(cmap)
        if hits > best_hits:
            best_idx, best_map, best_hits = idx, cmap, hits

    if best_hits >= 4 and best_idx is not None:
        header = ws.row_values(best_idx+1)
        if best_map is not None and "penalties" not in best_map:
            header = header + ["Penalties"]
            ws.update(values=[header], range_name=f"{best_idx+1}:{best_idx+1}", value_input_option="USER_ENTERED")
            best_map = _header_map_from_row(header)
        return best_idx, header, best_map

    header = ['Month','Amount Due','Amount paid','Date paid','REF Number','Date due','Prepayment/Arrears','Penalties']
    ws.insert_rows([header], row=1)
    return 0, header, _header_map_from_row(header)

def _month_key_from_date_str(date_str):
    dt = datetime.strptime(date_str, '%d/%m/%Y %I:%M %p')
    return dt.strftime('%b-%Y'), dt

def _find_month_row(values, month_col_idx, month_key):
    for r in range(1, len(values)):  # skip header row 0
        cell = str(values[r][month_col_idx]).strip()
        if not cell:
            continue
        if cell.lower().startswith(month_key.lower()[:3]) and month_key[-4:] in cell:
            return r
    return None

def _col_letter(row, col):
    return re.sub(r'\d+', '', rowcol_to_a1(row, col))

def update_tenant_month_row(tenant_ws, payment: Dict) -> Dict:
    """
    Realtime variant:
      - Writes ONLY: Amount paid, Date paid, REF Number
      - Sets formulas for: Prepayment/Arrears = N(Amount paid) - N(Amount Due)
                           Penalties = IF(AND(N(Paid)<N(Due), DATEVALUE(LEFT(DatePaid,10))>DATEVALUE(DateDue)+6), 3000, 0)
    Returns details for logging; does NOT read formula results (quota-safe).
    """
    header_row0, header, colmap = _detect_or_create_header(tenant_ws)
    if not colmap:
        raise ValueError(f"Sheet '{tenant_ws.title}' header mapping failed (colmap is None).")
    missing = [k for k in REQUIRED_KEYS if k not in colmap]
    if missing:
        raise ValueError(f"Sheet '{tenant_ws.title}' missing required columns after normalization: {missing}")

    all_vals = tenant_ws.get_all_values()
    vals = all_vals[header_row0:]
    base_row_1based = header_row0 + 1

    month_key, _pay_dt = _month_key_from_date_str(payment['Date Paid'])
    row_rel = _find_month_row(vals, colmap['month'], month_key)
    if row_rel is None:
        new_row = [''] * len(header)
        new_row[colmap['month']] = month_key
        new_row[colmap['amount_due']] = '0'
        new_row[colmap['amount_paid']] = '0'
        new_row[colmap['date_paid']] = ''
        new_row[colmap['ref']] = ''
        new_row[colmap['date_due']] = ''
        new_row[colmap['prepay_arrears']] = '0'
        new_row[colmap['penalties']] = '0'

        # Set Date due as the previous row's date due plus one month.
        # Try to get last row's Date due (skip header row)
        if len(vals) > 1 and vals[-1][colmap['date_due']]:
            try:
                last_date_due = datetime.strptime(vals[-1][colmap['date_due']], "%d/%m/%Y").replace(day=5)
                new_date_due = last_date_due + relativedelta(months=1)
            except Exception:
                # Fallback to payment date plus one month if parsing fails
                new_date_due = datetime.strptime(payment['Date Paid'], '%d/%m/%Y %I:%M %p') + relativedelta(months=1)
            new_row[colmap['date_due']] = new_date_due.strftime("%d/%m/%Y")
        else:
            new_date_due = datetime.strptime(payment['Date Paid'], '%d/%m/%Y %I:%M %p') + relativedelta(months=1)
            new_row[colmap['date_due']] = new_date_due.strftime("%d/%m/%Y")
            
        # prepay/arrears and penalties will be set as FORMULAS after append
        tenant_ws.append_row(new_row, value_input_option='USER_ENTERED')
        all_vals = tenant_ws.get_all_values()
        vals = all_vals[header_row0:]
        row_rel = len(vals) - 1

    if row_rel is None or row_rel < 0 or row_rel >= len(vals):
        raise ValueError("Failed to find or create a valid month row in the tenant sheet.")

    row_abs_1based = base_row_1based + row_rel
    row = vals[row_rel]
    def _num(v):
        try:
            s = str(v).replace(',','').strip()
            return float(s) if s else 0.0
        except:
            return 0.0
    def _str(v):
        return '' if v is None else str(v)

    paid0 = _num(row[colmap['amount_paid']])
    ref0  = _str(row[colmap['ref']])
    paid1 = paid0 + float(payment['Amount Paid'])

    # Write only the 3 direct fields (compact range write)
    updates = {
        colmap['amount_paid']:  paid1,
        colmap['date_paid']:    payment['Date Paid'],
        colmap['ref']:          (payment['REF Number'] if not ref0 else f"{ref0}, {payment['REF Number']}")
    }
    touched = sorted(updates.keys())
    c1 = touched[0] + 1
    c2 = touched[-1] + 1
    rng = f"{rowcol_to_a1(row_abs_1based, c1)}:{rowcol_to_a1(row_abs_1based, c2)}"
    payload = [''] * (c2 - c1 + 1)
    for cidx, val in updates.items():
        payload[(cidx + 1 - c1)] = val
    payload = [str(x) if x is not None else '' for x in payload]

    for attempt in range(5):
        try:
            tenant_ws.update(values=[payload], range_name=rng, value_input_option='USER_ENTERED')
            break
        except HttpError as e:
            if getattr(e, "resp", None) and e.resp.status == 429:
                time.sleep(5 * (attempt+1))
                continue
            raise

    # Ensure formulas exist (set once; then realtime recalcs)
    col_letters = {k: _col_letter(row_abs_1based, colmap[k] + 1) for k in colmap}
    amt_paid_addr = f"{col_letters['amount_paid']}{row_abs_1based}"
    amt_due_addr  = f"{col_letters['amount_due']}{row_abs_1based}"
    date_paid_addr= f"{col_letters['date_paid']}{row_abs_1based}"
    date_due_addr = f"{col_letters['date_due']}{row_abs_1based}"
    bal_addr      = f"{col_letters['prepay_arrears']}{row_abs_1based}"
    pen_addr      = f"{col_letters['penalties']}{row_abs_1based}"

    
    # Penalties formula: if DatePaid > DateDue + 2 days, penalty = 3000
    pen_formula = f"=IF(DATEVALUE(LEFT({date_paid_addr},10))>DATEVALUE({date_due_addr})+2, 3000, 0)"

    # Balance formula: if first data row, =N(amt_paid)-N(amt_due); else, =N(prev_bal)+N(amt_paid)-N(amt_due)
    if row_abs_1based == base_row_1based:
        bal_formula = f"=N({amt_paid_addr})-N({amt_due_addr})-N({pen_addr})"
    else:
        prev_bal_addr = f"{col_letters['prepay_arrears']}{row_abs_1based-1}"
        bal_formula = f"=N({prev_bal_addr})+N({amt_paid_addr})-N({amt_due_addr})-N({pen_addr})"

    # Only set formula if not already a formula (avoid read spam in loop)
    try:
        cur_bal = tenant_ws.acell(bal_addr).value or ""
        cur_pen = tenant_ws.acell(pen_addr).value or ""
    except Exception:
        cur_bal, cur_pen = "", ""

    needs_bal = not str(cur_bal).startswith("=")
    needs_pen = not str(cur_pen).startswith("=")

    if needs_bal or needs_pen:
        body = []
        if needs_bal: body.append({'range': bal_addr, 'values': [[bal_formula]]})
        if needs_pen: body.append({'range': pen_addr, 'values': [[pen_formula]]})
        tenant_ws.batch_update(body, value_input_option='USER_ENTERED')

    return {
        'sheet': tenant_ws.title,
        'month_row': row_abs_1based,
        'paid_before': paid0,
        'paid_after': paid1,
        'ref_added': payment['REF Number'],
        'formulas_set': {'balance': needs_bal, 'penalties': needs_pen},
    }
