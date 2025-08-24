import re, base64, time
import pandas as pd
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from IPython.display import display

# --- Google APIs ---
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
import gspread
from gspread.utils import rowcol_to_a1
from dateutil.relativedelta import relativedelta

# ---------------- CONFIG ----------------
CLIENT_SECRET = 'bot_secret.json'        # Gmail OAuth Desktop credentials
SERVICE_KEY   = 'bot_service.json'      # Sheets service account (shared on the target Sheet)
SHEET_NAME    = 'RENT TRACKING-Lemaiyan Heights'  # exact Google Sheet NAME
GMAIL_QUERY   = 'subject:"NCBA TRANSACTIONS STATUS UPDATE" newer_than:365d'  # tweak as needed

# This prototype uses a unified event schema for consistency:
PAYMENT_COLS  = ['Date Paid','Amount Paid','REF Number','Payer','Phone','Payment Mode']
MAX_PHONE_LEN = 13
REF_LEN       = 10


# ----- AUTH -----
gmail_flow = InstalledAppFlow.from_client_secrets_file(
    CLIENT_SECRET,
    scopes=[
        'https://www.googleapis.com/auth/gmail.modify',  # read + mark read
        'https://www.googleapis.com/auth/gmail.readonly',
        'https://www.googleapis.com/auth/gmail.send'
    ]
)
gmail_creds = gmail_flow.run_local_server(port=0)
gmail = build('gmail', 'v1', credentials=gmail_creds)

sheets_creds = Credentials.from_service_account_file(
    SERVICE_KEY,
    scopes=['https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'])
gc = gspread.authorize(sheets_creds)
sh = gc.open(SHEET_NAME)

# ----- PARSER (flexible account code; 10-char ref) -----
PATTERN = re.compile(
    rf'payment of KES ([\d,]+\.\d{{2}}) '
    rf'for account: PAYLEMAIYAN\s*#?\s*([A-Za-z]\d{{1,2}})'
    rf' has been received from (.+?) '
    rf'(.{{1,{MAX_PHONE_LEN}}}) '
    rf'on (\d{{2}}/\d{{2}}/\d{{4}} \d{{1,2}}:\d{{2}} [APM]{{2}})\. '
    rf'M-Pesa Ref: ([A-Z0-9]{{{REF_LEN}}})',
    flags=re.IGNORECASE
)

def parse_email(text: str):
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
        'AccountCode': code.upper(),                    # used for routing to the tenant sheet
    }

# ----- GMAIL: get message text (best-effort) -----
def get_message_text(service, msg_id):
    msg = service.users().messages().get(userId="me", id=msg_id, format="full").execute()
    payload = msg.get("payload", {})
    body_texts = []

    def walk(part):
        mime = part.get("mimeType", "")
        data = part.get("body", {}).get("data")
        parts = part.get("parts", [])
        if mime == "text/plain" and data:
            body_texts.append(base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore"))
        for p in parts:
            walk(p)

    walk(payload)
    if body_texts:
        return "\n".join(body_texts)
    return msg.get("snippet", "")

# Normalizer: lower, trim, collapse spaces, strip punctuation/currency/nbsp
_PUNCT = re.compile(r"[^\w\s/]+", re.UNICODE)
def _norm(s):
    if s is None: return ""
    s = str(s).replace("\xa0", " ")  # nbsp -> space
    s = s.strip().lower()
    s = _PUNCT.sub("", s)            # remove punctuation like ( ), :, KES, etc.
    s = re.sub(r"\s+", " ", s)
    return s

# Broad alias sets (normalized)
ALIASES = {
    "month": {
        "month","month/period","period","rent month","billing month"
    },
    "amount_due": {
        "amount due","rent due","due","amountdue","monthly rent","rent","amount due kes","rent (kes)"
    },
    "amount_paid": {
        "amount paid","paid","amt paid","paid (kes)","amountpaid"
    },
    "date_paid": {
        "date paid","paid date","payment date","datepaid"
    },
    "ref": {
        "ref number","ref","reference","ref no","reference no","mpesa ref","mpesa reference","receipt","receipt no"
    },
    "date_due": {
        "date due","due date","rent due date","datedue"
    },
    "prepay_arrears": {
        "prepayment/arrears","prepayment","arrears","balance","bal","prepayment arrears","carry forward","cf"
    },
    "penalties": {
        "penalties","penalty","late fee","late fees","fine","fines"
    },
}

REQUIRED_KEYS = ["month","amount_due","amount_paid","date_paid","ref","date_due","prepay_arrears","penalties"]

# --- Helper to score header row ---
def _score_header(row_norm):
    """How many required columns does this row satisfy?"""
    hits = 0
    for key in REQUIRED_KEYS:
        if any(a in row_norm for a in ALIASES[key]):
            hits += 1
    return hits

# --- Helper to map row tokens to column keys ---
def _header_map_from_row(row):
    """Return (colmap) by matching normalized row tokens against aliases."""
    row_norm = [_norm(c) for c in row]
    colmap = {}
    for key, aliases in ALIASES.items():
        for i, token in enumerate(row_norm):
            if token in aliases:
                colmap[key] = i
                break
    return colmap


# --- Helper to detect or create a header row ---
def _detect_or_create_header(ws):
    """
    Find a header row in the first 10 rows.
    If none reaches a threshold (>=4 matches), insert a standard header at row 1.
    Returns (header_row_idx_0based, header_list, colmap).
    """
    all_data = ws.get_all_values()
    max_rows = len(all_data) if all_data else 1
    probe_rows = min(max_rows, 10)
    last_col = ws.col_count or 12
    rn = f"A1:{rowcol_to_a1(probe_rows, last_col)}"
    values = ws.get_values(rn)  # rectangular cut

    best_idx, best_hits, best_map = None, -1, None
    for idx, row in enumerate(values):
        colmap = _header_map_from_row(row)
        hits = len(colmap)
        if hits > best_hits:
            best_idx, best_hits, best_map = idx, hits, colmap

    if best_hits >= 4:
        header = ws.row_values(best_idx+1)
        missing_keys = [k for k in REQUIRED_KEYS if k not in best_map]
        if missing_keys:
            standard_columns = {
                "month": "Month",
                "amount_due": "Amount Due",
                "amount_paid": "Amount paid",
                "date_paid": "Date paid",
                "ref": "REF Number",
                "date_due": "Date due",
                "prepay_arrears": "Prepayment/Arrears",
                "penalties": "Penalties"
            }
            for key in missing_keys:
                header.append(standard_columns[key])
            ws.update(values=[header], range_name=f"{best_idx+1}:{best_idx+1}", value_input_option="USER_ENTERED")
            best_map = _header_map_from_row(header)
        return best_idx, header, best_map
    # No good header found: create standard header on row 1
    header = ['Month','Amount Due','Amount paid','Date paid','REF Number','Date due','Prepayment/Arrears','Penalties']
    if max_rows == 0:
        ws.update(values=[header], range_name="1:1", value_input_option="USER_ENTERED")
    else:
        ws.insert_row(header, index=1, value_input_option="USER_ENTERED")
    return 0, header, _header_map_from_row(header)


# --- Helper to convert date string to month key ---
def _month_key_from_date_str(date_str):
    dt = datetime.strptime(date_str, '%d/%m/%Y %I:%M %p')
    return dt.strftime('%B-%Y'), dt   # e.g., January-2025

# --- Helper to find the month row in values ---
def _find_month_row(values, month_col_idx, month_key):
    for r in range(1, len(values)):  # skip header at 0
        cell = str(values[r][month_col_idx]).strip()
        if not cell:
            continue
        # accept "Jan-2025"/"JAN 2025"/"January 2025"
        if cell.lower().startswith(month_key.lower()[:3]) and month_key[-4:] in cell:
            return r
    return None

# --- Helper to convert row/col to letter(s) ---
def _col_letter(row, col):
    """Return column letter(s) for a given 1-based row/col using A1 conversion."""
    return re.sub(r'\d+', '', rowcol_to_a1(row, col))


# --- Main function to update tenant month row ---
def update_tenant_month_row(tenant_ws, payment):
    """
    Realtime version:
      - Writes ONLY: Amount paid, Date paid, REF Number
      - Sets once-per-row formulas for:
          Prepayment/Arrears = N(Amount paid) - N(Amount Due)
          Penalties          = IF(DATEVALUE(LEFT(DatePaid,10)) > DATEVALUE(DateDue)+2, 3000, 0)
    """

    # --- detect/insert header (uses your robust detector from the previous block) ---
    header_row0, header, colmap = _detect_or_create_header(tenant_ws)
    missing = [k for k in REQUIRED_KEYS if k not in colmap]
    if missing:
        raise ValueError(f"Sheet '{tenant_ws.title}' missing required columns after normalization: {missing}")

    # Reload values from header row downward
    all_vals = tenant_ws.get_all_values()
    vals = all_vals[header_row0:]
    base_row_1based = header_row0 + 1

    # --- find or create the month row ---
    month_key, pay_dt = _month_key_from_date_str(payment['Date Paid'])
    row_rel = _find_month_row(vals, colmap['month'], month_key)
    if row_rel is None:
        new_row = [''] * len(header)
        new_row[colmap['month']] = month_key
        new_row[colmap['amount_due']] = '0'
        new_row[colmap['amount_paid']] = '0'
        new_row[colmap['date_paid']] = ''
        new_row[colmap['ref']] = ''
        # Set Date due as the previous row's date due plus one month.
        # Try to get last row's Date due (skip header row)
        if len(vals) > 1 and vals[-1][colmap['date_due']]:
            try:
                last_date_due = datetime.strptime(vals[-1][colmap['date_due']], "%d/%m/%Y").replace(day=5)
                new_date_due = last_date_due + relativedelta(months=1)
            except Exception:
            # Fallback to payment date plus one month if parsing fails
                new_date_due = datetime.strptime(payment['Date Paid'], '%d/%m/%Y %I:%M %p') + relativedelta(months=1)
        else:
            new_date_due = datetime.strptime(payment['Date Paid'], '%d/%m/%Y %I:%M %p') + relativedelta(months=1)
            new_row[colmap['date_due']] = new_date_due.strftime("%d/%m/%Y")
            
        # prepay/arrears and penalties will be set as FORMULAS after append
        tenant_ws.append_row(new_row, value_input_option='USER_ENTERED')
        all_vals = tenant_ws.get_all_values()
        vals = all_vals[header_row0:]
        row_rel = len(vals) - 1

    row_abs_1based = base_row_1based + row_rel
    row = vals[row_rel]

    # --- helpers to coerce numbers/strings ---
    def _num(v):
        try:
            s = str(v).replace(',','').strip()
            return float(s) if s else 0.0
        except:
            return 0.0
    def _str(v):
        return '' if v is None else str(v)

    # current row values
    due0   = _num(row[colmap['amount_due']])
    paid0  = _num(row[colmap['amount_paid']])
    ref0   = _str(row[colmap['ref']])

    pay_amt = float(payment['Amount Paid'])

    # (if you previously tracked arrears carryover in this cell, you can ignore that here
    #  because the balance is now a live formula: Paid - Due)
    paid1 = paid0 + pay_amt

    # --- 1) write the three direct fields ---
    updates = {
        colmap['amount_paid']:  paid1,
        colmap['date_paid']:    payment['Date Paid'],
        colmap['ref']:          (payment['REF Number'] if not ref0 else f"{ref0}, {payment['REF Number']}")
    }

    # compact range write
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

    # --- 2) ensure the formula cells are present (set once; they’ll recalc automatically) ---
    col_letters = {k: _col_letter(row_abs_1based, colmap[k] + 1) for k in colmap}
    # addresses for this row:
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
    

    # Only set if not already a formula (so we don't overwrite intentional manual values)
    current_bal = tenant_ws.acell(bal_addr).value or ""
    current_pen = tenant_ws.acell(pen_addr).value or ""
    needs_bal = not str(current_bal).startswith("=")
    needs_pen = not str(current_pen).startswith("=")

    # Set any missing formulas in a single batch
    body = []
    if needs_bal:
        body.append({'range': bal_addr, 'values': [[bal_formula]]})
    if needs_pen:
        body.append({'range': pen_addr, 'values': [[pen_formula]]})
    if body:
        tenant_ws.batch_update(body, value_input_option='USER_ENTERED')

    # Return info (no computed numbers now—Sheet will reflect in realtime)
    return {
        'sheet': tenant_ws.title,
        'month_row': row_abs_1based,
        'paid_before': paid0,
        'paid_after': paid1,
        'ref_added': payment['REF Number'],
        'formulas_set': {'balance': needs_bal, 'penalties': needs_pen},
        'balance_addr': bal_addr,    
        'penalties_addr': pen_addr       
    }



# ----- META SHEETS (ProcessedRefs, PaymentHistory) -----
def ensure_meta(ws_name, header):
    try:
        ws = sh.worksheet(ws_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(ws_name, rows=2000, cols=max(10, len(header)))
        ws.append_row(header)
    return ws

refs_ws = ensure_meta("ProcessedRefs", ["Ref"])
hist_ws = ensure_meta("PaymentHistory", PAYMENT_COLS + ['AccountCode','TenantSheet','Month'])

# Load processed refs into a set
ref_vals = refs_ws.get_all_values()
processed_refs = set((r[0] or '').upper() for r in ref_vals[1:]) if len(ref_vals) > 1 else set()

# ----- GMAIL FETCH + PARSE -----
print("🔎 Searching Gmail…")
result = gmail.users().messages().list(userId="me", q=GMAIL_QUERY, maxResults=200).execute()
msg_list = result.get("messages", [])
print(f"Found {len(msg_list)} candidate emails.")

parsed, errors = [], []
for m in msg_list:
    try:
        text = get_message_text(gmail, m["id"])
        pay = parse_email(text)
        if not pay:
            errors.append(f"Could not parse message id {m['id']}")
            continue
        if pay['REF Number'] in processed_refs:
            continue
        parsed.append((m["id"], pay))
    except Exception as e:
        errors.append(f"Error reading message {m['id']}: {e}")

print(f"✅ Parsed {len(parsed)} new payments.")

# ----- APPLY: to tenant sheets + PaymentHistory + ProcessedRefs -----
logs = []
tenant_tally = {}

# Cache worksheets to reduce calls
worksheets = {ws.title: ws for ws in sh.worksheets()}

def find_or_create_tenant_sheet(account_code: str):
    for title, ws in worksheets.items():
        t = title.upper()
        if t.startswith(account_code) and 'PROCESSEDREFS' not in t and 'PAYMENTHISTORY' not in t:
            return ws
    title = f"{account_code} - AutoAdded"
    ws = sh.add_worksheet(title, rows=1000, cols=12)
    ws.update(values=[['Month','Amount Due','Amount paid','Date paid','REF Number','Date due','Prepayment/Arrears','Penalties']],
              range_name='A1', value_input_option='USER_ENTERED')
    ws.format('1:1', {'textFormat': {'bold': True}})
    ws.freeze(rows=1)
    worksheets[title] = ws
    logs.append(f"➕ Created tenant sheet: {title}")
    return ws

for msg_id, p in parsed:
    tenant_ws = find_or_create_tenant_sheet(p['AccountCode'])
    info = update_tenant_month_row(tenant_ws, p)
    # Read the live, recalculated values
    logs.append(
    f"🧾 {info['sheet']} R{info['month_row']} | "
    f"Paid {info['paid_before']}→{info['paid_after']} | "
    f"Ref {info['ref_added']} | Bal/penalties will auto-update in sheet"
    )
    
    tenant_tally[info['sheet']] = tenant_tally.get(info['sheet'], 0) + 1

    # PaymentHistory
    dt = datetime.strptime(p['Date Paid'], '%d/%m/%Y %I:%M %p')
    mon = dt.strftime('%Y-%m')
    hist_ws.append_row(
        [p[k] for k in PAYMENT_COLS] + [p['AccountCode'], tenant_ws.title, mon],
        value_input_option='USER_ENTERED'
    )

    # ProcessedRefs
    refs_ws.append_row([p['REF Number']], value_input_option='RAW')
    processed_refs.add(p['REF Number'])

    # Mark Gmail read (optional)
    try:
        gmail.users().messages().modify(userId='me', id=msg_id, body={'removeLabelIds': ['UNREAD']}).execute()
    except HttpError:
        pass

    time.sleep(2)  # throttle writes

# ----- GROUPED MONTHLY SUMMARY (display) -----
hist_vals = hist_ws.get_all_values()
if len(hist_vals) > 1:
    df = pd.DataFrame(hist_vals[1:], columns=hist_vals[0])
    with pd.option_context('display.float_format', '{:,.2f}'.format):
        df['Amount Paid'] = pd.to_numeric(df['Amount Paid'], errors='coerce').fillna(0.0)
        grouped = df.groupby('Month', dropna=False).agg(
            Payments=('REF Number','count'),
            TotalAmount=('Amount Paid','sum')
        ).reset_index().sort_values('Month')
        display(grouped)
else:
    print("No payment history yet.")

# ----- LOGS -----
print("\n------ BOT LOG ------")
for line in logs:
    print(line)
print("\nPayments per tenant sheet:")
for t, c in tenant_tally.items():
    print(f"  {t}: {c} payment(s)")
if errors:
    print("\nNon-fatal parse/read issues:")
    for e in errors:
        print("  -", e)
print("\n✅ Prototype run complete.")
