
# streamlit_app.py — Rent RPA (Gmail → Sheets)
import json, time, base64, re, math
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
import gspread
from gspread.exceptions import APIError
from gspread.utils import ValueInputOption

from bot_logic import (
    PATTERN,
    parse_email,
    update_tenant_month_row,
    PAYMENT_COLS,
    clear_cache
)

# ---------------- UI CONFIG & FOOTER ----------------
st.set_page_config(page_title="Rent RPA (Gmail → Sheets)", page_icon="🏠", layout="wide")
st.title("🏠 Rent RPA — Gmail → Google Sheets")

# Persistent footer/help (always visible)
st.markdown(
    """
<div style="padding:10px;border:1px solid #ddd;border-radius:8px;background:#0000ff;margin-bottom:8px">
<b>Tips:</b>
• Paste a valid <i>Google Sheet</i> URL (not an .xlsx file). 
• Use a targeted Gmail query to avoid quota issues. 
• New tenant tabs are auto-created on first payment.
• <b>Date due is always the 5th</b> of the month. Penalties apply if paid > 2 days after due date.
• <b>Comments</b> column is for landlord/caretaker notes and is never overwritten by the bot.
</div>
""",
    unsafe_allow_html=True,
)

st.caption("User-owned OAuth. Writes payment info from Gmail to your rent tracker sheet. Credentials never leave your browser+Google.")

st.divider()
st.header("About")
st.markdown("""
Scans Gmail for rent payment emails and logs them into a Google Sheet.

- OAuth 2.0 (your Google account)
- Gmail API for search + parsing
- Google Sheets API for appends/updates
- Per-sheet caching & batch writes (quota-friendly)
""")

# ---------------- OAUTH CONFIG ----------------
SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.readonly"
]


ENV            = st.secrets.get("ENV", "local")
CLIENT_ID      = st.secrets["google_oauth"]["client_id"]
CLIENT_SECRET  = st.secrets["google_oauth"]["client_secret"]
REDIRECT_LOCAL = st.secrets["google_oauth"]["redirect_uri_local"]
REDIRECT_PROD  = st.secrets["google_oauth"]["redirect_uri_prod"]
REDIRECT_URI   = REDIRECT_PROD if ENV == "prod" else REDIRECT_LOCAL

def build_flow():
    client_config = {
        "web": {
            "client_id": CLIENT_ID,
            "project_id": "RentRPA",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_secret": CLIENT_SECRET,
            "redirect_uris": [REDIRECT_URI],
        }
    }
    return Flow.from_client_config(client_config, scopes=SCOPES, redirect_uri=REDIRECT_URI)

def get_creds():
    if "creds_json" in st.session_state:
        return Credentials.from_authorized_user_info(
            json.loads(st.session_state["creds_json"]), SCOPES
        )
    return None

def store_creds(creds: Credentials):
    st.session_state["creds_json"] = creds.to_json()

# Optional: sign out
col_signout, _ = st.columns([1, 5])
with col_signout:
    if st.button("Sign out", help="Clear cached OAuth and re-authenticate"):
        st.session_state.pop("creds_json", None)
        st.session_state.pop("oauth_state", None)
        st.rerun()

# Try refresh if we already have creds
creds = get_creds()
if creds and not creds.valid and creds.refresh_token:
    try:
        creds.refresh(Request())
        store_creds(creds)
    except Exception:
        creds = None

# ---------- OAuth entrypoint ----------
if not creds or not creds.valid:
    # Normalize query params (Streamlit may hand lists)
    params = {k: (v[0] if isinstance(v, list) else v) for k, v in st.query_params.items()}
    code = params.get("code")
    state_param = params.get("state")

    if code:
        expected_state = st.session_state.get("oauth_state")
        if expected_state and state_param and state_param != expected_state:
            st.warning("OAuth state mismatch (session likely restarted). Attempting token exchange anyway…")

        flow = build_flow()
        # Primary attempt
        try:
            flow.fetch_token(code=code)
        except Exception:
            # Fallback: try full authorization_response URL
            try:
                q = "&".join(f"{k}={v}" for k, v in params.items())
                authorization_response = f"{REDIRECT_URI}?{q}"
                flow.fetch_token(authorization_response=authorization_response)
            except Exception as e:
                st.error(
                    "OAuth token exchange failed.\n\n"
                    "Double-check Google Cloud Console → OAuth Client:\n"
                    f"• Authorized redirect URI: {REDIRECT_URI}\n"
                    "• Client type: Web application (not Desktop)\n"
                    "If issues persist, try a new incognito window."
                )
                st.query_params.clear()
                st.stop()

        creds = flow.credentials
        store_creds(creds)
        st.query_params.clear()
        st.success("Signed in to Google successfully.")
        st.rerun()

    # No code yet: start the auth flow
    flow = build_flow()
    auth_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",  # must be "true"/"false" strings
        prompt="consent",
    )
    st.session_state["oauth_state"] = state
    st.link_button(
        "🔐 Sign in with Google",
        auth_url,
        help="Authorize Gmail & Sheets access",
        use_container_width=True,
    )
    st.stop()




# ---------------- INPUTS ----------------
sheet_url = st.text_input(
    "Google Sheet URL (your rent tracker):",
    placeholder="https://docs.google.com/spreadsheets/d/xxxxxxxxxxxxxxxxxxxxxxxxxxxx/edit#gid=0",
    help="Open your Sheet in Google Sheets (not Excel in Drive) and paste the full URL here."
)
gmail_query = st.text_input(
    "Gmail search query",
    value='PAYLEMAIYAN subject:"NCBA TRANSACTIONS STATUS UPDATE" newer_than:365d',
    help='Use Gmail operators, e.g. is:unread, after:, before:. Narrow the scope to avoid API quota issues.'
)
colA, colB, colC, colD, colE, colF = st.columns([1,1,1,1,1,1])

with colA:
    mark_read = st.checkbox("Mark processed emails as Read", value=True, help="If checked, emails are marked Read after logging.")
with colB:
    throttle_ms = st.number_input("Throttle per Sheets write (ms)", min_value=0, value=250, step=50, help="Wait time between write batches. Increase if you hit 429 quota errors.")
with colC:
    max_results = st.number_input("Max Gmail messages to scan", min_value=10, max_value=1000, value=200, step=10, help="Upper bound on messages fetched for this run.")
with colD:
    batch_size = st.number_input("Append batch size", min_value=10, max_value=500, value=50, step=10, help="How many history/refs rows to append in one batch.")
with colE:
    automation_enabled = st.checkbox("Enable weekly automation", value=False, help="Runs automatically on Mondays 09:00 EAT while the app is open. Uncheck if worried about credentials.")
with colF:
    if st.button("Clear cached data", help="Clears per-sheet cached data (processed refs, tenant month rows). Use if you suspect stale cache issues."):
        clear_cache()
        st.success("Cache cleared.")
st.caption("Note: automation only triggers when this app is running (e.g., deployed on Streamlit Cloud or a server).")

run = st.button("▶️ Run Bot Now", type="primary", use_container_width=True)

# Automation trigger (opt-in, Monday 09:00 EAT; no background jobs — fires while app open)
def should_auto_run():
    if not automation_enabled:
        return False
    now = datetime.utcnow()  # treat as UTC; adjust to EAT (+3) for the window check
    eat_hour = (now.hour + 3) % 24
    eat_weekday = (now.weekday() + (1 if now.hour + 3 >= 24 else 0)) % 7 if False else now.weekday()  # keep simple
    # Trigger if it's Monday in EAT between 09:00–09:30 EAT and we haven't run in last 6 days
    in_window = (eat_weekday == 0) and (eat_hour == 9)
    last = st.session_state.get("last_auto_run_at")
    if in_window and (last is None or (datetime.utcnow() - last) > timedelta(days=6)):
        return True
    return False

auto_trigger = should_auto_run()
if auto_trigger:
    st.info("🤖 Weekly automation window detected — running bot.")
run = run or auto_trigger

st.divider()

# ---------------- HELPERS ----------------
def extract_sheet_id(url: str) -> str:
    try:
        return url.split("/d/")[1].split("/")[0]
    except Exception:
        return ""

def _decode_base64url(data: str) -> str:
    padding = '=' * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding).decode("utf-8", errors="ignore")

def _strip_html(html: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    return text

def get_message_text(service, msg_id):
    msg = service.users().messages().get(userId="me", id=msg_id, format="full").execute()
    payload = msg.get("payload", {})
    body_texts = []
    def walk(part):
        mime = part.get("mimeType", "")
        data = part.get("body", {}).get("data")
        parts = part.get("parts", [])
        if mime == "text/plain" and data:
            body_texts.append(_decode_base64url(data))
        elif mime == "text/html" and data and not body_texts:
            body_texts.append(_strip_html(_decode_base64url(data)))
        for p in parts or []:
            walk(p)
    walk(payload)
    if body_texts:
        return "\n".join(body_texts)
    return msg.get("snippet", "")

# Backoff wrapper for Sheets calls that may 429
def with_backoff(fn, *args, **kwargs):
    delay = 1.0
    for _ in range(6):  # ~63s worst case
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            if hasattr(e, "response") and e.response.status_code == 429:
                time.sleep(delay); delay *= 2; continue
            raise
    return fn(*args, **kwargs)

# ---------------- MAIN ----------------
if run:
    if not sheet_url:
        st.error("Please paste your Google Sheet URL.")
        st.stop()

    sheet_id = extract_sheet_id(sheet_url)
    if not sheet_id:
        st.error("That doesn't look like a valid Google Sheet URL.")
        st.stop()

    gmail = build("gmail", "v1", credentials=creds)
    gspread_client = gspread.authorize(creds)

    # Open sheet
    try:
        sh = gspread_client.open_by_key(sheet_id)
    except Exception as e:
        st.error(f"Could not open the Google Sheet. Ensure you own it or have edit access.\n\n{e}")
        st.stop()

    # Meta sheets
    def ensure_meta(ws_name, header):
        try:
            ws = sh.worksheet(ws_name)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=ws_name, rows=2000, cols=max(10, len(header)))
            ws.append_row(header)
        return ws

    refs_ws = ensure_meta("ProcessedRefs", ["Ref"])
    hist_ws = ensure_meta("PaymentHistory", PAYMENT_COLS + ['AccountCode','TenantSheet','Month'])

    # Load processed refs (normalize case)
    ref_vals = with_backoff(refs_ws.get_all_values)
    processed_refs = set((r[0] or '').upper() for r in ref_vals[1:]) if len(ref_vals) > 1 else set()

    st.write("🔎 Searching Gmail…")
    result = gmail.users().messages().list(userId="me", q=gmail_query, maxResults=int(max_results)).execute()
    msg_list = result.get("messages", [])
    st.write(f"Found {len(msg_list)} candidate emails.")

    parsed, errors = [], []
    for m in msg_list:
        try:
            text = get_message_text(gmail, m["id"])
            pay = parse_email(text)
            if not pay:
                errors.append(f"Could not parse message id {m['id']}")
                continue
            ref_norm = (pay.get('REF Number','') or '').upper()
            if ref_norm in processed_refs:
                continue
            parsed.append((m["id"], pay))
        except Exception as e:
            errors.append(f"Error reading message {m['id']}: {e}")

    st.success(f"Parsed {len(parsed)} new payments.")

    # Map worksheets, lazy-create when first seen
    worksheets = {ws.title: ws for ws in sh.worksheets()}
    def find_or_create_tenant_sheet(account_code: str):
        acct = (account_code or '').strip().upper()
        for title, ws in list(worksheets.items()):
            t = title.strip().upper()
            if t.startswith(acct) and 'PROCESSEDREFS' not in t and 'PAYMENTHISTORY' not in t:
                return ws
        title = f"{account_code} - AutoAdded"
        ws = sh.add_worksheet(title=title, rows=1000, cols=12)
        with_backoff(
            ws.update,
            range_name='A1',
            values=[['Month','Amount Due','Amount paid','Date paid','REF Number','Date due','Prepayment/Arrears','Penalties','Comments']],
            value_input_option=ValueInputOption.user_entered
        )
        try:
            ws.format('1:1', {'textFormat': {'bold': True}})
            ws.freeze(rows=1)
        except Exception:
            pass
        worksheets[title] = ws
        st.info(f"➕ Created tenant sheet: {title}")
        return ws

    # Prepare batched appends
    history_rows = []
    ref_rows = []
    logs = []

    for idx, (msg_id, p) in enumerate(parsed, start=1):
        ws = find_or_create_tenant_sheet(p["AccountCode"])
        info = update_tenant_month_row(ws, p)

        # Adapted to new bot_logic return keys
        logs.append(
            f"🧾 {info.get('sheet')} R{info.get('row')} | {info.get('month')} | "
            f"Paid {info.get('paid_before')}→{info.get('paid_after')} | Ref {p.get('REF Number')}"
        )

        dt = datetime.strptime(p["Date Paid"], "%d/%m/%Y %I:%M %p")
        mon = dt.strftime("%Y-%m")
        history_rows.append([p[k] for k in PAYMENT_COLS] + [p['AccountCode'], ws.title, mon])

        ref_val = (p.get("REF Number","") or "").upper()
        ref_rows.append([ref_val])
        processed_refs.add(ref_val)

        if mark_read:
            try:
                gmail.users().messages().modify(
                    userId="me", id=msg_id, body={"removeLabelIds": ["UNREAD"]}
                ).execute()
            except Exception:
                pass

        if throttle_ms > 0:
            time.sleep(throttle_ms / 1000.0)

        if len(history_rows) >= batch_size or idx == len(parsed):
            with_backoff(hist_ws.append_rows, history_rows, value_input_option=ValueInputOption.user_entered)
            history_rows.clear()
            with_backoff(refs_ws.append_rows, ref_rows, value_input_option=ValueInputOption.raw)
            ref_rows.clear()

    # ---------- DASHBOARDS ----------
    st.success("Ingestion complete.")
    st.subheader("Run Log")
    if logs:
        st.code("\n".join(logs), language="text")
    if errors:
        st.subheader("Non-fatal Parse/Read Errors")
        st.code("\n".join(errors), language="text")

    # 1) Payment History Aggregates (from PaymentHistory)
    hist_vals = with_backoff(hist_ws.get_all_values)
    if len(hist_vals) > 1:
        df_hist = pd.DataFrame(hist_vals[1:], columns=hist_vals[0])
        with pd.option_context('display.float_format', '{:,.2f}'.format):
            # Numeric cast
            for col in ("Amount Paid",):
                df_hist[col] = pd.to_numeric(df_hist[col], errors="coerce").fillna(0.0)

            # Grouped by Month
            grouped = df_hist.groupby("Month", dropna=False).agg(
                Payments=("REF Number","count"),
                TotalAmount=("Amount Paid","sum")
            ).reset_index().sort_values("Month")

            # This-month income metric
            this_month = datetime.now().strftime("%Y-%m")
            income_this_month = float(df_hist.loc[df_hist["Month"] == this_month, "Amount Paid"].sum())

            st.subheader("Payment History — Grouped by Month")
            st.dataframe(grouped, use_container_width=True)

    else:
        df_hist = pd.DataFrame(columns=PAYMENT_COLS + ['AccountCode','TenantSheet','Month'])
        income_this_month = 0.0
        st.info("No PaymentHistory yet.")

    # 2) Portfolio metrics from tenant sheets (arrears/prepayments & penalties)
    total_prepay = 0.0
    total_arrears = 0.0
    penalty_freq = {}  # AccountCode -> count of rows with penalties>0

    def parse_float(x):
        try:
            return float(str(x).replace(",", "").strip())
        except Exception:
            return 0.0

    for ws in sh.worksheets():
        name = ws.title.upper()
        if name in ("PAYMENTHISTORY", "PROCESSEDREFS"):
            continue
        try:
            vals = with_backoff(ws.get_all_values)
            if not vals:
                continue
            header = [c.strip() for c in vals[0]]
            def idx(colname):
                try: return header.index(colname)
                except ValueError: return -1

            i_bal = idx("Prepayment/Arrears")
            i_pen = idx("Penalties")
            i_mon = idx("Month")
            if i_bal == -1 and "Prepayment/Arrears" in header:
                i_bal = header.index("Prepayment/Arrears")

            rows = vals[1:]
            if not rows:
                continue

            # Latest balance (last non-empty in Month col preferred)
            # Fallback: last row
            latest_row = None
            for r in reversed(rows):
                if i_mon != -1 and len(r) > i_mon and str(r[i_mon]).strip():
                    latest_row = r; break
            if latest_row is None:
                latest_row = rows[-1]

            if i_bal != -1 and len(latest_row) > i_bal:
                bal = parse_float(latest_row[i_bal])
                if bal > 0:
                    total_prepay += bal
                elif bal < 0:
                    total_arrears += abs(bal)

            # Penalty frequency
            if i_pen != -1:
                cnt = 0
                for r in rows:
                    if len(r) > i_pen and parse_float(r[i_pen]) > 0:
                        cnt += 1
                acct = ws.title.split(" - ")[0].strip().upper()
                penalty_freq[acct] = penalty_freq.get(acct, 0) + cnt

        except APIError:
            continue
        except Exception:
            continue

    # Display metrics
    st.subheader("📊 Portfolio Metrics")
    m1, m2, m3 = st.columns(3)
    m1.metric("Income (this month)", f"{income_this_month:,.0f} KES")
    m2.metric("Total Prepayments", f"{total_prepay:,.0f} KES")
    m3.metric("Total Arrears", f"{total_arrears:,.0f} KES")

    if penalty_freq:
        df_pen = pd.DataFrame(
            [{"AccountCode": k, "Penalty Rows": v} for k, v in penalty_freq.items()]
        ).sort_values("Penalty Rows", ascending=False)
        st.markdown("**Penalty frequency by AccountCode** (rows with penalties > 0):")
        st.dataframe(df_pen, use_container_width=True)

    # mark automation time if auto
    if auto_trigger:
        st.session_state["last_auto_run_at"] = datetime.utcnow()

# Footer (hardwired caption)
st.divider()
st.caption("Rent-RPA © {year}. Built by Eugene Maina. — Need help? The top banner explains the flow; hover over inputs for tips."
           .format(year=datetime.now().year))
