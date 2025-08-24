
# streamlit_app.py (v2) — quota-hardened
import json, time, base64, re
import streamlit as st
import pandas as pd
from datetime import datetime

from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
import gspread
from gspread.exceptions import APIError
from gspread.utils import ValueInputOption

from bot_logic_merged import (
                 PATTERN, 
                 parse_email, 
                 update_tenant_month_row, 
                 PAYMENT_COLS
)

# ---------- Streamlit UI config ----------
st.set_page_config(page_title="Rent RPA (Gmail → Sheets)", page_icon="🏠", layout="wide")
st.title("🏠 Rent RPA — Gmail → Google Sheets")
st.caption("User-owned OAuth. Writes payment info from Gmail to your rent tracker sheet.")
st.divider()
st.header("About")
st.markdown("""
Scans Gmail for rent payment emails and logs them into a Google Sheet.
- OAuth 2.0 (your account, your tokens)
- Gmail API for search + parsing
- Google Sheets API for appends/updates
""")

# ---------- OAuth config from Streamlit Secrets ----------
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
            "project_id": "Rent-rpa",
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
        return Credentials.from_authorized_user_info(json.loads(st.session_state["creds_json"]), SCOPES)
    return None

def store_creds(creds: Credentials):
    st.session_state["creds_json"] = creds.to_json()

# OAuth: refresh if possible
creds = get_creds()
if creds and not creds.valid and creds.refresh_token:
    try:
        creds.refresh(Request())
        store_creds(creds)
    except Exception:
        creds = None

# Callback before gate
params = st.query_params
if "code" in params and "state" in params and "creds_json" not in st.session_state:
    flow = build_flow()
    flow.fetch_token(code=params["code"])
    creds = flow.credentials
    store_creds(creds)
    st.query_params.clear()
    st.success("Signed in to Google successfully.")
    st.rerun()

# Auth gate
if not creds or not creds.valid:
    flow = build_flow()
    auth_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",  # must be string per Google API
        prompt="consent",
    )
    st.link_button("🔐 Sign in with Google", auth_url, use_container_width=True)
    st.stop()

# ---------- UI ----------
sheet_url = st.text_input(
    "Google Sheet URL (your rent tracker):",
    placeholder="https://docs.google.com/spreadsheets/d/xxxxxxxxxxxxxxxxxxxxxxxxxxxx/edit#gid=0"
)
gmail_query = st.text_input(
    "Gmail search query:",
    value='PAYLEMAIYAN subject:"NCBA TRANSACTIONS STATUS UPDATE" newer_than:365d',
    help='Use Gmail operators. Add "is:unread" when confident.'
)
colA, colB, colC, colD = st.columns([1,1,1,1])
with colA:
    mark_read = st.checkbox("Mark processed emails as Read", value=True)
with colB:
    throttle_ms = st.number_input("Throttle per Sheets write (ms)", min_value=0, value=250, step=50)
with colC:
    max_results = st.number_input("Max messages to scan", min_value=10, max_value=500, value=200, step=10)
with colD:
    batch_size = st.number_input("Append batch size", min_value=10, max_value=200, value=50, step=10)

run = st.button("▶️ Run Bot", type="primary", use_container_width=True)
st.divider()

# ---------- Helpers ----------
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
    for i in range(6):  # ~1+2+4+8+16+32 = 63s worst case
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            if hasattr(e, "response") and e.response.status_code == 429:
                time.sleep(delay)
                delay *= 2
                continue
            raise
    return fn(*args, **kwargs)

# ---------- Main ----------
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

    # meta sheets
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
        with_backoff(ws.update, range_name='A1',
                     values=[['Month','Amount Due','Amount paid','Date paid','REF Number','Date due','Prepayment/Arrears','Penalties']],
                     value_input_option=ValueInputOption.user_entered)
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

        logs.append(
            f"🧾 {info['sheet']} R{info['month_row']} | Paid {info['paid_before']}→{info['paid_after']} | Ref {info['ref_added']} | formulas set: {info['formulas_set']}"
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

    hist_vals = with_backoff(hist_ws.get_all_values)
    if len(hist_vals) > 1:
        df = pd.DataFrame(hist_vals[1:], columns=hist_vals[0])
        with pd.option_context('display.float_format', '{:,.2f}'.format):
            df["Amount Paid"] = pd.to_numeric(df["Amount Paid"], errors="coerce").fillna(0.0)
            grouped = df.groupby("Month", dropna=False).agg(
                Payments=("REF Number","count"),
                TotalAmount=("Amount Paid","sum")
            ).reset_index().sort_values("Month")
            st.subheader("Payment History — Grouped by Month")
            st.dataframe(grouped, use_container_width=True)
    else:
        st.info("No PaymentHistory yet.")

    st.success("Done.")
    st.subheader("Run Log")
    if logs:
        st.code("\n".join(logs), language="text")
    if errors:
        st.subheader("Non-fatal Parse/Read Errors")
        st.code("\n".join(errors), language="text")
