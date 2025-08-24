# streamlit_app.py
# Loading necessary libraries

import json
import time
import base64
import streamlit as st
import pandas as pd
from datetime import datetime

from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import gspread
from google_auth_oauthlib.helpers import session_from_client_config
from gspread.utils import ValueInputOption

from bot_logic import (
    PATTERN,
    parse_email,
    update_tenant_month_row,
    PAYMENT_COLS,
)

# ---------- Streamlit UI config ----------
st.set_page_config(page_title="Rent RPA (Gmail → Sheets)", page_icon="🏠", layout="wide")
st.title("🏠 Rent RPA — Gmail → Google Sheets")
st.caption("User-owned OAuth. Writes payment info from Gmail to your rent tracker sheet.")
st.divider()
st.header("About")

st.markdown("""
A Streamlit app that scans your Gmail for rent payment emails (e.g. from NCBA) and logs them into a Google Sheet.
- **Gmail**: Uses Gmail API to search for emails matching your query, and extracts payment details using regex.
- **Google Sheets**: Uses Google Sheets API to append payment records to your rent tracker sheet, creating tenant sheets as needed.
- **OAuth**: Uses OAuth 2.0 for secure access to your Gmail and Google Sheets data. You control the permissions.
- **No backend**: Runs entirely in Streamlit with user-owned credentials. No data is stored on any server.
""")

# ---------- OAuth config from Streamlit Secrets ----------
CLIENT_ID     = st.secrets["google_oauth"]["client_id"]
CLIENT_SECRET = st.secrets["google_oauth"]["client_secret"]
REDIRECT_URI  = st.secrets["google_oauth"]["redirect_uri"]

SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/spreadsheets",
]

# ---------- OAuth helpers ----------
def build_flow():
    client_config = {
        "web": {
            "client_id": CLIENT_ID,
            "project_id": "rent-rpa",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_secret": CLIENT_SECRET,
            "redirect_uris": [REDIRECT_URI],
            # optional but nice to have:
            "javascript_origins": [REDIRECT_URI.rstrip("/")],
        }
    }
    return Flow.from_client_config(client_config, scopes=SCOPES, redirect_uri=REDIRECT_URI)

#---------- OAuth state in Streamlit session ----------
def get_creds():
    if "creds_json" in st.session_state:
        return Credentials.from_authorized_user_info(json.loads(st.session_state["creds_json"]), SCOPES)
    return None

# Store creds in session state
def store_creds(creds):
    st.session_state["creds_json"] = creds.to_json()

# Handle OAuth callback
params = st.query_params
if "code" in params and "state" in params and "creds_json" not in st.session_state:
    flow = build_flow()
    flow.fetch_token(code=params["code"])
    creds = flow.credentials
    store_creds(creds)
    st.query_params.clear()
    st.success("Signed in to Google successfully.")

creds = get_creds()
if not creds or not creds.valid:
    flow = build_flow()
    auth_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent"
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
colA, colB, colC = st.columns([1,1,1])
with colA:
    mark_read = st.checkbox("Mark processed emails as Read", value=True)
with colB:
    throttle_ms = st.number_input("Throttle per Sheets write (ms)", min_value=0, value=250, step=50)
with colC:
    max_results = st.number_input("Max messages to scan", min_value=10, max_value=500, value=200, step=10)

run = st.button("▶️ Run Bot", type="primary", use_container_width=True)
st.divider()


# ---------- Main logic ----------
def extract_sheet_id(url: str) -> str:
    try:
        return url.split("/d/")[1].split("/")[0]
    except Exception:
        return ""

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
        for p in parts or []:
            walk(p)
    walk(payload)
    if body_texts:
        return "\n".join(body_texts)
    return msg.get("snippet", "")

# Run bot
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

    # Open sheet (user must be owner/editor)
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
            ws = sh.add_worksheet(ws_name, rows=2000, cols=max(10, len(header)))
            ws.append_row(header)
        return ws

    refs_ws = ensure_meta("ProcessedRefs", ["Ref"])
    hist_ws = ensure_meta("PaymentHistory", PAYMENT_COLS + ['AccountCode','TenantSheet','Month'])

    # Load processed refs
    ref_vals = refs_ws.get_all_values()
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
            if pay['REF Number'] in processed_refs:
                continue
            parsed.append((m["id"], pay))
        except Exception as e:
            errors.append(f"Error reading message {m['id']}: {e}")

    st.success(f"Parsed {len(parsed)} new payments.")

    # Process each payment
    st.write("✍️ Updating Google Sheet…")
    logs = []
    worksheets = {ws.title: ws for ws in sh.worksheets()}

    def find_or_create_tenant_sheet(account_code: str):
        for title, ws in worksheets.items():
            t = title.upper()
            if t.startswith(account_code) and 'PROCESSEDREFS' not in t and 'PAYMENTHISTORY' not in t:
                return ws
        title = f"{account_code} - AutoAdded"
        ws = sh.add_worksheet(title, rows=1000, cols=12)
        ws.update(values=[['Month','Amount Due','Amount paid','Date paid','REF Number','Date due','Prepayment/Arrears','Penalties']],
                  range_name='A1', value_input_option=ValueInputOption.user_entered)
        ws.format('1:1', {'textFormat': {'bold': True}})
        ws.freeze(rows=1)
        worksheets[title] = ws
        logs.append(f"➕ Created tenant sheet: {title}")
        return ws

    for msg_id, p in parsed:
        ws = find_or_create_tenant_sheet(p["AccountCode"])
        info = update_tenant_month_row(ws, p)

        # Quota-safe log (don’t read computed cells)
        logs.append(
            f"🧾 {info['sheet']} R{info['month_row']} | "
            f"Paid {info['paid_before']}→{info['paid_after']} | "
            f"Ref {info['ref_added']} | formulas set: {info['formulas_set']}"
        )

        # PaymentHistory append
        dt = datetime.strptime(p["Date Paid"], "%d/%m/%Y %I:%M %p")
        mon = dt.strftime("%Y-%m")
        
        hist_ws.append_row(
            [p[k] for k in PAYMENT_COLS] + [p['AccountCode'], ws.title, mon],
            value_input_option=ValueInputOption.user_entered
        )

        # ProcessedRefs append
        refs_ws.append_row([p["REF Number"]], value_input_option=ValueInputOption.raw)
        processed_refs.add(p["REF Number"])

        if mark_read:
            try:
                gmail.users().messages().modify(
                    userId="me", id=msg_id, body={"removeLabelIds": ["UNREAD"]}
                ).execute()
            except Exception:
                pass

        if throttle_ms > 0:
            time.sleep(throttle_ms / 1000.0)

    # Show grouped PaymentHistory
    hist_vals = hist_ws.get_all_values()
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
