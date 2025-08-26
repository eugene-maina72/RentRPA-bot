# streamlit_app.py — Rent RPA (Gmail → Google Sheets)
import json, time, base64, re
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, timezone

from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
import gspread
from gspread.exceptions import APIError

from bot_logic import (
    PATTERN,
    parse_email,
    update_tenant_month_row,
    PAYMENT_COLS,
    clear_cache,           # optional: to reset per-sheet cache
)

# ---------------- UI CONFIG & TOP HELP ----------------
st.set_page_config(page_title="Lemaiyan Rent RPA (Gmail → Sheets)", page_icon="🏠", layout="wide")
st.title("🏠 Lemaiyan Rent RPA — Gmail → Google Sheets")

st.markdown(
    """
<div style="padding:10px;border:1px solid #ddd;border-radius:8px;background:#0000ff;margin-bottom:8px">
<b>How it works:</b> Scans Gmail for rent payments and logs them into your Google Sheet (per-tenant tabs).<br>
<b>Rules:</b> Date due is always the <b>5th</b>; 3,000 KES penalty if paid >2 days after due. Rolling Prepayment/Arrears.<br>
<b>Notes:</b> Header row is auto-detected (row 7 common). Existing headers are not renamed or deleted. <b>Comments</b> is preserved.
</div>
""",
    unsafe_allow_html=True,
)
st.caption("Uses your own Google OAuth. Nothing is stored server-side beyond standard tokens in memory.")

st.divider()

# ---------------- OAUTH CONFIG ----------------
# ---------------- OAUTH CONFIG ----------------
SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/gmail.readonly",
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
            "javascript_origins": [REDIRECT_URI],
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

# Refresh if possible
creds = get_creds()
if creds and not creds.valid and creds.refresh_token:
    try:
        creds.refresh(Request())
        store_creds(creds)
    except Exception:
        creds = None

# ---------- OAuth entrypoint ----------
if not creds or not creds.valid:
    # If we’re returning from Google with ?code=... handle it first
    params = dict(st.query_params)
    if "code" in params:
        # Validate state to prevent CSRF & accidental reruns with stale codes
        if "state" in params and params["state"] != st.session_state.get("oauth_state"):
            st.error("OAuth state mismatch. Try signing in again.")
            st.query_params.clear()
            st.stop()

        flow = build_flow()
        try:
            # Primary: exchange using the code (redirect_uri is already set on the flow)
            flow.fetch_token(code=params["code"])
        except Exception:
            # Fallback: build the full authorization_response URL and retry
            try:
                # Reconstruct ?query string
                q = "&".join(f"{k}={v}" for k, v in params.items())
                authorization_response = f"{REDIRECT_URI}?{q}"
                flow.fetch_token(authorization_response=authorization_response)
            except Exception as e:
                st.error(
                    "OAuth token exchange failed. "
                    "Check that your Google Cloud OAuth **Authorized redirect URI** "
                    f"matches exactly: {REDIRECT_URI}\n\n"
                    "Also ensure the OAuth client type is **Web application**, not Desktop, "
                    "and try again (incognito can help)."
                )
                st.query_params.clear()
                st.stop()

        creds = flow.credentials
        store_creds(creds)
        st.query_params.clear()
        st.success("Signed in to Google successfully.")
        st.rerun()

    # No creds and not returning from Google: start the flow
    flow = build_flow()
    auth_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",  # must be "true"/"false" string
        prompt="consent",
    )
    st.session_state["oauth_state"] = state
    st.session_state["oauth_redirect_uri"] = REDIRECT_URI

    st.link_button(
        "🔐 Sign in with Google",
        auth_url,
        help="Authorize Gmail & Sheets access",
        use_container_width=True,
    )
    st.stop()


# ---------------- INPUTS ----------------
sheet_url = st.text_input(
    "Google Sheet URL",
    placeholder="https://docs.google.com/spreadsheets/d/xxxxxxxxxxxxxxxxxxxxxxxxxxxx/edit#gid=0",
    help="Use a *Google Sheet* (not .xlsx). If you uploaded Excel, File → Save as Google Sheets first."
)
gmail_query = st.text_input(
    "Gmail search query",
    value='PAYLEMAIYAN subject:"NCBA TRANSACTIONS STATUS UPDATE" newer_than:365d',
    help='Use Gmail operators (e.g., is:unread, after:, before:). Narrow to avoid quota issues.'
)
c1, c2, c3, c4, c5 = st.columns([1,1,1,1,1])
with c1:
    mark_read = st.checkbox("Mark processed as Read", value=True, help="Remove UNREAD label from processed emails.")
with c2:
    throttle_ms = st.number_input("Throttle (ms) between writes", min_value=0, value=200, step=50, help="Backoff to avoid 429s.")
with c3:
    max_results = st.number_input("Max messages to scan", min_value=10, max_value=1000, value=200, step=10)
with c4:
    weekly_auto = st.checkbox("Enable weekly automation", value=False, help="Runs Mondays 09:00 EAT while app is open.")
with c5:
    clear_before_run = st.checkbox("Reset in-memory cache each run", value=True, help="Safer if sheet structure changes mid-session.")

run_now = st.button("▶️ Run Bot Now", type="primary", use_container_width=True)
st.caption("Automation triggers only when the app is open. We never run unattended without your checkbox enabled.")

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

def with_backoff(fn, *args, **kwargs):
    delay = 1.0
    for _ in range(6):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            # Sheets quota throttling
            if hasattr(e, "response") and getattr(e.response, "status_code", None) == 429:
                time.sleep(delay); delay *= 2; continue
            raise
        except Exception as e:
            if "Rate Limit Exceeded" in str(e) or "quota" in str(e).lower():
                time.sleep(delay); delay *= 2; continue
            raise

def should_auto_run():
    if not weekly_auto: return False
    # Fire Mondays 09:00 EAT (UTC+3) when app is open
    now_utc = datetime.now(timezone.utc)
    eat = now_utc + timedelta(hours=3)
    in_window = (eat.weekday() == 0 and eat.hour == 9)
    last = st.session_state.get("last_auto_run_at")
    if in_window and (last is None or (now_utc - last) > timedelta(days=6)):
        return True
    return False

auto_trigger = should_auto_run()
if auto_trigger:
    st.info("🤖 Weekly automation window detected — running now.")
run_now = run_now or auto_trigger

# ---------------- MAIN ----------------
if run_now:
    if clear_before_run:
        clear_cache()

    if not sheet_url:
        st.error("Please paste your Google Sheet URL."); st.stop()
    sheet_id = extract_sheet_id(sheet_url)
    if not sheet_id:
        st.error("That doesn't look like a valid Google Sheet URL."); st.stop()

    gmail = build("gmail", "v1", credentials=creds)
    gs = gspread.authorize(creds)

    # Open sheet
    try:
        sh = gs.open_by_key(sheet_id)
    except Exception as e:
        st.error(f"Could not open the Google Sheet. Ensure you own it or have edit access.\n\n{e}")
        st.stop()

    # Ensure meta sheets
    def ensure_meta(ws_name, header):
        try:
            ws = sh.worksheet(ws_name)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=ws_name, rows=2000, cols=max(10, len(header)))
            with_backoff(ws.update, "1:1", [header], value_input_option="USER_ENTERED")
        return ws

    refs_ws = ensure_meta("ProcessedRefs", ["Ref"])
    hist_ws = ensure_meta("PaymentHistory", PAYMENT_COLS + ['AccountCode','TenantSheet','Month'])

    # Load processed refs
    ref_vals = with_backoff(refs_ws.get_all_values)
    processed_refs = set((r[0] or '').upper() for r in ref_vals[1:]) if len(ref_vals) > 1 else set()

    st.write("🔎 Searching Gmail…")
    result = gmail.users().messages().list(userId="me", q=gmail_query, maxResults=int(max_results)).execute()
    messages = result.get("messages", [])
    st.write(f"Found {len(messages)} candidate emails.")

    parsed, errors = [], []
    for m in messages:
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
    worksheets = {ws.title: ws for ws in sh.worksheets()}

    # Create a tenant sheet if missing (use row 7 for header to match your schema)
    def find_or_create_tenant_sheet(account_code: str):
        acct = (account_code or '').strip().upper()
        for title, ws in worksheets.items():
            t = title.strip().upper()
            if t.startswith(acct) and 'PROCESSEDREFS' not in t and 'PAYMENTHISTORY' not in t:
                return ws
        title = f"{account_code} - AutoAdded"
        ws = sh.add_worksheet(title=title, rows=2000, cols=20)
        # Write header on row 7
        hdr = ['Month','Amount Due','Amount paid','Date paid','REF Number','Date due','Prepayment/Arrears','Penalties','Comments']
        with_backoff(ws.update, "7:7", [hdr], value_input_option="USER_ENTERED")
        try:
            ws.freeze(rows=7)
        except Exception:
            pass
        worksheets[title] = ws
        st.info(f"➕ Created tenant sheet: {title}")
        return ws

    # Batch appends to meta
    hist_rows, ref_rows, logs = [], [], []

    for idx, (msg_id, p) in enumerate(parsed, start=1):
        ws = find_or_create_tenant_sheet(p["AccountCode"])
        info = update_tenant_month_row(ws, p)

        logs.append(
            f"🧾 {info.get('sheet')} R{info.get('row')} | {info.get('month')} | "
            f"Paid {info.get('paid_before')}→{info.get('paid_after')} | Ref {p.get('REF Number')}"
        )

        dt = datetime.strptime(p["Date Paid"], "%d/%m/%Y %I:%M %p")
        mon = dt.strftime("%Y-%m")
        hist_rows.append([p[k] for k in PAYMENT_COLS] + [p['AccountCode'], ws.title, mon])

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

        # Flush in batches
        if len(hist_rows) >= 100 or idx == len(parsed):
            with_backoff(hist_ws.append_rows, hist_rows, value_input_option="USER_ENTERED")
            hist_rows.clear()
            with_backoff(refs_ws.append_rows, ref_rows, value_input_option="RAW")
            ref_rows.clear()

    # ---------- RUN OUTPUT ----------
    st.success("Ingestion complete.")
    st.subheader("Run Log")
    if logs:
        st.code("\n".join(logs), language="text")
    if errors:
        st.subheader("Non-fatal Parse/Read Errors")
        st.code("\n".join(errors), language="text")

    # ---------- METRICS ----------
    st.subheader("📊 Portfolio Metrics")

    # PaymentHistory aggregates
    hist_vals = with_backoff(hist_ws.get_all_values)
    income_this_month = 0.0
    if len(hist_vals) > 1:
        df_hist = pd.DataFrame(hist_vals[1:], columns=hist_vals[0])
        for col in ("Amount Paid",):
            df_hist[col] = pd.to_numeric(df_hist[col], errors="coerce").fillna(0.0)
        this_month = datetime.now().strftime("%Y-%m")
        income_this_month = float(df_hist.loc[df_hist["Month"] == this_month, "Amount Paid"].sum())
        grouped = df_hist.groupby("Month", dropna=False).agg(
            Payments=("REF Number","count"),
            TotalAmount=("Amount Paid","sum")
        ).reset_index().sort_values("Month")
        st.markdown("**Payment History — by Month**")
        st.dataframe(grouped, use_container_width=True)

    # Per-tenant balances & penalties
    total_prepay = 0.0
    total_arrears = 0.0
    penalty_freq = {}
    def parse_float(x):
        try:
            return float(str(x).replace(",", "").strip())
        except Exception:
            return 0.0

    def detect_header_row(all_vals, scan_rows: int = 30) -> int:
        def score(row):
            s = 0
            for cell in row:
                t = str(cell or "").strip().lower()
                if not t: continue
                if t in {"month","amount due","amount paid","date paid","ref number","date due","prepayment/arrears","penalties","comments"}:
                    s += 1
            return s
        best_i, best_score = 0, 0
        limit = min(len(all_vals), max(1, scan_rows))
        for i in range(limit):
            sc = score(all_vals[i])
            if sc > best_score:
                best_i, best_score = i, sc
        return best_i if best_score >= 3 else 0

    for ws in sh.worksheets():
        name = ws.title.upper()
        if name in ("PAYMENTHISTORY", "PROCESSEDREFS"):
            continue
        try:
            vals = with_backoff(ws.get_all_values)
            if not vals: continue
            header_row0 = detect_header_row(vals)
            header = [c.strip() for c in vals[header_row0]]
            rows = vals[header_row0+1:]
            if not rows: continue

            def idx(colname):
                try: return header.index(colname)
                except ValueError: return -1

            i_bal = idx("Prepayment/Arrears")
            i_pen = idx("Penalties")
            i_mon = idx("Month")

            # Latest balance = last populated Month row (or last row)
            latest_row = None
            for r in reversed(rows):
                if i_mon != -1 and len(r) > i_mon and str(r[i_mon]).strip():
                    latest_row = r; break
            if latest_row is None: latest_row = rows[-1]

            if i_bal != -1 and len(latest_row) > i_bal:
                bal = parse_float(latest_row[i_bal])
                if bal > 0: total_prepay += bal
                elif bal < 0: total_arrears += abs(bal)

            if i_pen != -1:
                cnt = 0
                for r in rows:
                    if len(r) > i_pen and parse_float(r[i_pen]) > 0:
                        cnt += 1
                acct = ws.title.split(" - ")[0].strip().upper()
                penalty_freq[acct] = penalty_freq.get(acct, 0) + cnt

        except Exception:
            continue

    c1, c2, c3 = st.columns(3)
    c1.metric("Income (this month)", f"{income_this_month:,.0f} KES")
    c2.metric("Total Prepayments", f"{total_prepay:,.0f} KES")
    c3.metric("Total Arrears", f"{total_arrears:,.0f} KES")

    if penalty_freq:
        df_pen = pd.DataFrame(
            [{"AccountCode": k, "Penalty Rows": v} for k, v in penalty_freq.items()]
        ).sort_values("Penalty Rows", ascending=False)
        st.markdown("**Penalty frequency by AccountCode** (rows with penalties > 0):")
        st.dataframe(df_pen, use_container_width=True)

    if auto_trigger:
        st.session_state["last_auto_run_at"] = datetime.now(timezone.utc)

# ---------------- FOOTER ----------------
st.divider()
st.caption("Rent-RPA © {year}. Built by [Eugene Maina](https://github.com/eugene-maina72). — Need help? The top banner explains the flow; hover over inputs for tips."
           .format(year=datetime.now().year))

