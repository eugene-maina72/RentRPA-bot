# -*- coding: utf-8 -*-
"""
streamlit_app.py — RentRPA Streamlit UI

What this app does
------------------
- Authenticates to Google via OAuth (Gmail + Sheets scopes).
- Searches Gmail for payment alerts, parses them, and de-duplicates by REF.
- For each parsed payment, finds the matching tenant tab and updates the correct month row
  using `bot_logic.update_tenant_month_row` (which is header-row aware and non-destructive).
- Writes a clean PaymentHistory and ProcessedRefs to help with metrics and avoiding duplicates.
- Shows portfolio metrics (income this month, total prepayments/arrears, penalty frequency).
- Optional weekly automation (opt-in checkbox) runs Mondays 09:00 EAT *while the app is open*.

Key UX / Safety details
-----------------------
- The app never renames user headers or deletes columns/rows. It adds missing canonical columns to the far right.
- It assumes Date Due is always the 5th of the month, and computes Penalties + Prepayment/Arrears accordingly.
- The Comments column is never overwritten; new rows get "None" for Comments so a human can fill it in later.
"""

import json, time, base64, re
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
import gspread
from gspread.exceptions import APIError

# Extra: capture common OAuth errors to show helpful guidance
try:
    from oauthlib.oauth2.rfc6749.errors import (
        InvalidGrantError,
        MismatchingStateError,
        InvalidClientError,
        InvalidRequestError,
    )  # type: ignore
except Exception:  # pragma: no cover
    InvalidGrantError = Exception  # fallback if package surface changes
    MismatchingStateError = Exception
    InvalidClientError = Exception
    InvalidRequestError = Exception

from bot_logic import (
    PATTERN,
    parse_email,
    update_tenant_month_row,
    PAYMENT_COLS,
    clear_cache           # to reset per-sheet cache between runs
)

# ---------------------------------------------------------------------------
# 0) PAGE CHROME + TOP HELP
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Rent RPA (Gmail → Sheets)", page_icon="🏠", layout="wide")
st.title("🏠 Rent RPA — Gmail → Google Sheets")

st.markdown(
    """
<div style="padding:10px;border:1px solid #ddd;border-radius:8px;background:#0000ff;margin-bottom:8px">
<b>What this does:</b> Scans Gmail for rent payments and logs them into your Google Sheet (per-tenant tabs).<br>
<b>Rules:</b> Date due = <b>5th</b> of the month. <b>Penalty</b> = 3000 KES if payment > due + 2 days. <b>Prepayment/Arrears</b> rolls forward.<br>
<b>Safety:</b> Existing headers are preserved. Missing canonical columns are appended at the far right. <b>Comments</b> is never overwritten.
</div>
""",
    unsafe_allow_html=True,
)
st.caption("Uses your Google OAuth; tokens are stored in-memory only for the session. Nothing is persisted server-side.")

# ---------------------------------------------------------------------------
# 1) OAUTH CONFIG — lenient and debug-friendly
# ---------------------------------------------------------------------------

SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.readonly",
]
ENV            = st.secrets.get("ENV", "local")
CLIENT_ID      = st.secrets["google_oauth"]["client_id"]
CLIENT_SECRET  = st.secrets["google_oauth"]["client_secret"]
REDIRECT_LOCAL = st.secrets["google_oauth"]["redirect_uri_local"]
REDIRECT_PROD  = st.secrets["google_oauth"]["redirect_uri_prod"]
REDIRECT_URI   = REDIRECT_PROD if ENV == "prod" else REDIRECT_LOCAL


def build_flow(state: str | None = None) -> Flow:
    """Create an OAuth 2.0 Flow; `state` is persisted across the auth dance.
    Why: Google checks `state` to prevent CSRF — mismatches raise InvalidGrant.
    """
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
    # Pass both redirect and state so the callback Flow matches the initial one
    return Flow.from_client_config(
        client_config,
        scopes=SCOPES,
        redirect_uri=REDIRECT_URI,
        state=state,
    )


def get_creds():
    if "creds_json" in st.session_state:
        return Credentials.from_authorized_user_info(json.loads(st.session_state["creds_json"]), SCOPES)
    return None


def store_creds(creds: Credentials):
    st.session_state["creds_json"] = creds.to_json()


# ---------------------------------------------------------------------------
# 1a) OAUTH SETUP CHECKER — prevents Error 400: invalid_request
# ---------------------------------------------------------------------------

def oauth_setup_checker():
    """Static checks to catch redirect/client mistakes before hitting Google."""
    issues: list[str] = []
    tips: list[str] = []

    if not CLIENT_ID or not CLIENT_SECRET:
        issues.append("Missing CLIENT_ID or CLIENT_SECRET in st.secrets['google_oauth'].")
    if not REDIRECT_URI:
        issues.append("Missing REDIRECT_URI (set redirect_uri_local / redirect_uri_prod in secrets).")
    else:
        if not REDIRECT_URI.startswith(("http://", "https://")):
            issues.append("REDIRECT_URI must start with http:// or https://")
        if "streamlit.app" in REDIRECT_URI and not REDIRECT_URI.endswith("/"):
            tips.append("For Streamlit Cloud, use a trailing slash, e.g., https://your-app.streamlit.app/ ")
        if "localhost" in REDIRECT_URI:
            if not REDIRECT_URI.startswith("http://"):
                tips.append("For localhost, use http:// (not https://) and include the exact port, e.g., http://localhost:8501/")
        if REDIRECT_URI.endswith("/") is False:
            tips.append("Ensure the exact trailing slash matches what you registered in GCP.")

    tips.append("In Google Cloud Console › Credentials, the OAuth client type must be **Web application**. Desktop/Other will 400.")
    tips.append("Authorized redirect URIs must include this exact REDIRECT_URI (case, scheme, host, path, and trailing slash).")

    with st.expander("🔧 OAuth Setup Checker (click to verify before Sign in)", expanded=False):
        st.code(
            {
                "ENV": ENV,
                "CLIENT_ID_suffix": CLIENT_ID[-12:] if CLIENT_ID else None,
                "REDIRECT_URI": REDIRECT_URI,
                "SCOPES": SCOPES,
            },
            language="json",
        )
        if issues:
            st.error("\n".join(f"• {i}" for i in issues))
        if tips:
            st.info("\n".join(f"• {t}" for t in tips))

    return not issues  # True if basic setup looks OK

# OAuth: refresh if possible
creds = get_creds()
if creds and not creds.valid and creds.refresh_token:
    try:
        creds.refresh(Request()); store_creds(creds)
    except Exception:
        # Why: refresh_token can be revoked or expired; force re-auth cleanly.
        creds = None

# OAuth callback
params = st.query_params
if "code" in params and "state" in params and "creds_json" not in st.session_state:
    returned_state = params.get("state")
    saved_state = st.session_state.get("oauth_state")

    # Guard: if the state doesn't match, stop to avoid CSRF & invalid_grant
    if saved_state and returned_state != saved_state:
        st.error("OAuth state mismatch. Please retry sign-in. If this repeats, clear cookies and ensure only one app tab is open.")
        st.stop()

    flow = build_flow(state=saved_state)
    try:
        # Use the code we just received
        flow.fetch_token(code=params["code"])
    except (MismatchingStateError, InvalidRequestError):
        st.error("OAuth state/parameters invalid during token exchange. Retry sign-in.")
        st.stop()
    except (InvalidClientError, InvalidGrantError) as e:  # pragma: no cover
        # Purposefully concise but actionable diagnostics
        st.error(
            "Google rejected the OAuth exchange (invalid_grant). Common causes: redirect URI mismatch, reused/expired code, or clock skew.")
        with st.expander("Debug details"):
            st.write({
                "ENV": ENV,
                "REDIRECT_URI": REDIRECT_URI,
                "query_state": returned_state,
                "saved_state": saved_state,
                "hint": "Ensure this REDIRECT_URI is registered in Google Cloud Console > OAuth consent screen > Credentials > Authorized redirect URIs.",
            })
        st.stop()

    creds = flow.credentials
    store_creds(creds)
    st.query_params.clear()
    st.success("Signed in to Google successfully.")
    st.rerun()

# Auth gate
if not creds or not creds.valid:
    # Show setup checker to pre-empt invalid_request before opening Google
    oauth_setup_checker()

    flow = build_flow()
    auth_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes=True,  # bool here, library formats as needed
        prompt="consent",
    )
    # Persist state so the callback can verify it and rebuild Flow
    st.session_state["oauth_state"] = state

    st.link_button(
        "🔐 Sign in with Google",
        auth_url,
        help="Authorize Gmail & Sheets. We only act on your behalf.",
        use_container_width=True,
    )
    st.stop()



# ---------------------------------------------------------------------------
# 2) INPUT CONTROLS
# ---------------------------------------------------------------------------

sheet_url = st.text_input(
    "Google Sheet URL",
    placeholder="https://docs.google.com/spreadsheets/d/xxxxxxxxxxxxxxxxxxxxxxxxxxxx/edit#gid=0",
    help="Make sure it's a real Google Sheet (not an uploaded Excel). If it's an .xlsx in Drive, open it and File → Save as Google Sheets."
)
gmail_query = st.text_input(
    "Gmail search query",
    value='PAYLEMAIYAN subject:"NCBA TRANSACTIONS STATUS UPDATE" newer_than:365d',
    help="Use Gmail operators (is:unread, after:, before:). Narrow the window to reduce quota usage."
)

c1, c2, c3, c4, c5, c6 = st.columns([1,1,1,1,1,1])
with c1:
    mark_read = st.checkbox("Mark processed as Read", value=True, help="Remove UNREAD label after logging a payment.")
with c2:
    throttle_ms = st.number_input("Throttle (ms) between writes", min_value=0, value=200, step=50, help="Delay to avoid 429s.")
with c3:
    max_results = st.number_input("Max messages to scan", min_value=10, max_value=1000, value=200, step=10)
with c4:
    weekly_auto = st.checkbox("Enable weekly automation", value=False, help="Runs Mondays 09:00 EAT while the app is open.")
with c5:
    verbose_debug = st.checkbox("Verbose debug", value=True, help="Collect step-by-step notes from bot_logic for troubleshooting.")
with c6:
    create_if_missing = st.checkbox("Auto-create tenant tabs", value=True, help="If a tab isn't found for an AccountCode, create it.")

run_now = st.button("▶️ Run Bot Now", type="primary", use_container_width=True)
st.caption("Automation only runs while the app is open. We never run unattended without your checkbox enabled.")

# ---------------------------------------------------------------------------
# 3) HELPERS FOR GMAIL + SHEETS
# ---------------------------------------------------------------------------

def extract_sheet_id(url: str) -> str:
    """Extract the spreadsheet ID from a Drive URL."""
    try:
        return url.split("/d/")[1].split("/")[0]
    except Exception:
        return ""

def _decode_base64url(data: str) -> str:
    """Decode base64url Gmail parts safely (padding fixed)."""
    padding = '=' * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding).decode("utf-8", errors="ignore")

def _strip_html(html: str) -> str:
    """Basic HTML to text for simple emails that don't include text/plain parts."""
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    return text

def get_message_text(service, msg_id):
    """
    Fetch a Gmail message by id and return best-effort text.
    - Prefers text/plain parts
    - Falls back to stripped HTML
    - Finally, uses snippet if nothing else
    """
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
    """Backoff wrapper for Sheets operations (append_rows, update, etc.)."""
    delay = 1.0
    for _ in range(6):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            if hasattr(e, "response") and getattr(e.response, "status_code", None) == 429:
                time.sleep(delay); delay *= 2; continue
            raise
        except Exception as e:
            if "Rate Limit Exceeded" in str(e) or "quota" in str(e).lower():
                time.sleep(delay); delay *= 2; continue
            raise

def should_auto_run():
    """
    Weekly automation gate:
    - Fires Mondays 09:00 EAT (UTC+3) when the app is open.
    - Only once per week (tracked in session).
    """
    if not weekly_auto: return False
    now_utc = datetime.utcnow()
    eat = now_utc + timedelta(hours=3)
    in_window = (eat.weekday() == 0 and eat.hour == 9)
    last = st.session_state.get("last_auto_run_at")
    if in_window and (last is None or (now_utc - last) > timedelta(days=6)):
        return True
    return False

# ---------------------------------------------------------------------------
# 4) MAIN RUN
# ---------------------------------------------------------------------------

auto_trigger = should_auto_run()
if auto_trigger:
    st.info("🤖 Weekly automation window detected — running now.")
run_now = run_now or auto_trigger

if run_now:
    # Reset bot_logic cache at the start of each run to avoid stale header/col counts
    clear_cache()

    # Validate inputs
    if not sheet_url:
        st.error("Please paste your Google Sheet URL."); st.stop()
    sheet_id = extract_sheet_id(sheet_url)
    if not sheet_id:
        st.error("That doesn't look like a valid Google Sheet URL."); st.stop()

    # Build clients
    gmail = build("gmail", "v1", credentials=creds)
    gs = gspread.authorize(creds)

    # Open spreadsheet
    try:
        sh = gs.open_by_key(sheet_id)
    except Exception as e:
        st.error(f"Could not open the Google Sheet. Ensure you own it or have edit access.\n\n{e}")
        st.stop()

    # Ensure meta sheets exist
    def ensure_meta(ws_name, header):
        try:
            ws = sh.worksheet(ws_name)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=ws_name, rows=2000, cols=max(10, len(header)))
            with_backoff(ws.update, "1:1", [header], value_input_option="USER_ENTERED")
        return ws

    refs_ws = ensure_meta("ProcessedRefs", ["Ref"])
    hist_ws = ensure_meta("PaymentHistory", PAYMENT_COLS + ['AccountCode','TenantSheet','Month'])

    # Load processed refs to avoid duplicates
    ref_vals = with_backoff(refs_ws.get_all_values)
    processed_refs = set((r[0] or '').upper() for r in ref_vals[1:]) if len(ref_vals) > 1 else set()

    # Gmail search
    st.write("🔎 Searching Gmail…")
    result = gmail.users().messages().list(userId="me", q=gmail_query, maxResults=int(max_results)).execute()
    messages = result.get("messages", [])
    st.write(f"Found {len(messages)} candidate emails.")

    # Parse & filter new payments
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
                # skip duplicates
                continue
            parsed.append((m["id"], pay))
        except Exception as e:
            errors.append(f"Error reading message {m['id']}: {e}")

    st.success(f"Parsed {len(parsed)} new payments.")

    # Find/create tenant worksheet by AccountCode (prefix match)
    worksheets = {ws.title: ws for ws in sh.worksheets()}
    def find_or_create_tenant_sheet(account_code: str):
        acct = (account_code or '').strip().upper()
        for title, ws in worksheets.items():
            t = title.strip().upper()
            if t.startswith(acct) and title not in ("ProcessedRefs","PaymentHistory"):
                return ws
        if not create_if_missing:
            raise RuntimeError(f"No tenant sheet found for AccountCode {account_code}")
        # Create with header on row 7 (matches your schema)
        title = f"{account_code} - AutoAdded"
        ws = sh.add_worksheet(title=title, rows=2000, cols=20)
        hdr = ['Month','Amount Due','Amount paid','Date paid','REF Number','Date due','Prepayment/Arrears','Penalties','Comments']
        with_backoff(ws.update, "7:7", [hdr], value_input_option="USER_ENTERED")
        try:
            ws.freeze(rows=7)
        except Exception:
            pass
        worksheets[title] = ws
        st.info(f"➕ Created tenant sheet: {title}")
        return ws

    # Process payments
    hist_rows, ref_rows, logs = [], [], []
    debug_accum = [] if verbose_debug else None

    for idx, (msg_id, p) in enumerate(parsed, start=1):
        ws = find_or_create_tenant_sheet(p["AccountCode"])
        info = update_tenant_month_row(ws, p, debug_accum)

        logs.append(
            f"🧾 {info.get('sheet')} R{info.get('row')} | {info.get('month')} | "
            f"Paid {info.get('paid_before')}→{info.get('paid_after')} | Ref {p.get('REF Number')}"
        )

        # Append to history (for metrics)
        dt = datetime.strptime(p["Date Paid"], "%d/%m/%Y %I:%M %p")
        mon = dt.strftime("%Y-%m")
        hist_rows.append([p[k] for k in PAYMENT_COLS] + [p['AccountCode'], ws.title, mon])

        # Mark ref as processed
        ref_val = (p.get("REF Number","") or "").upper()
        ref_rows.append([ref_val])
        processed_refs.add(ref_val)

        # Optionally mark Gmail as read
        if mark_read:
            try:
                gmail.users().messages().modify(userId="me", id=msg_id, body={"removeLabelIds": ["UNREAD"]}).execute()
            except Exception:
                pass

        # Throttle to be gentle with API quotas
        if throttle_ms > 0:
            time.sleep(throttle_ms / 1000.0)

        # Flush batches periodically
        if len(hist_rows) >= 100 or idx == len(parsed):
            with_backoff(hist_ws.append_rows, hist_rows, value_input_option="USER_ENTERED")
            hist_rows.clear()
            with_backoff(refs_ws.append_rows, ref_rows, value_input_option="RAW")
            ref_rows.clear()

    # Run results
    st.success("Ingestion complete.")
    st.subheader("Run Log")
    if logs:
        st.code("\n".join(logs), language="text")
    if errors:
        st.subheader("Non-fatal Parse/Read Errors")
        st.code("\n".join(errors), language="text")
    if debug_accum:
        st.subheader("Verbose Debug (bot_logic)")
        st.code("\n".join(debug_accum), language="text")

    # -----------------------------------------------------------------------
    # METRICS
    # -----------------------------------------------------------------------
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

    # Per-tenant balances & penalties (scan each tenant tab)
    total_prepay = 0.0
    total_arrears = 0.0
    penalty_freq = {}
    def parse_float(x):
        try:
            return float(str(x).replace(",", "").strip())
        except Exception:
            return 0.0

    def detect_header_row(all_vals, scan_rows: int = 30) -> int:
        # Lightweight detector for metrics-only purposes
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

            # Latest balance = last populated Month row (or last row if Month empty)
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
        st.session_state["last_auto_run_at"] = datetime.utcnow()

# ---------------------------------------------------------------------------
# 5) FOOTER
# ---------------------------------------------------------------------------

st.divider()
st.caption(
    "Rent-RPA © {year}. Tips: keep Gmail queries narrow; use Google Sheets (not Excel uploads); "
    "Made by [Eugene Maina](https://github.com/eugene-maina72)."
    .format(year=datetime.now().year)
)
