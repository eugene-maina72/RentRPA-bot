# /streamlit_app.py
# -*- coding: utf-8 -*-
"""
Streamlit UI to ingest NCBA emails → Google Sheets.

WHY:
- Safe OAuth, throttled Gmail/Sheets.
- Robust parser, schema alignment, idempotent by REF.
- Business rules:
    * Rent due = 5th.
    * Penalty = 3000 if (DatePaid >= DateDue + 2) AND (net_after <= 0).
- Defensive formulas (no #VALUE!).
- MonthKey for stable sorting (quota-friendly per-row set).
- Maintenance tools:
    * Backfill MonthKey (throttled).
    * NEW: Migration/Repair — rewrite formulas for all rows and optionally backfill MonthKey.
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

try:
    from oauthlib.oauth2.rfc6749.errors import (
        InvalidGrantError, MismatchingStateError, InvalidClientError, InvalidRequestError,
    )
except Exception:  # pragma: no cover
    InvalidGrantError = MismatchingStateError = InvalidClientError = InvalidRequestError = Exception

from bot_logic import (
    parse_email,
    update_tenant_month_row,
    PAYMENT_COLS,
    clear_cache,
    _parse_month_cell,  # internal helper reuse is fine
)

# --- Page & config -----------------------------------------------------------

st.set_page_config(page_title="Rent RPA (Gmail → Sheets)", page_icon="🏠", layout="wide")
st.title("🏠 Rent RPA — Gmail → Google Sheets")
st.markdown(
    """
<div style="padding:10px;border:1px solid #ddd;border-radius:8px;background:#f7f5f4;margin-bottom:8px">
<b>Rules:</b> Date due = <b>5th</b>. <b>Penalty</b> = 3000 KES if paid on/after <i>due + 2</i> and balance ≤ 0.<br>
<b>Safety:</b> We append missing headers; never overwrite <b>Comments</b>. Defensive formulas prevent #VALUE!.
</div>
""",
    unsafe_allow_html=True,
)
st.caption("Tokens live only in your session memory. No server-side persistence.")

# --- OAuth -------------------------------------------------------------------

SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.readonly",
]

ENV            = st.secrets.get("ENV", "local")
GOOGLE_OAUTH   = st.secrets.get("google_oauth", {})
CLIENT_ID      = GOOGLE_OAUTH.get("client_id")
CLIENT_SECRET  = GOOGLE_OAUTH.get("client_secret")
REDIRECT_LOCAL = GOOGLE_OAUTH.get("redirect_uri_local")
REDIRECT_PROD  = GOOGLE_OAUTH.get("redirect_uri_prod")
REDIRECT_URI   = REDIRECT_PROD if ENV == "prod" else REDIRECT_LOCAL

def build_flow(state: str | None = None) -> Flow:
    cfg = {
        "web": {
            "client_id": CLIENT_ID or "",
            "project_id": "rent-rpa",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_secret": CLIENT_SECRET or "",
            "redirect_uris": [REDIRECT_URI or ""],
        }
    }
    return Flow.from_client_config(cfg, scopes=SCOPES, redirect_uri=REDIRECT_URI or "", state=state)

def get_creds():
    if "creds_json" in st.session_state:
        return Credentials.from_authorized_user_info(json.loads(st.session_state["creds_json"]), SCOPES)
    return None

def store_creds(creds: Credentials):
    st.session_state["creds_json"] = creds.to_json()

def oauth_setup_checker():
    issues, tips = [], []
    if not CLIENT_ID or not CLIENT_SECRET:
        issues.append("Missing CLIENT_ID or CLIENT_SECRET in st.secrets['google_oauth'].")
    if not REDIRECT_URI:
        issues.append("Missing REDIRECT_URI (set redirect_uri_local / redirect_uri_prod).")
    else:
        if not REDIRECT_URI.startswith(("http://", "https://")):
            issues.append("REDIRECT_URI must start with http:// or https://")
        if "localhost" in REDIRECT_URI and not REDIRECT_URI.startswith("http://"):
            tips.append("For localhost, use http:// and include the exact port (e.g., http://localhost:8501/)")
        if not REDIRECT_URI.endswith("/"):
            tips.append("Ensure trailing slash matches GCP OAuth.")
    with st.expander("🔧 OAuth Setup Checker", expanded=False):
        st.code({"ENV": ENV, "CLIENT_ID_suffix": (CLIENT_ID or "")[-12:], "REDIRECT_URI": REDIRECT_URI, "SCOPES": SCOPES}, language="json")
        if issues: st.error("\n".join(f"• {i}" for i in issues))
        if tips:   st.info("\n".join(f"• {t}" for t in tips))
    return not issues

# Try refresh
creds = get_creds()
if creds and not creds.valid and creds.refresh_token:
    try:
        creds.refresh(Request()); store_creds(creds)
    except Exception:
        creds = None

# OAuth callback
params = st.query_params
if "code" in params and "state" in params and "creds_json" not in st.session_state:
    if st.session_state.get("oauth_state") and params.get("state") != st.session_state["oauth_state"]:
        st.error("OAuth state mismatch. Please retry."); st.stop()
    flow = build_flow(state=st.session_state.get("oauth_state"))
    try:
        flow.fetch_token(code=params["code"])
    except (MismatchingStateError, InvalidRequestError):
        st.error("OAuth parameters invalid. Retry sign-in."); st.stop()
    except (InvalidClientError, InvalidGrantError):  # pragma: no cover
        st.error("Google rejected the OAuth exchange. Check redirect URI and retry."); st.stop()
    creds = flow.credentials
    store_creds(creds)
    st.query_params.clear()
    st.success("Signed in.")
    st.rerun()

# Auth gate
if not creds or not creds.valid:
    oauth_setup_checker()
    flow = build_flow()
    auth_url, state = flow.authorization_url(access_type="offline", include_granted_scopes=True, prompt="consent")
    st.session_state["oauth_state"] = state
    st.link_button("🔐 Sign in with Google", auth_url, use_container_width=True)
    st.stop()

# --- Inputs ------------------------------------------------------------------

sheet_url = st.text_input("Google Sheet URL", placeholder="https://docs.google.com/spreadsheets/d/xxxxxxxxxxxxxxxxxxxx/edit#gid=0")
gmail_query = st.text_input(
    "Gmail search query",
    value='PAYLEMAIYAN subject:"NCBA TRANSACTIONS STATUS UPDATE" newer_than:365d',
)

c1, c2, c3, c4, c5, c6 = st.columns([1,1,1,1,1,1])
with c1: mark_read = st.checkbox("Mark processed as Read", value=True)
with c2: throttle_ms = st.number_input("Throttle (ms) between writes", min_value=0, value=200, step=50)
with c3: max_results = st.number_input("Max messages to scan", min_value=10, max_value=1000, value=200, step=10)
with c4: weekly_auto = st.checkbox("Enable weekly automation", value=False)
with c5: verbose_debug = st.checkbox("Verbose debug", value=True)
with c6: create_if_missing = st.checkbox("Auto-create tenant tabs", value=True)
run_now = st.button("▶️ Run Bot Now", type="primary", use_container_width=True)

# --- Helpers -----------------------------------------------------------------

def extract_sheet_id(url: str) -> str:
    try: return url.split("/d/")[1].split("/")[0]
    except Exception: return ""

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
        for p in parts or []: walk(p)
    walk(payload or {})
    return "\n".join(body_texts) if body_texts else msg.get("snippet", "")

def with_backoff(fn, *args, **kwargs):
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

def _detect_header_row(all_vals, scan_rows: int = 30) -> int:
    def score(row):
        s = 0
        for cell in row:
            t = str(cell or "").strip().lower()
            if t in {"month","amount due","amount paid","date paid","ref number","date due","prepayment/arrears","penalties","comments","monthkey"}:
                s += 1
        return s
    best_i, best_score = 0, 0
    limit = min(len(all_vals), max(1, scan_rows))
    for i in range(limit):
        sc = score(all_vals[i])
        if sc > best_score: best_i, best_score = i, sc
    return best_i if best_score >= 3 else 0

# --- Maintenance: MonthKey Backfill (existing) & NEW Migration/Repair --------

st.divider()
with st.expander("🧰 Maintenance", expanded=False):
    st.markdown("**Run these one-time (or occasionally) to repair legacy rows and keep sorting stable.**")

    # MonthKey backfill (existing tool)
    colA, colB = st.columns([1,1])
    with colA:
        backfill_chunk = st.number_input("MonthKey chunk size", min_value=10, max_value=500, value=50, step=10)
    with colB:
        backfill_delay = st.number_input("Delay (seconds) between MonthKey batches", min_value=0.0, max_value=5.0, value=0.75, step=0.25)

    do_backfill = st.button("🛠 Backfill MonthKey (maintenance)", use_container_width=True)

    # NEW: Migration / Repair formulas
    st.markdown("---")
    colC, colD, colE = st.columns([1,1,2])
    with colC:
        mig_chunk = st.number_input("Repair chunk size", min_value=10, max_value=500, value=75, step=5,
                                    help="Cells per batch update (Penalties & Balance).")
    with colD:
        mig_delay = st.number_input("Delay (seconds) between repair batches", min_value=0.0, max_value=5.0, value=0.5, step=0.25)
    with colE:
        mig_backfill_mkey = st.checkbox("Also backfill MonthKey during repair", value=True)

    do_migrate = st.button("🔧 Repair Formulas (migration)", use_container_width=True)

    # Common sheet open
    if (do_backfill or do_migrate):
        if not sheet_url:
            st.error("Please paste your Google Sheet URL above."); st.stop()
        sheet_id = extract_sheet_id(sheet_url)
        if not sheet_id:
            st.error("That doesn't look like a valid Google Sheet URL."); st.stop()

        gs = gspread.authorize(creds)
        try:
            sh = gs.open_by_key(sheet_id)
        except Exception as e:
            st.error(f"Could not open the Google Sheet. Ensure edit access.\n\n{e}")
            st.stop()

    # --- Backfill MonthKey action
    if do_backfill:
        ws_list = [ws for ws in sh.worksheets() if ws.title not in ("PaymentHistory","ProcessedRefs")]
        pbar = st.progress(0, text="Scanning sheets...")
        total_candidates = total_updated = total_skipped = touched_sheets = 0

        for idx, ws in enumerate(ws_list, start=1):
            try:
                vals = with_backoff(ws.get_all_values)
                if not vals:
                    pbar.progress(idx / len(ws_list), text=f"Skipping empty: {ws.title}")
                    continue
                header_row0 = _detect_header_row(vals)
                header = [c.strip() for c in vals[header_row0]]

                # Ensure MonthKey
                norm = [h.strip().lower() for h in header]
                if "monthkey" not in norm:
                    header.append("MonthKey")
                    with_backoff(ws.update, f"{header_row0+1}:{header_row0+1}", [header], value_input_option="USER_ENTERED")
                    norm = [h.strip().lower() for h in header]
                i_month = [h.lower() for h in header].index("month")
                i_mkey  = [h.lower() for h in header].index("monthkey")

                rows = vals[header_row0+1:]
                updates = []
                for r_pos, row in enumerate(rows, start=header_row0+2):
                    month_txt = row[i_month] if len(row) > i_month else ""
                    mkey_txt  = row[i_mkey]  if len(row) > i_mkey  else ""
                    if mkey_txt: continue
                    ym = _parse_month_cell(month_txt)
                    if not ym:
                        total_skipped += 1; continue
                    y, m = ym
                    a1 = gspread.utils.rowcol_to_a1(r_pos, i_mkey+1)
                    updates.append({"range": a1, "values": [[f"{y:04d}-{m:02d}"]]})

                total_candidates += len(updates)
                if updates:
                    touched_sheets += 1
                    for j in range(0, len(updates), int(backfill_chunk)):
                        batch = updates[j:j+int(backfill_chunk)]
                        with_backoff(ws.batch_update, batch, value_input_option="USER_ENTERED")
                        total_updated += len(batch)
                        time.sleep(float(backfill_delay))
                pbar.progress(idx / len(ws_list), text=f"Processed {ws.title}")

            except Exception as e:
                pbar.progress(idx / len(ws_list), text=f"Error on {ws.title}: {e}")
                continue

        st.success(f"MonthKey Backfill: Sheets touched={touched_sheets} | Written={total_updated} | Skipped invalid={total_skipped} | Candidates={total_candidates}")

    # --- Migration/Repair action
    if do_migrate:
        ws_list = [ws for ws in sh.worksheets() if ws.title not in ("PaymentHistory","ProcessedRefs")]
        pbar = st.progress(0, text="Rewriting formulas…")
        total_pen = total_bal = total_mk = 0
        touched_sheets = 0
        errors = []

        for idx, ws in enumerate(ws_list, start=1):
            try:
                vals = with_backoff(ws.get_all_values)
                if not vals:
                    pbar.progress(idx/len(ws_list), text=f"Skipping empty: {ws.title}")
                    continue

                header_row0 = _detect_header_row(vals)
                header = [c.strip() for c in vals[header_row0]]
                hl = [h.strip().lower() for h in header]

                # Minimal required columns
                def col(name): return hl.index(name)
                try:
                    i_mon = col("month"); i_due = col("amount due"); i_paid = col("amount paid")
                    i_dp  = col("date paid"); i_dd = col("date due")
                    i_pen = col("penalties"); i_bal = col("prepayment/arrears")
                except ValueError:
                    pbar.progress(idx/len(ws_list), text=f"Missing core headers on {ws.title}; skipping")
                    continue

                # Ensure MonthKey column if requested
                i_mkey = None
                if mig_backfill_mkey:
                    if "monthkey" not in hl:
                        header.append("MonthKey")
                        with_backoff(ws.update, f"{header_row0+1}:{header_row0+1}", [header], value_input_option="USER_ENTERED")
                        hl = [h.strip().lower() for h in header]
                    i_mkey = hl.index("monthkey")

                rows = vals[header_row0+1:]
                # Build updates
                updates = []
                mk_updates = []
                for n, row in enumerate(rows, start=1):  # n=1 is first data row
                    r_abs = header_row0 + 1 + n
                    # A1 addresses
                    ap = gspread.utils.rowcol_to_a1(r_abs, i_paid+1)
                    ad = gspread.utils.rowcol_to_a1(r_abs, i_due+1)
                    dp = gspread.utils.rowcol_to_a1(r_abs, i_dp+1)
                    dd = gspread.utils.rowcol_to_a1(r_abs, i_dd+1)
                    pe = gspread.utils.rowcol_to_a1(r_abs, i_pen+1)
                    ba = gspread.utils.rowcol_to_a1(r_abs, i_bal+1)
                    prev_ba = gspread.utils.rowcol_to_a1(r_abs-1, i_bal+1)

                    # Defensive coercions
                    paid_num = f"IFERROR(VALUE({ap}), N({ap}))"
                    due_num  = f"IFERROR(VALUE({ad}), N({ad}))"
                    prev_bal = f"IFERROR({prev_ba}, 0)"
                    pen_num  = f"IFERROR({pe}, 0)"
                    dpaid    = f"IFERROR(DATEVALUE(TO_TEXT({dp})), {dp})"
                    ddue     = f"IFERROR(DATEVALUE(TO_TEXT({dd})), {dd})"
                    hasp     = f"LEN(TO_TEXT({dp}))>0"
                    hasd     = f"LEN(TO_TEXT({dd}))>0"
                    net_after= f"({prev_bal}+{paid_num}-{due_num})"

                    pen_formula = f"=IF(AND({hasp}, {hasd}, {net_after} <= 0, {dpaid} >= {ddue} + 2), 3000, 0)"
                    if n == 1:
                        bal_formula = f"=({paid_num})-({due_num})-({pen_num})"
                    else:
                        bal_formula = f"=({prev_bal})+({paid_num})-({due_num})-({pen_num})"

                    updates.append({"range": pe, "values": [[pen_formula]]})
                    updates.append({"range": ba, "values": [[bal_formula]]})
                    total_pen += 1; total_bal += 1

                    # MonthKey backfill per-row (if enabled)
                    if mig_backfill_mkey:
                        try:
                            mon_txt = row[i_mon] if len(row) > i_mon else ""
                            ym = _parse_month_cell(mon_txt)
                            if ym:
                                if i_mkey is not None:
                                    mk_cell = gspread.utils.rowcol_to_a1(r_abs, i_mkey+1)
                                    mk_updates.append({"range": mk_cell, "values": [[f"{ym[0]:04d}-{ym[1]:02d}"]]})
                                    total_mk += 1
                        except Exception:
                            pass

                # Throttled writes
                def _emit(batches, chunk, delay):
                    for j in range(0, len(batches), int(chunk)):
                        batch = batches[j:j+int(chunk)]
                        with_backoff(ws.batch_update, batch, value_input_option="USER_ENTERED")
                        time.sleep(float(delay))

                if updates:
                    touched_sheets += 1
                    _emit(updates, mig_chunk, mig_delay)
                if mig_backfill_mkey and mk_updates:
                    _emit(mk_updates, max(25, int(mig_chunk//2)), mig_delay)

                pbar.progress(idx/len(ws_list), text=f"Repaired {ws.title}")

            except Exception as e:
                errors.append(f"{ws.title}: {e}")
                pbar.progress(idx/len(ws_list), text=f"Error on {ws.title}: {e}")

        st.success(f"Repair complete. Sheets touched={touched_sheets} | Penalties set={total_pen} | Balances set={total_bal} | MonthKeys set={total_mk}")
        if errors:
            st.warning("Some sheets had issues:")
            st.code("\n".join(errors), language="text")

# --- Main ingestion ----------------------------------------------------------

def should_auto_run():
    if not weekly_auto: return False
    now_utc = datetime.utcnow()
    eat = now_utc + timedelta(hours=3)
    in_window = (eat.weekday() == 0 and eat.hour == 9)
    last = st.session_state.get("last_auto_run_at")
    if in_window and (last is None or (now_utc - last) > timedelta(days=6)):
        return True
    return False

# --- Main run ---------------------------------------------------------------

auto_trigger = should_auto_run()
if auto_trigger:
    st.info("🤖 Weekly automation window detected — running.")
run_now = run_now or auto_trigger

if run_now:
    clear_cache()
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
        st.error(f"Could not open the Google Sheet. Ensure edit access.\n\n{e}")
        st.stop()

    def ensure_meta(ws_name, header):
        try:
            ws = sh.worksheet(ws_name)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=ws_name, rows=2000, cols=max(10, len(header)))
            with_backoff(ws.update, "1:1", [header], value_input_option="USER_ENTERED")
        return ws

    refs_ws = ensure_meta("ProcessedRefs", ["Refs"])
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
            if "PAYLEMAIYAN" not in (text or "").upper():
                continue
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
        # Canonical header (row 7) — WHY: matches logic aliasing and keeps uniform
        hdr = ['Month','Date Due','Amount Due','Amount Paid','Date Paid','REF Number','Comments','Prepayment/Arrears','Penalties']
        with_backoff(ws.update, "7:7", [hdr], value_input_option="USER_ENTERED")
        try: ws.freeze(rows=7)
        except Exception: pass
        worksheets[title] = ws
        st.info(f"➕ Created tenant sheet: {title}")
        return ws

    # Process payments
    hist_rows, ref_rows, logs = [], [], []
    debug_accum = [] if verbose_debug else None

    for idx, (msg_id, p) in enumerate(parsed, start=1):
        ws = find_or_create_tenant_sheet(p["AccountCode"])
        info = update_tenant_month_row(ws, p, debug_accum)

        logs.append(f"🧾 {info.get('sheet')} R{info.get('row')} | {info.get('month')} | Paid {info.get('paid_before')}→{info.get('paid_after')} | Ref {p.get('REF Number')}")

        # Append to history using aligned schema; preserve extras at end
        hist_rows.append([p.get(k, "") for k in PAYMENT_COLS] + [
            p.get('AccountCode',''), ws.title, datetime.strptime(p["Date Paid"], "%d/%m/%Y").strftime("%Y-%m")
        ])

        ref_val = (p.get("REF Number","") or "").upper()
        ref_rows.append([ref_val])
        processed_refs.add(ref_val)

        # Optionally mark Gmail as read
        if mark_read:
            try:
                gmail.users().messages().modify(userId="me", id=msg_id, body={"removeLabelIds": ["UNREAD"]}).execute()
            except Exception:
                pass

        if throttle_ms > 0: time.sleep(throttle_ms / 1000.0)

        # Flush in batches
        if len(hist_rows) >= 100 or idx == len(parsed):
            with_backoff(hist_ws.append_rows, hist_rows, value_input_option="USER_ENTERED"); hist_rows.clear()
            with_backoff(refs_ws.append_rows, ref_rows, value_input_option="RAW"); ref_rows.clear()

    # Run results
    st.success("Ingestion complete.")
    st.subheader("Run Log")
    if logs: st.code("\n".join(logs), language="text")
    if errors:
        st.subheader("Non-fatal Parse/Read Errors"); st.code("\n".join(errors), language="text")
    if debug_accum:
        st.subheader("Verbose Debug (bot_logic)"); st.code("\n".join(debug_accum), language="text")

    # --- Metrics -------------------------------------------------------------
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
        grouped = df_hist.groupby("Month", dropna=False).agg(Payments=("REF Number","count"), TotalAmount=("Amount Paid","sum")).reset_index().sort_values("Month")
        st.markdown("**Payment History — by Month**")
        st.dataframe(grouped, use_container_width=True)

    # Per-tenant balances & penalties (scan each tenant tab)

    total_prepay = 0.0
    total_arrears = 0.0
    penalty_freq = {}

    def parse_float(x):
        try: return float(str(x).replace(",", "").strip())
        except Exception: return 0.0

    for ws in sh.worksheets():
        name = ws.title.upper()
        if name in ("PAYMENTHISTORY", "PROCESSEDREFS"):
            continue
        try:
            vals = with_backoff(ws.get_all_values)
            if not vals: continue
            header_row0 = _detect_header_row(vals)
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
            if latest_row is None and rows:
                latest_row = rows[-1]

            if latest_row and i_bal != -1 and len(latest_row) > i_bal:
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
