# Setup
1) Create venv and install requirements (see README).
2) In Google Cloud Console, enable **Gmail API** and **Google Sheets API**.
3) Create OAuth **Web application**; add redirect URIs for local + prod.
4) Put secrets into `.streamlit/secrets.toml` using the example file.
5) Make sure your target is a **Google Sheet** (not an uploaded .xlsx).
6) Run: `streamlit run streamlit_app.py`.
