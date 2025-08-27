# Deploy
## Streamlit Community Cloud
- Push repo to GitHub.
- Set app to run `streamlit_app.py`.
- Add secrets per `.streamlit/secrets.example.toml`.

## Docker (example)

```markdown

FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
EXPOSE 8501
CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```
