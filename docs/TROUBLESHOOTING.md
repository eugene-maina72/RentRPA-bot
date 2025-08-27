# Troubleshooting
- **400 This operation is not supported**: Your file is an Excel file in Drive. Open it and use *File → Save as Google Sheets*.
- **429 Quota exceeded**: Narrow Gmail query, increase throttle, avoid simultaneous runs.
- **Range exceeds grid limits**: The app now expands grid before writes, but very narrow tabs may still need manual columns added.
- **Wrong header row**: The bot auto-detects; if headers live on row 7, it will use that. You can hardcode row 7 if all tabs are consistent.
- **Date value errors**: Locale/format mismatch — formulas coerce with DATEVALUE. Ensure dd/mm/YYYY when typing manually.
