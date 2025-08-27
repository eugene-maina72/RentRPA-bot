# Usage
1. Sign in with Google (OAuth).
2. Paste Google Sheet URL.
3. Enter Gmail query, e.g.:
   ```
   PAYLEMAIYAN subject:"NCBA TRANSACTIONS STATUS UPDATE" newer_than:365d
   ```
4. Configure options (mark as read, throttle, max messages, weekly automation).
5. Run bot.

## What the bot writes
- Finds or creates the month row on the tenant tab.
- Carries forward "Amount Due"; sets "Date due" = 5th of that month.
- Updates Amount paid / Date paid / REF Number.
- Applies formulas:
  - **Penalties** = 3000 if paid > due + 2 days (works for text/serial dates).
  - **Prepayment/Arrears** = rolling carry-forward.
- Conditional formatting:
  - Arrears < 0 → light red.
  - Penalties > 0 → light yellow.

## Metrics view
- Income this month (from PaymentHistory).
- Total Prepayments / Total Arrears.
- Penalty frequency by AccountCode.
