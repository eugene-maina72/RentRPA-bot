# 📊 Rental Income Record keeping Automation Bot 🏠

<img src="images/rent%20bot.jpg" alt="Rent bot" width="400"/>

***
Automate your apartment rent payment tracking using this Streamlit-based RPA bot. The bot fetches MPESA Paybill payment notifications from Gmail and updates a Google Sheet in real-time—no manual logging, no spreadsheet headaches!

## 🚀 Features

* 🔑 Secure Google OAuth 2.0 Login – No need to store passwords.

* 📩 Fetches MPESA Payment Emails – Automatically pulls bank transaction notifications.

* 📄 Parses Payment Details – Extracts Amount, Payer Name, Reference Code, etc.

* 📊 Updates Google Sheets in Real-Time – Adds new payment entries for each tenant.

* 🛡 Deduplication Logic – Never logs the same transaction twice.

* 🖥 Streamlit Web Interface – Simple, user-friendly dashboard.

## 🗂 Project Structure

```bash

/RentRPA-bot/
│
├── data
├── images
├── LICENSE
├── RentRPA.ipynb
├── README.md                # This file
├── requirements.txt         # Python dependencies
├── prototype.py             # Prototype program(Local running)
├── streamlit_app.py         # Main Streamlit UI file
└── bot_functions.py         # Backend logic for email parsing & Sheets updating

```

## 🔧 Setup Instructions

1. Clone this Repository

    ```bash

    git clone https://github.com/eugene-maina72/RentRAP-bot
    cd RentRAP-bot

    ```

2. Add Google OAuth Credentials

    * Go to Google Cloud Console.

    * Create an OAuth 2.0 Client ID (Desktop App).

    * Download the client_secret.json file and place it in this project folder.

3. Install Python Dependencies

    * If running locally:

```bash


    pip install -r requirements.txt

```

4. Run the Streamlit App Locally

```bash

streamlit run streamlit_app.py

```

5. ☁ Deploying to Streamlit Cloud (Optional)

    * Push this project to a GitHub repository.

    * Go to Streamlit Cloud and link your GitHub.

    * Add client_secret.json to the repository (secure it properly in production).

    * Deploy and run the app!

## 📈 How It Works

* Authenticate with Google – Click the login link on the app.

* Fetch Recent Payment Emails – The bot pulls in latest NCBA MPESA notifications.

* Parse & Extract Payment Data – It extracts transaction amount, payer info, reference codes.

* Update Google Sheet – Matches tenants by account code (e.g., E5) and adds payment as a new row.

* Avoids Duplicates – Already-logged payments (MPESA Ref) are skipped automatically.

## 🔐 Security Notes

* OAuth flow uses Google’s secure login system.

* No passwords are ever stored or handled by the app.

* Tokens are session-based and expire after logout.

* Sensitive files like client_secret.json should never be publicly exposed in production.

## 🤝 Contribution Guide

* Fork this repository.

* Create a new branch (feature/your-feature).

* Commit your changes.

* Push to the branch.

* Open a Pull Request.

## 📄 License

This project is licensed under the MIT License.

## 👨‍💻 Author

Eugene Maina
Data Scientist | RPA Developer

* [LinkedIn](https://www.linkedin.com/in/eugene-maina-4a8b9a128/) | [GitHub](https://github.com/eugene-maina72) | [Email](mailto:eugenemaina72@gmail.com)
