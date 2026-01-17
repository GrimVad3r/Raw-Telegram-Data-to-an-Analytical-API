
---

# Telegram Channel Scraper (Enterprise-Bypass Edition)

A robust Python-based scraper using the **Telethon** library to extract messages and media from Telegram channels. This version is specifically configured to operate within **restricted corporate networks** by utilizing MTProto Proxy tunneling.

## 🚀 Features

* **Message Extraction:** Scrapes text, views, forwards, and metadata from public channels.
* **Media Downloader:** Automatically detects and downloads images, organizing them by channel.
* **Corporate Firewall Bypass:** Uses `TcpMTProxyRandomizedIntermediate` to tunnel traffic through port 8443, bypassing Deep Packet Inspection (DPI).
* **Data Lake Ready:** Saves output in structured JSON format, partitioned by date and channel name.
* **Automatic Sanitization:** Handles URL formatting to create clean directory structures.

## 🏗️ Project Structure

```text
.
├── data/
│   ├── raw/
│   │   ├── images/              # Downloaded channel media (.jpg)
│   │   └── telegram_messages/   # Daily JSON exports
├── logs/                        # Execution logs
├── .env                         # API Credentials (ignored by git)
└── scraper.py                   # Main script

```

## 🛠️ Installation

1. **Clone the repository:**
```bash
git clone <https://github.com/GrimVad3r/Raw-Telegram-Data-to-an-Analytical-API>
cd Raw-Telegram-Data-to-an-Analytical-API

```


2. **Install Dependencies:**
```bash
pip install telethon python-dotenv PySocks

```


3. **Configure Environment Variables:**
Create a `.env` file in the root directory:
```env
API_ID=your_api_id
API_HASH=your_api_hash
PHONE=+your_phone_number
proxy_addr = proxy_address ( Data from Mtpro.xyz)
proxy_port = proxy_port ( Data from Mtpro.xyz)
proxy_secret = proxy_secret ( Data from Mtpro.xyz)

```



## 🌐 Networking & Proxies

In a standard environment, Telegram's MTProto traffic is often flagged by corporate firewalls. This script utilizes a **Randomized Intermediate MTProto Proxy** to mask traffic:

If the current proxy (`193.17.95.241`) becomes slow, you can find fresh proxies from [MTPro.xyz](https://mtpro.xyz) and update the `proxy_addr`, `proxy_port`, and `proxy_secret` in the env file.

## 📝 Usage

Simply run the script via the terminal. On the first run, you will be prompted to enter the login code sent to your Telegram app.

```bash
python src/scraper.py

```

## 📊 Data Output Format

Each channel generates a JSON file structured as follows:

```json
[
  {
    "message_id": 1234,
    "channel_name": "CheMed123",
    "message_date": "2026-01-17T11:04:36",
    "message_text": "Sample message content here...",
    "has_media": true,
    "image_path": "data/raw/images/CheMed123/1234.jpg"
  }
]

```

## ⚠️ Important Notes

* **Rate Limiting:** The script includes an `asyncio.sleep(2)` between channel requests to avoid `FloodWait` errors.
* **Session Management:** The script creates a `corporate_proxy_session.session` file. Do not share this file, as it contains your authentication tokens.
* **Compliance:** Ensure you have permission to scrape the targeted channels and comply with Telegram's Terms of Service.

---
