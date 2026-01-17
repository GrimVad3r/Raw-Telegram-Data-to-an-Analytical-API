
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

---

# Telegram Data Warehouse Loader

This script serves as the **Ingestion Layer** of your data pipeline. It is responsible for moving raw JSON data from your local Data Lake into a structured **PostgreSQL** database. It automates schema creation and handles the bulk loading of scraped Telegram messages.

## 🏛️ Architecture Overview

The script follows the "E" and "L" of an ETL (Extract, Transform, Load) process:

1. **Extract**: Scans the `data/raw/telegram_messages` directory recursively for `.json` files.
2. **Load**: Parses JSON objects and inserts them into a relational PostgreSQL schema.

---

## 🚀 Features

* **Automated Schema Management**: Automatically creates the `raw` schema and the `telegram_messages` table if they do not exist.
* **Recursive File Discovery**: Uses `pathlib` to find all JSON files across nested daily folders.
* **Relational Mapping**: Maps unstructured JSON fields to specific PostgreSQL data types (BIGINT, TIMESTAMP, TEXT, etc.).
* **Traceability**: Adds a `loaded_at` timestamp to every row to track when the data entered the warehouse.

---

## 🛠️ Requirements

* **Python 3.x**
* **PostgreSQL** instance
* **Dependencies**:
```bash
pip install psycopg2-binary python-dotenv

```



---

## ⚙️ Configuration

The loader uses the same `.env` file as your scraper. Ensure the following variables are defined:

```env
DB_HOST=your_database_host (e.g., localhost)
DB_NAME=medical_warehouse
DB_USER=your_username
DB_PASS=your_password

```

---

## 📊 Database Schema (`raw.telegram_messages`)

| Column | Data Type | Description |
| --- | --- | --- |
| `message_id` | BIGINT | Unique Telegram message ID |
| `channel_name` | VARCHAR | Sanitized name of the source channel |
| `message_date` | TIMESTAMP | Original publication time |
| `message_text` | TEXT | Content of the message |
| `has_media` | BOOLEAN | Flag for image/video presence |
| `image_path` | TEXT | Local file path to the downloaded image |
| `views` | INTEGER | View count at time of scrape |
| `forwards` | INTEGER | Forward count at time of scrape |
| `loaded_at` | TIMESTAMP | Metadata: When this row was inserted |

---

## 📝 Usage

Run the script from your project root:

```bash
python src/database_loader.py

```

### What happens during execution:

1. The script connects to your PostgreSQL instance.
2. It creates a `raw` schema to isolate "dirty" data from your analytical tables.
3. It wipes the existing `raw.telegram_messages` table (Full Refresh mode).
4. It iterates through every JSON file found in your data lake and commits the records to the database.

---

## ⚠️ Important Considerations

* **Full Refresh**: The current version uses `DROP TABLE IF EXISTS`. This is ideal for initial development but should be changed to an `UPSERT` (Insert on Conflict) pattern if you want to avoid duplicating or losing historical data in production.
* **Error Handling**: If one JSON file is corrupted, the current script will log the error but stop. Adding a `try/except` block inside the file loop is recommended for large-scale production runs.

---
