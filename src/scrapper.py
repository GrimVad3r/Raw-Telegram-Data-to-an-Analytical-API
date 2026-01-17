import os
import json
import logging
import asyncio
import socks
import socket
import urllib.request
from datetime import datetime
from pathlib import Path
from telethon import TelegramClient, connection
from telethon.tl.types import MessageMediaPhoto
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs/scraper.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

class TelegramScraper:
    def __init__(self):
        api_id_str = os.getenv("API_ID")
        self.api_id = int(api_id_str) if api_id_str else None
        self.api_hash = os.getenv("API_HASH")
        self.phone = os.getenv("PHONE")

        if not self.api_id or not self.api_hash:
            raise ValueError("API_ID or API_HASH not found in .env file.")

        # Verified MTProto Proxy Settings
        proxy_addr = os.getenv("proxy_addr")
        proxy_port_str = os.getenv("proxy_port")
        proxy_port = int(proxy_port_str) if proxy_port_str else None
        proxy_secret = os.getenv("proxy_secret")

        self.client = TelegramClient(
            "proxy_scrapping_session",
            self.api_id,
            self.api_hash,
            connection=connection.ConnectionTcpMTProxyRandomizedIntermediate,
            proxy=(proxy_addr, proxy_port, proxy_secret)
        )

    def _sanitize_channel_name(self, channel_username):
        return channel_username.strip("/").split("/")[-1].replace("@", "")

    async def download_image(self, message, channel_name):
        try:
            image_dir = Path(f"data/raw/images/{channel_name}")
            image_dir.mkdir(parents=True, exist_ok=True)
            image_path = image_dir / f"{message.id}.jpg"
            path = await self.client.download_media(message, file=str(image_path))
            return str(path) if path else None
        except Exception as e:
            logger.error(f"Download error: {str(e)}")
            return None

    async def scrape_channel(self, channel_username, limit=100):
        logger.info(f"Scraping channel: {channel_username}")
        messages_data = []
        channel_name = self._sanitize_channel_name(channel_username)

        try:
            async for message in self.client.iter_messages(channel_username, limit=limit):
                message_dict = {
                    'message_id': message.id,
                    'channel_name': channel_name,
                    'message_date': message.date.isoformat(),
                    'message_text': message.message if message.message else '',
                    'has_media': message.media is not None,
                    'image_path': None,
                    'views': message.views if message.views else 0,
                    'forwards': message.forwards if message.forwards else 0
                }
                
                if message.media and isinstance(message.media, MessageMediaPhoto):
                    image_path = await self.download_image(message, channel_name)
                    message_dict["image_path"] = image_path

                messages_data.append(message_dict)
            return messages_data
        except Exception as e:
            logger.error(f"Error scraping {channel_username}: {str(e)}")
            return []

    def save_to_data_lake(self, messages_data, channel_name):
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            data_dir = Path(f"data/raw/telegram_messages/{today}")
            data_dir.mkdir(parents=True, exist_ok=True)
            output_file = data_dir / f"{channel_name}.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(messages_data, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(messages_data)} messages for {channel_name}.")
        except Exception as e:
            logger.error(f"Save error: {str(e)}")

    async def run(self, channels):
        try:
            logger.info("Starting Telegram Client...")
            await self.client.start(phone=self.phone)
            logger.info("Connection established!")

            for channel in channels:
                await asyncio.sleep(2) # Prevent rate limiting
                messages = await self.scrape_channel(channel)
                if messages:
                    name = self._sanitize_channel_name(channel)
                    self.save_to_data_lake(messages, name)

        except Exception as e:
            logger.error(f"Scraper failed: {e}")
        finally:
            await self.client.disconnect()

async def main():
    # Ensure directories exist
    Path("data/raw/telegram_messages").mkdir(parents=True, exist_ok=True)
    Path("data/raw/images").mkdir(parents=True, exist_ok=True)

    channels = [
        "https://t.me/CheMed123",
        "https://t.me/lobelia4cosmetics",
        "https://t.me/tikvahpharma",
    ]

    scraper = TelegramScraper()
    await scraper.run(channels)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass