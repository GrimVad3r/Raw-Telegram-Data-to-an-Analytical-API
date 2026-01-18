import os
import json
import logging
import asyncio
from datetime import datetime
from pathlib import Path
from telethon import TelegramClient, connection, errors
from telethon.tl.types import MessageMediaPhoto
from dotenv import load_dotenv

# --- 1. ENHANCED LOGGING & ENVIRONMENT ---
load_dotenv()
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs/scraper.log"), logging.StreamHandler()],
)
logger = logging.getLogger("TelegramScraper")

class TelegramScraper:
    def __init__(self):
        self.api_id = os.getenv("API_ID")
        self.api_hash = os.getenv("API_HASH")
        self.phone = os.getenv("PHONE")
        
        if not self.api_id or not self.api_hash:
            raise ValueError("API_ID or API_HASH missing in .env")

        # Proxy configuration from environment
        proxy_addr = os.getenv("proxy_addr")
        proxy_port = int(os.getenv("proxy_port")) if os.getenv("proxy_port") else None
        proxy_secret = os.getenv("proxy_secret")

        self.client = TelegramClient(
            "medical_scrapping_session",
            int(self.api_id),
            self.api_hash,
            connection=connection.ConnectionTcpMTProxyRandomizedIntermediate,
            proxy=(proxy_addr, proxy_port, proxy_secret)
        )

    def _sanitize_channel_name(self, url: str) -> str:
        return url.strip("/").split("/")[-1].replace("@", "")

    async def download_image(self, message, channel_name):
        """Downloads media with path resolution for YOLO resilience."""
        try:
            image_dir = Path(f"data/raw/images/{channel_name}")
            image_dir.mkdir(parents=True, exist_ok=True)
            
            # Use absolute paths to prevent issues with downstream YOLO services
            image_path = (image_dir / f"{message.id}.jpg").resolve()
            
            path = await self.client.download_media(message, file=str(image_path))
            return str(path) if path else None
        except Exception as e:
            logger.error(f"Media download failed for msg {message.id}: {e}")
            return None

    async def scrape_channel(self, channel_username, limit=100):
        """Scrapes a single channel with internal error handling."""
        channel_name = self._sanitize_channel_name(channel_username)
        messages_data = []
        scraped_at = datetime.utcnow().isoformat() # For dbt Source Freshness

        try:
            logger.info(f"Targeting: {channel_name}")
            async for message in self.client.iter_messages(channel_username, limit=limit):
                message_dict = {
                    'message_id': message.id,
                    'channel_name': channel_name,
                    'message_date': message.date.isoformat(),
                    'message_text': message.message or '',
                    'has_media': message.media is not None,
                    'image_path': None,
                    'views': message.views or 0,
                    'forwards': message.forwards or 0,
                    'scraped_at': scraped_at  # Critical for 'Script 11' dbt tests
                }

                if message.media and isinstance(message.media, MessageMediaPhoto):
                    message_dict["image_path"] = await self.download_image(message, channel_name)

                messages_data.append(message_dict)
            
            return messages_data

        except errors.FloodWaitError as e:
            logger.warning(f"Flood wait for {e.seconds} seconds. Sleeping...")
            await asyncio.sleep(e.seconds)
            return []
        except Exception as e:
            logger.error(f"Unexpected error in {channel_name}: {e}")
            return []

    def save_to_data_lake(self, messages_data, channel_name):
        """Saves raw JSON to a dated folder structure (Data Lake pattern)."""
        if not messages_data: return
        
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            data_dir = Path(f"data/raw/telegram_messages/{today}")
            data_dir.mkdir(parents=True, exist_ok=True)
            
            output_file = data_dir / f"{channel_name}.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(messages_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Successfully archived {len(messages_data)} records for {channel_name}")
        except Exception as e:
            logger.error(f"Storage failure: {e}")

    async def run(self, channels):
        """Main execution loop with rate-limit protection."""
        try:
            await self.client.start(phone=self.phone)
            logger.info("Session Active.")

            for channel in channels:
                data = await self.scrape_channel(channel)
                if data:
                    name = self._sanitize_channel_name(channel)
                    self.save_to_data_lake(data, name)
                
                # Production Resilience: Mandatory jitter to avoid bot detection
                await asyncio.sleep(5) 

        except Exception as e:
            logger.critical(f"Scraper Pipeline Crashed: {e}")
        finally:
            await self.client.disconnect()
            logger.info("Session Closed.")

# --- 2. EXECUTION ---
async def main():
    # Setup core paths
    for p in ["data/raw/telegram_messages", "data/raw/images"]:
        Path(p).mkdir(parents=True, exist_ok=True)

    channels = [
        "https://t.me/CheMed123",
        "https://t.me/lobelia4cosmetics",
        "https://t.me/tikvahpharma",
    ]

    scraper = TelegramScraper()
    await scraper.run(channels)

if __name__ == "__main__":
    asyncio.run(main())