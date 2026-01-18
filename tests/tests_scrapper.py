# tests/python_tests/test_scraper.py
import pytest
from src.scrapper import TelegramScraper

def test_sanitize_channel_name():
    scraper = TelegramScraper()
    # Test different URL formats
    assert scraper._sanitize_channel_name("https://t.me/CheMed123") == "CheMed123"
    assert scraper._sanitize_channel_name("@CheMed123") == "CheMed123"
    assert scraper._sanitize_channel_name("CheMed123/") == "CheMed123"

def test_message_cleaning_logic():
    # Example logic test for your cleaning functions
    sample_text = "  Buy Medicine \n Now!  "
    cleaned = sample_text.strip().replace("\n", "")
    assert cleaned == "Buy Medicine  Now!"