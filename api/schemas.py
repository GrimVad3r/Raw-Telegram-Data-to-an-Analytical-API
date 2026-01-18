from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime, date
from typing import Optional

# --- 1. PRODUCT MODELS ---

class TopProduct(BaseModel):
    """Schema for high-frequency product term analysis."""
    product_term: str = Field(..., description="The medical/pharma term identified in the message", example="paracetamol")
    mention_count: int = Field(..., ge=0, description="Total number of times this term appeared")

    model_config = ConfigDict(from_attributes=True)

# --- 2. CHANNEL ACTIVITY MODELS ---

class ChannelActivity(BaseModel):
    """Daily metrics for a specific Telegram channel."""
    date: date = Field(..., description="The calendar date for activity tracking")
    message_count: int = Field(..., ge=0, description="Total messages posted on this day")
    total_views: int = Field(..., ge=0, description="Aggregate view count across all messages for this day")

    model_config = ConfigDict(from_attributes=True)

# --- 3. SEARCH MODELS ---

class MessageSearch(BaseModel):
    """Schema for searching through raw message history."""
    message_id: int = Field(..., description="Unique ID of the Telegram message")
    channel_name: str = Field(..., description="Source channel name")
    message_date: datetime = Field(..., description="Timestamp when the message was posted")
    message_text: Optional[str] = Field(None, description="The textual content of the message")
    views: int = Field(0, ge=0, description="Number of views at the time of scraping")

    model_config = ConfigDict(from_attributes=True)

# --- 4. VISUAL CONTENT MODELS ---

class VisualContentStats(BaseModel):
    """Aggregated stats from the YOLO image enrichment pipeline."""
    channel_name: str = Field(..., description="The name of the Telegram channel")
    total_messages: int = Field(..., ge=0)
    messages_with_images: int = Field(..., ge=0)
    image_percentage: float = Field(..., ge=0.0, le=100.0, description="Percentage of messages containing images")
    promotional_count: int = Field(..., ge=0, description="Count of images classified as promotional/banners")
    product_display_count: int = Field(..., ge=0, description="Count of images showing specific medical products")

    model_config = ConfigDict(from_attributes=True)