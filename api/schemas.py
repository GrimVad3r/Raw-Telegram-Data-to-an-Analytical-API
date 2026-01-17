from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class TopProduct(BaseModel):
    product_term: str
    mention_count: int
    
class ChannelActivity(BaseModel):
    date: datetime
    message_count: int
    total_views: int
    
class MessageSearch(BaseModel):
    message_id: int
    channel_name: str
    message_date: datetime
    message_text: str
    views: int
    
class VisualContentStats(BaseModel):
    channel_name: str
    total_messages: int
    messages_with_images: int
    image_percentage: float
    promotional_count: int
    product_display_count: int