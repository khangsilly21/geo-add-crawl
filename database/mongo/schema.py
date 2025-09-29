from datetime import datetime, timezone
from typing import Annotated

from beanie import Document, Indexed
from pydantic import Field


def get_current_utc_time() -> datetime:
    return datetime.now(tz= timezone.utc)
class Wallet(Document):
    address: Annotated[str,Indexed(unique=True)]
    label: str
    created_at: Annotated[datetime, Indexed()] = Field(
        default_factory=get_current_utc_time
    )
    
    

    