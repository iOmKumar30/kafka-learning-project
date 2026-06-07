from pydantic import BaseModel

from datetime import datetime, timezone

import uuid

class UserEvent(BaseModel):
    event_id: str = str(uuid.uuid4())
    user_id: str
    action: str
    timestamp: datetime = datetime.now(timezone.utc)