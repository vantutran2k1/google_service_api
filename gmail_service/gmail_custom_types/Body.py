from typing import Optional
from pydantic import BaseModel


class Body(BaseModel):
	size: int
	data: Optional[str] = None
	attachmentId: Optional[str] = None
