from typing import Optional
from pydantic import BaseModel

from .Header import Header
from .Body import Body
from .Part import Part


class Payload(BaseModel):
	partId: str
	mimeType: str
	filename: str
	headers: list[Header]
	body: Body
	parts: Optional[list[Part]]
