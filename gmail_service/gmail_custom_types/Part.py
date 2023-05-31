from __future__ import annotations
from typing import Optional
from pydantic import BaseModel

from .Header import Header
from .Body import Body


class Part(BaseModel):
	partId: str
	mimeType: str
	filename: str
	headers: list[Header]
	body: Body
	parts: Optional[list[Part]] = None
