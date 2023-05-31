from typing import Optional
from pydantic import BaseModel

from .Payload import Payload


class GmailMessage(BaseModel):
	id: str
	threadId: str
	labelIds: Optional[list[str]]
	snippet: Optional[str]
	payload: Optional[Payload]
	sizeEstimate: Optional[int]
	historyId: Optional[str]
	internalDate: Optional[str]
