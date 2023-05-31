from pydantic import BaseModel

from .GmailMessage import GmailMessage


class GmailThread(BaseModel):
	id: str
	historyId: str
	messages: list[GmailMessage]
