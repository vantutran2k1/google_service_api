from pydantic import BaseModel


class AttachmentIdNamePair(BaseModel):
	messageId: str
	attachmentId: str
	attachment_file_name: str
