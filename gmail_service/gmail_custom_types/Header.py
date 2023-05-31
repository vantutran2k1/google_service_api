from pydantic import BaseModel


class Header(BaseModel):
	name: str
	value: str
