from pydantic import BaseModel


class CreateCommand(BaseModel):
  count: int
  age: int

