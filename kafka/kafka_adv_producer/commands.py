from pydantic import BaseModel

class CreatePeopleCommand(BaseModel):
    cound: int