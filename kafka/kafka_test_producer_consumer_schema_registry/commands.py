from pydantic import BaseModel, Field

class CreateLogsCommand(BaseModel):
    count: int = Field(..., gt=0)