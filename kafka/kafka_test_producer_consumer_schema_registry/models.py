from pydantic import BaseModel
from typing import Optional

class MobileLog(BaseModel):
    hour: str
    lat: float
    long: float
    signal: int
    network: str
    operator: str
    status: int
    description: str
    speed: float
    satellites: int
    precission: str
    provider: str
    activity: str
    postal_code: Optional[float]