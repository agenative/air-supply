from pydantic import BaseModel
from typing import Dict, Any

class TariffRequest(BaseModel):
    product: str
    partner: str
    reporter: str
    year: int

class TariffResponse(BaseModel):
    hs_code: str
    reason: Dict[str, Any]
    tariff: float | None