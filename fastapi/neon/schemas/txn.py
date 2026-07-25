from pydantic import BaseModel
from typing import Optional, List


class TxnCreate(BaseModel):
    bettor_id: int
    syndicate_id: int
    bet_hash: str
    unit: float
    price: float
    game_dt: Optional[str] = None
    unit_enhanced: Optional[float] = None
    price_enhanced: Optional[float] = None

class ParlayLeg(BaseModel):
    bet_hash: str
    price: float

class ParlayCreate(BaseModel):
    bettor_id: int
    syndicate_id: int
    unit: float
    legs: List[ParlayLeg]
    game_dt: Optional[str] = None
    unit_enhanced: Optional[float] = None
    price_enhanced: Optional[float] = None
