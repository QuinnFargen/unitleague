from pydantic import BaseModel
from typing import Optional


class EnhancedCreate(BaseModel):
    bettor_id: int
    syndicate_id: int
    runner_id: int
    enhancement_id: int
    team_id: Optional[int] = 0
    level: Optional[int] = 1
    option_hash: str
    sell_enhanced_id: Optional[int] = None
