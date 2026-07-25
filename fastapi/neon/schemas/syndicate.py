from pydantic import BaseModel
from typing import Optional


class SyndicateCreate(BaseModel):
    bettor_id: int
    name: str
    description: Optional[str] = None
    is_public: Optional[bool] = False
    password: Optional[str] = None
    max_runner: Optional[int] = None
    start_units: Optional[int] = None
    config: Optional[dict] = None

class RunnerCreate(BaseModel):
    bettor_id: int
    password: Optional[str] = None

class SyndicateStart(BaseModel):
    bettor_id: int

class SyndicateUpdate(BaseModel):
    name: Optional[str] = None
    symbol: Optional[str] = None
    color: Optional[str] = None
    config: Optional[dict] = None

class RunnerProfile(BaseModel):
    profile_name: Optional[str] = None
    symbol: Optional[str] = None
    color: Optional[str] = None
