from pydantic import BaseModel
from typing import Optional


class BettorCreate(BaseModel):
    apple_sub: str
    apple_email: Optional[str] = None
    apple_name: Optional[str] = None
    apple_refresh_token: Optional[str] = None

class BettorProfile(BaseModel):
    profile_name: Optional[str] = None
    symbol: Optional[str] = None
    color: Optional[str] = None
    apple_email: Optional[str] = None
    favorite_team_id: Optional[int] = None

class BettorSignin(BaseModel):
    bettor_id: Optional[int] = None
    apple_sub: Optional[str] = None

class BettorDelete(BaseModel):
    # Fresh Sign in with Apple authorizationCode from the app; exchanged for a
    # refresh token so it can be revoked, and its sub must match the bettor.
    authorization_code: Optional[str] = None
