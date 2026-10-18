from pydantic import BaseModel
from typing import Optional


class BettorCreate(BaseModel):
    # Sign in with Apple identityToken (JWT); the bettor's apple_sub comes from it.
    identity_token: str
    # Apple only sends name/email to the app on the first authorization.
    apple_email: Optional[str] = None
    apple_name: Optional[str] = None

class BettorProfile(BaseModel):
    profile_name: Optional[str] = None
    symbol: Optional[str] = None
    color: Optional[str] = None
    apple_email: Optional[str] = None
    favorite_team_id: Optional[int] = None

class BettorDelete(BaseModel):
    # Fresh Sign in with Apple authorizationCode from the app; exchanged for a
    # refresh token so it can be revoked, and its sub must match the bettor.
    authorization_code: Optional[str] = None
