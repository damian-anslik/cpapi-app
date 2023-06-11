import uuid
import datetime
import pydantic


class Symbol(pydantic.BaseModel):
    ticker: str
    name: str
    logo: str = None

class Token(pydantic.BaseModel):
    access_token: str
    access_token_expires: float
    token_type: str


class TokenData(pydantic.BaseModel):
    username: str | None = None


class Position(pydantic.BaseModel):
    symbol: str
    quantity: float
    side: str|None
    value: float
    conid: int
    pnl: float = 0.0
    id: str = pydantic.Field(default_factory=lambda: str(uuid.uuid4()))

class Order(pydantic.BaseModel):
    symbol: str
    quantity: float
    portfolio_id: str
    side: str
    order_type: str
    limit_price: float = None
    created_at: str = pydantic.Field(default_factory=lambda: str(datetime.datetime.utcnow()))
    id: str = pydantic.Field(default_factory=lambda: str(uuid.uuid4()))

class Portfolio(pydantic.BaseModel):
    name: str
    owner_id: str
    id: str = pydantic.Field(default_factory=lambda: str(uuid.uuid4()))
    is_public: bool = False
    positions: list[Position] = []
    orders: list[Order] = []


class User(pydantic.BaseModel):
    username: str
    email: str
    disabled: bool = False
    id: str = pydantic.Field(default_factory=lambda: str(uuid.uuid4()))
    portfolios: list[dict] = []


class UserInDB(User):
    hashed_password: str
