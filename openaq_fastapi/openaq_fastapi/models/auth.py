from pydantic import BaseModel


class User(BaseModel):
    full_name: str
    email_address: str
    password_hash: str
    entity_type: str
    ip_address: str
