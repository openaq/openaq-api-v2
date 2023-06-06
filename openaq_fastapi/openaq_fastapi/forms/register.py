from zxcvbn import zxcvbn
from fastapi import HTTPException, Request, status


from ..db import DB


class RegisterForm:
    def __init__(self, request: Request, db: DB):
        self.request: Request = request
        self.full_name: str
        self.email_address: str
        self.entity_type: str
        self.password: str
        self.password_confirm: str
        self.db: DB = db

    async def load_data(self):
        form_data = await self.request.form()
        self.full_name = form_data.get("fullname")
        self.email_address = form_data.get("emailaddress")
        self.entity_type = form_data.get("entitytype")
        self.password = form_data.get("password")
        self.password_confirm = form_data.get("passwordconfirm")
        if self.password == None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"password required",
            )

    async def _check_user_exists(self) -> bool:
        query = """
        SELECT 
            email_address
        FROM
            users
        WHERE email_address = :email_address
        """
        user = await self.db.fetch(query, {"email_address": self.email_address})
        if user:
            return True
        else:
            return False

    async def validate(self):
        password_strength = zxcvbn(self.password)
        if password_strength.get("score", 0) < 3:
            warning = password_strength["feedback"]["warning"]
            suggestions = password_strength["feedback"]["suggestions"]
            suggestions = ",".join(suggestions)
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"password not strong enough. {warning} {suggestions}",
            )
        if self.password != self.password_confirm:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="passwords do not match",
            )
        existing_user = await self._check_user_exists()
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="user already exists with that email",
            )
