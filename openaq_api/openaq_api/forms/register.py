from zxcvbn import zxcvbn
from fastapi import HTTPException, status
from email_validator import validate_email, EmailNotValidError


from ..db import DB


class UserExistsException(Exception):
    ...


class RegisterForm:
    def __init__(
        self,
        full_name: str,
        email_address: str,
        entity_type: str,
        password: str,
        password_confirm: str,
        db: DB,
    ):
        self.full_name = full_name
        self.email_address = email_address
        self.entity_type = entity_type
        self.password = password
        self.password_confirm = password_confirm
        self._db: DB = db

    async def _check_user_exists(self) -> bool:
        query = """
        SELECT 
            email_address
        FROM
            users
        WHERE email_address = :email_address
        """
        user = await self._db.fetch(query, {"email_address": self.email_address})
        if user:
            return True
        else:
            return False

    async def validate(self):
        try:
            emailinfo = validate_email(self.email_address, check_deliverability=False)
            self.email_address = emailinfo.normalized.lower()
        except EmailNotValidError as e:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"valid email required",
            )
        if self.entity_type not in ("Person", "Organization"):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"entity type required (Person or Organization)",
            )
        if self.password == None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"password required",
            )
        if self.password != self.password_confirm:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="passwords do not match",
            )
        password_strength = zxcvbn(self.password)
        if password_strength.get("score", 0) < 3:
            warning = password_strength["feedback"]["warning"]
            suggestions = password_strength["feedback"]["suggestions"]
            suggestions = ",".join(suggestions)
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"password not strong enough. {warning} {suggestions}",
            )
        existing_user = await self._check_user_exists()
        if existing_user:
            raise UserExistsException
