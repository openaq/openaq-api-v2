import logging
import os
import pathlib
from datetime import datetime, timezone
from email.message import EmailMessage

import boto3
from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from fastapi.templating import Jinja2Templates
from passlib.context import CryptContext
from pydantic import EmailStr
from zxcvbn import zxcvbn

from ..db import DB
from ..models.auth import User
from ..settings import settings

logger = logging.getLogger("auth")

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

templates = Jinja2Templates(
    directory=os.path.join(str(pathlib.Path(__file__).parent.parent), "templates")
)


router = APIRouter()


credentials_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Could not validate credentials",
    headers={"WWW-Authenticate": "Bearer"},
)

incorrect_username_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Incorrect username or password",
    headers={"WWW-Authenticate": "Bearer"},
)


def send_verification_email(verifiation_code: str, full_name: str, email: str):
    ses_client = boto3.client("ses")
    TEXT_EMAIL_CONTENT = f"""
    Thank your for signing up for an OpenAQ API Key
    Visit the following URL to verify your email:
    https://api.openaq.org/verify/{verifiation_code}
    """
    HTML_EMAIL_CONTENT = f"""
        <html>
            <head></head>
            <body>
            <table width="100%" border="0" cellpadding="0" cellspacing="0">
            <tr>
                <td bgcolor="#FFFFFF" style="padding:30px;">
                    <h1 style='text-align:center'>Thank your for signing up for an OpenAQ API Key</h1>
                    <p>Click the following link to verify your email:</p>
                    <a href="https://api.openaq.org/verify/{verifiation_code}">https://api.openaq.org/verify/{verifiation_code}</a>
                </td>
            </tr>
            </table>
            </body>
        </html>
    """
    msg = EmailMessage()
    msg.set_content(TEXT_EMAIL_CONTENT)
    msg.add_alternative(HTML_EMAIL_CONTENT, subtype="html")
    msg["Subject"] = "OpenAQ API - Verify your email"
    msg["From"] = settings.EMAIL_SENDER
    msg["To"] = email
    response = ses_client.send_raw_email(
        Source=settings.EMAIL_SENDER,
        Destinations=[f"{full_name} <{email}>"],
        RawMessage={"Data": msg.as_string()},
    )
    return response


async def check_user_exists(db: DB, email_address: str) -> bool:
    query = """
    SELECT 
        email_address
    FROM
        users
    WHERE email_address = :email_address
    """
    user = await db.fetch(query, {"email_address": email_address})
    if user:
        return True
    else:
        return False


@router.get("/verify/{verification_code}")
async def verify(request: Request, verification_code: str, db: DB = Depends()):
    query = """
    SELECT 
        users_id, is_active, expires_on
    FROM 
        users
    WHERE
        verification_code = :verification_code
    """
    row = await db.fetchrow(query, {"verification_code": verification_code})
    message = ""
    if len(row) == 0:
        # verification code not found
        message = "Not a valid verification code"
        return templates.TemplateResponse(
            "verify_error.html", {"request": request, "message": message}
        )
    if row[1]:
        # user has already verified their email
        message = "Email already verified"
        return templates.TemplateResponse(
            "verify_error.html", {"request": request, "message": message}
        )
    if row[2] < datetime.now().replace(tzinfo=timezone.utc):
        # verification code has expired
        message = "Verification token has expired, request a new one."
        return templates.TemplateResponse(
            "verify_error.html", {"request": request, "message": message}
        )
    else:
        message = "Email address verified. You will recieve a email containing your OpenAQ API key shortly."
        token = db.verify_user(row[0])
        redis_client = request.app.state.redis_client
        await redis_client.sadd("keys", token)
        return templates.TemplateResponse(
            "verify_sent.html", {"request": request, "message": message}
        )


@router.get("/register")
async def get_register(request: Request):
    return templates.TemplateResponse("register/index.html", {"request": request})


@router.post("/register")
async def post_register(
    request: Request,
    full_name: str = Form(),
    entity_type: str = Form(),
    email_address: EmailStr = Form(),
    password: str = Form(min_length=8),
    password_verify: str = Form(min_length=8),
    db: DB = Depends(),
):
    password_strength = zxcvbn(password)
    if password_strength.get("score", 0) < 3:
        warning = password_strength["feedback"]["warning"]
        suggestions = password_strength["feedback"]["suggestions"]
        suggestions = ",".join(suggestions)
        return HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"password not strong enough. {warning} {suggestions}",
        )
    if password != password_verify:
        return HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="passwords do not match",
        )
    existing_user = await check_user_exists(db, email_address)
    if existing_user:
        return HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="user already exists with that email",
        )
    password_hash = pwd_context.hash(password)
    user = User(
        email_address=email_address,
        password_hash=password_hash,
        full_name=full_name,
        entity_type=entity_type,
        ip_address=request.client.host,
    )
    verification_code = await db.create_user(user)
    send_verification_email(verification_code, full_name, email_address)
