import logging
import os
import pathlib
from datetime import datetime, timezone
from email.message import EmailMessage

import boto3
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.templating import Jinja2Templates
from passlib.context import CryptContext
from fastapi.responses import RedirectResponse


from ..db import DB
from ..forms.register import RegisterForm
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
    Thank you for signing up for an OpenAQ API Key
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
                    <h1 style='text-align:center'>Thank you for signing up for an OpenAQ API Key</h1>
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


def send_api_key_email(token: str, full_name: str, email: str):
    ses_client = boto3.client("ses")
    TEXT_EMAIL_CONTENT = f"""
    Thank you for registering an OpenAQ API Key
    Your API Key: {token}
    """
    HTML_EMAIL_CONTENT = f"""
        <html>
            <head></head>
            <body>
            <table width="100%" border="0" cellpadding="0" cellspacing="0">
            <tr>
                <td bgcolor="#FFFFFF" style="padding:30px;">
                    <h1 style='text-align:center'>Thank you for signing up for an OpenAQ API Key</h1>
                    <p>You API Key: {token}</p>
                </td>
            </tr>
            </table>
            </body>
        </html>
    """
    msg = EmailMessage()
    msg.set_content(TEXT_EMAIL_CONTENT)
    msg.add_alternative(HTML_EMAIL_CONTENT, subtype="html")
    msg["Subject"] = "OpenAQ - API Key"
    msg["From"] = settings.EMAIL_SENDER
    msg["To"] = email
    response = ses_client.send_raw_email(
        Source=settings.EMAIL_SENDER,
        Destinations=[f"{full_name} <{email}>"],
        RawMessage={"Data": msg.as_string()},
    )
    return response


@router.get("/check-email")
async def check_email(request: Request):
    return templates.TemplateResponse("check_email/index.html", {"request": request})


@router.get("/verify/{verification_code}")
async def verify(request: Request, verification_code: str, db: DB = Depends()):
    query = """
    SELECT 
        users.users_id,  users.is_active, users.expires_on, entities.full_name, users.email_address
    FROM 
        users
    JOIN
        users_entities USING (users_id)
    JOIN 
        entities USING (entities_id)
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
        message = "Verification token has expired, request a new one."
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
        token = await db.get_user_token(row[0])
        # redis_client = request.app.state.redis_client
        # await redis_client.sadd("keys", token)
        send_api_key_email(token, row[3], row[4])
        return templates.TemplateResponse(
            "verify_sent.html", {"request": request, "message": message}
        )


@router.get("/login")
async def get_login(request: Request):
    return templates.TemplateResponse("login/index.html", {"request": request})


@router.get("/register")
async def get_register(request: Request):
    return templates.TemplateResponse("register/index.html", {"request": request})


@router.post("/register")
async def post_register(
    request: Request,
    db: DB = Depends(),
):
    form = RegisterForm(request, db)
    try:
        await form.load_data()
        await form.validate()
    except Exception as e:
        return e
    password_hash = pwd_context.hash(form.password)
    user = User(
        email_address=form.email_address,
        password_hash=password_hash,
        full_name=form.full_name,
        entity_type=form.entity_type,
        ip_address=request.client.host,
    )
    verification_code = await db.create_user(user)
    send_verification_email(verification_code, form.full_name, form.email_address)
    return RedirectResponse("/check-email")
