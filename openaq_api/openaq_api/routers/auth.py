import logging
import os
import pathlib
from datetime import datetime, timezone
from email.message import EmailMessage
import json
from typing import Annotated

import boto3
from fastapi import APIRouter, Depends, HTTPException, Request, status, Form
from fastapi.templating import Jinja2Templates
from passlib.hash import pbkdf2_sha256
from fastapi.responses import RedirectResponse

from ..models.logging import InfoLog


from ..db import DB
from ..forms.register import RegisterForm, UserExistsException
from ..models.auth import User
from ..settings import settings

logger = logging.getLogger("auth")


templates = Jinja2Templates(
    directory=os.path.join(str(pathlib.Path(__file__).parent.parent), "templates")
)


router = APIRouter(include_in_schema=False)


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
            "verify/index.html",
            {"request": request, "error": True, "error_message": message},
        )
    if row[1]:
        # user has already verified their email
        return RedirectResponse("/check-email", status_code=status.HTTP_303_SEE_OTHER)
    if row[2] < datetime.now().replace(tzinfo=timezone.utc):
        # verification code has expired
        message = "Verification token has expired, request a new one."
        return templates.TemplateResponse(
            "verify/index.html",
            {"request": request, "error": True, "error_message": message},
        )
    else:
        token = await db.get_user_token(row[0])
        if request.app.state.redis_client:
            redis_client = request.app.state.redis_client
            redis_client.sadd("keys", token)
        send_api_key_email(token, row[3], row[4])
        return templates.TemplateResponse(
            "verify/index.html", {"request": request, "error": False, "verify": True}
        )


@router.get("/register")
async def get_register(request: Request):
    return templates.TemplateResponse("register/index.html", {"request": request})


@router.post("/register")
async def post_register(
    request: Request,
    fullname: str = Form(),
    emailaddress: str = Form(),
    entitytype: str = Form(),
    password: str = Form(),
    passwordconfirm: str = Form(),
    db: DB = Depends(),
):
    form = RegisterForm(
        fullname, emailaddress, entitytype, password, passwordconfirm, db
    )
    try:
        await form.validate()
    except UserExistsException as e:
        logger.info(InfoLog(detail=f"user already exists - {form.email_address}"))
        return RedirectResponse("/check-email", status_code=status.HTTP_303_SEE_OTHER)
    except Exception as e:
        return e
    password_hash = pbkdf2_sha256.hash(form.password)
    user = User(
        email_address=form.email_address,
        password_hash=password_hash,
        full_name=form.full_name,
        entity_type=form.entity_type,
        ip_address=request.client.host,
    )
    verification_code = await db.create_user(user)
    response = send_verification_email(
        verification_code, form.full_name, form.email_address
    )
    logger.info(InfoLog(detail=json.dumps(response)).model_dump_json())
    return RedirectResponse("/check-email", status_code=status.HTTP_303_SEE_OTHER)


@router.get("/email-key")
async def request_key(request: Request):
    return templates.TemplateResponse("email_key/index.html", {"request": request})


@router.post("/email-key")
async def request_key(
    request: Request,
    emailaddress: str = Form(),
    password: str = Form(),
    db: DB = Depends(),
):
    query = """
    SELECT
        users.password_hash, users.email_address, user_keys.token
    FROM
        users
    JOIN 
        user_keys USING (users_id)
    WHERE
        users.email_address = :email_address
    """
    row = await db.fetchrow(query, {"email_address": emailaddress})
    if len(row) == 0:
        return templates.TemplateResponse(
            "email_key/index.html",
            {"request": request, "error": "Invalid credentials, please try again"},
            status_code=status.HTTP_401_UNAUTHORIZED,
        )
    if pbkdf2_sha256.verify(password, row[0]):
        send_api_key_email(row[2], "", row[1])
        return templates.TemplateResponse(
            "verify/index.html",
            {"request": request},
            status_code=status.HTTP_303_SEE_OTHER,
        )
    else:
        return templates.TemplateResponse(
            "email_key/index.html",
            {"request": request, "error": "Invalid credentials, please try again"},
            status_code=status.HTTP_401_UNAUTHORIZED,
        )
