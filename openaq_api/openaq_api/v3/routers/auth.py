import json
import logging
import os
import pathlib
from email.message import EmailMessage

import boto3
from fastapi import APIRouter, Body, Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from openaq_api.db import DB
from openaq_api.models.logging import AuthLog, ErrorLog, InfoLog, SESEmailLog
from openaq_api.settings import settings
from openaq_api.v3.models.responses import JsonBase

logger = logging.getLogger("auth")


templates = Jinja2Templates(
    directory=os.path.join(str(pathlib.Path(__file__).parent.parent), "templates")
)

router = APIRouter(
    prefix="/auth",
    include_in_schema=False,
)


def send_change_password_email(full_name: str, email: str):
    ses_client = boto3.client("ses")
    TEXT_EMAIL_CONTENT = """
    We are contacting you to notify you that your OpenAQ Explorer password has been changed.

    If you did not make this change, please contact info@openaq.org.
    """
    HTML_EMAIL_CONTENT = """
        <html>
            <head></head>
            <body>
            <table width="100%" border="0" cellpadding="0" cellspacing="0">
            <tr>
                <td bgcolor="#FFFFFF" style="padding:30px;">
                    <p>We are contacting you to notify you that your OpenAQ Explorer password has been changed.</p>
                    <p>If you did not make this change, please contact <a href="mailto:info@openaq.org">info@openaq.org</a>.</p>
                </td>
            </tr>
            </table>
            </body>
        </html>
    """
    msg = EmailMessage()
    msg.set_content(TEXT_EMAIL_CONTENT)
    msg.add_alternative(HTML_EMAIL_CONTENT, subtype="html")
    msg["Subject"] = "OpenAQ Explorer - Password changed"
    msg["From"] = settings.EMAIL_SENDER
    msg["To"] = email
    response = ses_client.send_raw_email(
        Source=settings.EMAIL_SENDER,
        Destinations=[f"{full_name} <{email}>"],
        RawMessage={"Data": msg.as_string()},
    )
    logger.info(
        SESEmailLog(
            detail=json.dumps(
                {
                    "email": email,
                    "name": full_name,
                    "reponse": response,
                }
            )
        ).model_dump_json()
    )
    return response


def send_verification_email(verification_code: str, full_name: str, email: str):
    ses_client = boto3.client("ses")
    TEXT_EMAIL_CONTENT = f"""
    Thank you for signing up for an OpenAQ Explorer Account
    Visit the following URL to verify your email:
    https://explore.openaq.org/verify/{verification_code}
    """
    HTML_EMAIL_CONTENT = f"""
        <html>
            <head></head>
            <body>
            <table width="100%" border="0" cellpadding="0" cellspacing="0">
            <tr>
                <td bgcolor="#FFFFFF" style="padding:30px;">
                    <h1 style='text-align:center'>Thank you for signing up for an OpenAQ Explorer Account</h1>
                    <p>Click the following link to verify your email:</p>
                    <a href="https://explore.openaq.org/verify/{verification_code}">https://explore.openaq.org/verify/{verification_code}</a>
                </td>
            </tr>
            </table>
            </body>
        </html>
    """
    msg = EmailMessage()
    msg.set_content(TEXT_EMAIL_CONTENT)
    msg.add_alternative(HTML_EMAIL_CONTENT, subtype="html")
    msg["Subject"] = "OpenAQ Explorer - Verify your email"
    msg["From"] = settings.EMAIL_SENDER
    msg["To"] = email
    response = ses_client.send_raw_email(
        Source=settings.EMAIL_SENDER,
        Destinations=[f"{full_name} <{email}>"],
        RawMessage={"Data": msg.as_string()},
    )
    logger.info(
        SESEmailLog(
            detail=json.dumps(
                {
                    "email": email,
                    "name": full_name,
                    "verificationCode": verification_code,
                    "reponse": response,
                }
            )
        ).model_dump_json()
    )
    return response


def send_password_reset_email(verification_code: str, email: str):
    ses_client = boto3.client("ses")
    TEXT_EMAIL_CONTENT = f"""
    You have requested a password reset for your OpenAQ Explorer account. Please visit the following link (expires in 30 minutes):
    https://explore.openaq.org/new-password?code={verification_code}
    """
    HTML_EMAIL_CONTENT = f"""
        <html>
            <head></head>
            <body>
            <table width="100%" border="0" cellpadding="0" cellspacing="0">
            <tr>
                <td bgcolor="#FFFFFF" style="padding:30px;">
                    <h1 style='text-align:center'>OpenAQ password reset requests</h1>
                    <p>You have requested a password reset for your OpenAQ Explorer account. Please visit the following link (expires in 30 minutes):</p>
                    <a href="https://explore.openaq.org/new-password?code={verification_code}">https://explore.openaq.org/new-password?code={verification_code}</a>
                </td>
            </tr>
            </table>
            </body>
        </html>
    """
    msg = EmailMessage()
    msg.set_content(TEXT_EMAIL_CONTENT)
    msg.add_alternative(HTML_EMAIL_CONTENT, subtype="html")
    msg["Subject"] = "OpenAQ Explorer - Reset password request"
    msg["From"] = settings.EMAIL_SENDER
    msg["To"] = email
    response = ses_client.send_raw_email(
        Source=settings.EMAIL_SENDER,
        Destinations=[f"<{email}>"],
        RawMessage={"Data": msg.as_string()},
    )
    logger.info(
        SESEmailLog(
            detail=json.dumps(
                {
                    "email": email,
                    "verificationCode": verification_code,
                    "reponse": response,
                }
            )
        ).model_dump_json()
    )
    return response



def send_password_changed_email(email: str):
    ses_client = boto3.client("ses")
    TEXT_EMAIL_CONTENT = """
        This email confirms you have successfully changed your password for you OpenAQ Explorer account.

        If you believe you have recieved this email in error please reach out to info@openaq.org.
    """
    HTML_EMAIL_CONTENT = """
        <html>
            <head></head>
            <body>
            <table width="100%" border="0" cellpadding="0" cellspacing="0">
            <tr>
                <td bgcolor="#FFFFFF" style="padding:30px;">
                    <h1 style='text-align:center'>OpenAQ password reset</h1>
                    <p>This email confirms you have successfully changed your password for you OpenAQ Explorer account.</p>
                    <p>If you believe you have recieved this email in error please reach out to info@openaq.org</p>
                </td>
            </tr>
            </table>
            </body>
        </html>
    """
    msg = EmailMessage()
    msg.set_content(TEXT_EMAIL_CONTENT)
    msg.add_alternative(HTML_EMAIL_CONTENT, subtype="html")
    msg["Subject"] = "OpenAQ Explorer - Reset password success"
    msg["From"] = settings.EMAIL_SENDER
    msg["To"] = email
    response = ses_client.send_raw_email(
        Source=settings.EMAIL_SENDER,
        Destinations=[f"<{email}>"],
        RawMessage={"Data": msg.as_string()},
    )
    logger.info(
        SESEmailLog(
            detail=json.dumps(
                {
                    "email": email,
                    "reponse": response,
                }
            )
        ).model_dump_json()
    )
    return response

class RegenerateTokenBody(JsonBase):
    users_id: int
    token: str


@router.post("/regenerate-token")
async def get_register(
    request: Request,
    body: RegenerateTokenBody,
    db: DB = Depends(),
):
    """ """
    try:
        user_token = await db.get_user_token(body.users_id)
        if user_token != body.token:
            return HTTPException(401)
        await db.regenerate_user_token(body.users_id, body.token)
        new_token = await db.get_user_token(body.users_id)
        redis_client = getattr(request.app.state, "redis_client")
        if redis_client:
            await redis_client.srem("keys", body.token)
            await redis_client.sadd("keys", new_token)
        return {"message": "success"}
    except Exception as e:
        return e


class VerificationBody(JsonBase):
    users_id: int


@router.post("/send-verification")
async def send_verification(
    body: VerificationBody,
    db: DB = Depends(),
):
    user = await db.get_user(body.users_id)
    full_name = user[0]
    email_address = user[1]
    verification_code = user[2]
    response = send_verification_email(verification_code, full_name, email_address)
    logger.info(InfoLog(detail=json.dumps(response)).model_dump_json())


class PasswordResetEmailBody(JsonBase):
    email_address: str


@router.post("/send-password-email")
async def request_password_reset_email(
    body: PasswordResetEmailBody,
    db: DB = Depends(),
):
    email_address = body.email_address
    verification_code = await db.generate_verification_code(email_address)
    response = send_password_reset_email(verification_code, email_address)
    logger.info(InfoLog(detail=json.dumps(response)).model_dump_json())


@router.post("/send-password-changed-email")
async def password_changed_email(
    body: PasswordResetEmailBody,
):
    email_address = body.email_address
    response = send_password_changed_email(email_address)
    logger.info(InfoLog(detail=json.dumps(response)).model_dump_json())


class VerifyBody(JsonBase):
    users_id: int


@router.post("/verify")
async def verify_email(
    request: Request,
    body: VerificationBody,
    db: DB = Depends(),
):
    try:
        token = await db.get_user_token(body.users_id)
        redis_client = getattr(request.app.state, "redis_client")
        if redis_client:
            await redis_client.sadd("keys", token)
    except Exception as e:
        logger.error(ErrorLog(detail=f"something went wrong: {e}"))
        return HTTPException(500)
    return {"message": "success"}


class ChangePasswordEmailBody(JsonBase):
    users_id: int


@router.post("/change-password-email")
async def change_password_email(
    body: ChangePasswordEmailBody,
    db: DB = Depends(),
):
    """ """
    user = await db.get_user(body.users_id)
    full_name = user[0]
    email_address = user[1]
    response = send_change_password_email(full_name, email_address)
    logger.info(InfoLog(detail=json.dumps(response)).model_dump_json())
