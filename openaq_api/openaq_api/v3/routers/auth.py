import json
import logging
import os
import pathlib
from email.message import EmailMessage

import boto3
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from openaq_api.db import DB
from openaq_api.models.logging import AuthLog, ErrorLog, InfoLog, SESEmailLog
from openaq_api.settings import settings

logger = logging.getLogger("auth")


templates = Jinja2Templates(
    directory=os.path.join(str(pathlib.Path(__file__).parent.parent), "templates")
)

router = APIRouter(
    include_in_schema=False,
)


def send_verification_email(verification_code: str, full_name: str, email: str):
    ses_client = boto3.client("ses")
    TEXT_EMAIL_CONTENT = f"""
    Thank you for signing up for an OpenAQ API Key
    Visit the following URL to verify your email:
    https://api.openaq.org/verify/{verification_code}
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
                    <a href="https://api.openaq.org/verify/{verification_code}">https://api.openaq.org/verify/{verification_code}</a>
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


@router.get("/auth-test")
async def auth_test(
    request: Request,
):
    print("REQUEST", request.__dict__)
    # redis_client = getattr(request.auth_app.state, "redis_client")
    # print(redis_client)
    return {"FOO": "BAR"}


@router.post("/regenerate-token")
async def get_register(
    request: Request,
    users_id: int,
    token: str,
    db: DB = Depends(),
):
    """ """
    _token = token
    try:
        db.get_user_token
        await db.regenerate_user_token(users_id, _token)
        token = await db.get_user_token(users_id)
        redis_client = getattr(request.auth_app.state, "redis_client")
        if redis_client:
            await redis_client.srem("keys", _token)
            await redis_client.sadd("keys", token)
        return {"success"}
    except Exception as e:
        return e


@router.post("/send-verification")
async def get_register(
    request: Request,
    users_id: int,
    db: DB = Depends(),
):
    user = db.get_user(users_id=users_id)
    full_name = user[0]
    email_address = user[1]
    verification_code = user[2]
    response = send_verification_email(verification_code, full_name, email_address)
    logger.info(InfoLog(detail=json.dumps(response)).model_dump_json())
