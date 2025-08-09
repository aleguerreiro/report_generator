import os
import smtplib
import mimetypes
import logging
from email.message import EmailMessage
from typing import Union, List


def send_email_with_attachment(
    from_email: str,
    to_email: Union[str, List[str]],
    subject: str,
    body: str,
    app_password: str,
    attachment_path: str
):
    """
    Envia um e-mail com um anexo (arquivo Excel ou outro).

    Args:
        from_email (str): E-mail remetente.
        to_email (str | list): Destinatário(s).
        subject (str): Assunto do e-mail.
        body (str): Corpo do e-mail.
        app_password (str): Senha de app do Gmail.
        attachment_path (str): Caminho do arquivo a anexar.
    """
    if isinstance(to_email, str):
        to_email = [to_email]

    msg = EmailMessage()
    msg["From"] = from_email
    msg["To"] = ", ".join(to_email)
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        with open(attachment_path, "rb") as file:
            file_data = file.read()
            file_name = os.path.basename(attachment_path)
            mime_type, _ = mimetypes.guess_type(file_name)
            main_type, sub_type = mime_type.split("/", 1) if mime_type else ("application", "octet-stream")
            msg.add_attachment(file_data, maintype=main_type, subtype=sub_type, filename=file_name)
    except Exception as e:
        logging.error(f"❌ Erro ao anexar o arquivo '{attachment_path}': {e}")
        return

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(from_email, app_password)
            smtp.send_message(msg)
            logging.info(f"📧 E-mail com anexo enviado com sucesso para: {to_email}")
    except Exception as e:
        logging.error(f"❌ Erro ao enviar e-mail com anexo: {e}")


def send_simple_email(
    from_email: str,
    to_emails: Union[str, List[str]],
    subject: str,
    body: str,
    app_password: str
):
    """
    Envia um e-mail simples, sem anexos.

    Args:
        from_email (str): E-mail remetente.
        to_emails (str | list): Destinatário(s).
        subject (str): Assunto do e-mail.
        body (str): Corpo do e-mail.
        app_password (str): Senha de app do Gmail.
    """
    if isinstance(to_emails, str):
        to_emails = [to_emails]

    msg = EmailMessage()
    msg["From"] = from_email
    msg["To"] = ", ".join(to_emails)
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(from_email, app_password)
            smtp.send_message(msg)
            logging.info(f"📧 E-mail simples enviado com sucesso para: {to_emails}")
    except Exception as e:
        logging.error(f"❌ Erro ao enviar e-mail simples: {e}")
