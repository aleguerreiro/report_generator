# email_sender.py

import os
import smtplib
import mimetypes
import logging
from email.message import EmailMessage

def send_email_with_attachment(from_email, to_email, subject, body, app_password, attachment_path):
    """
    Envia um e-mail com um anexo (arquivo Excel).

    Args:
        from_email (str): E-mail remetente.
        to_email (str): Lista ou string de destinatários.
        subject (str): Assunto do e-mail.
        body (str): Corpo do e-mail.
        app_password (str): Senha do app (Gmail).
        attachment_path (str): Caminho completo do anexo.
    """
    msg = EmailMessage()
    msg["From"] = from_email
    msg["To"] = to_email
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
        logging.error(f"❌ Erro ao anexar o arquivo: {e}")
        return

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(from_email, app_password)
            smtp.send_message(msg)
            logging.info(f"📧 E-mail enviado com sucesso para: {to_email}")
    except Exception as e:
        logging.error(f"❌ Erro ao enviar e-mail: {e}")
