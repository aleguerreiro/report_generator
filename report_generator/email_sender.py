import os
import smtplib
import mimetypes
import logging
from email.message import EmailMessage
from typing import Union, List, Optional
import html as _html


# ---------------------------
# Helpers
# ---------------------------

def _normalize_recipients(val: Union[str, List[str], None]) -> List[str]:
    """
    Aceita:
      - string com e-mails separados por vírgula/semicolon/quebra de linha/espacos
      - lista de e-mails
      - None
    Retorna lista normalizada, sem vazios e sem duplicados simples (case-insensitive).
    """
    if val is None:
        return []
    if isinstance(val, list):
        parts = val
    else:
        s = str(val).replace("\xa0", " ").strip()
        for sep in [",", ";", "\n"]:
            s = s.replace(sep, " ")
        parts = s.split()

    out, seen = [], set()
    for p in parts:
        e = (p or "").strip()
        if not e:
            continue
        k = e.lower()
        if k not in seen:
            seen.add(k)
            out.append(e)
    return out


def _set_bodies(msg: EmailMessage, body_text: Optional[str], body_html: Optional[str]) -> None:
    """
    Define o corpo do e-mail:
      - Sempre inclui uma parte text/plain (fallback).
      - Se body_html vier, inclui como text/html (desescapando se necessário).
      - Se body_html não vier, tenta heurística: se body_text tiver tags, também adiciona text/html.
    """
    txt = (body_text or "").strip()
    html_part = (body_html or "").strip()

    # Sempre texto puro
    msg.set_content(txt, subtype="plain")

    # HTML explícito
    if html_part:
        unescaped = _html.unescape(html_part)
        html_final = unescaped if ("<" in unescaped and ">" in unescaped) else html_part
        if "<html" not in html_final.lower():
            html_final = f"<!doctype html><html><body>{html_final}</body></html>"
        msg.add_alternative(html_final, subtype="html")
        return

    # Heurística: texto parece HTML
    if txt and ("<" in txt and ">" in txt):
        html_final = txt
        if "<html" not in html_final.lower():
            html_final = f"<!doctype html><html><body>{html_final}</body></html>"
        msg.add_alternative(html_final, subtype="html")


def _attach_file(msg: EmailMessage, attachment_path: str) -> bool:
    """
    Tenta anexar o arquivo. Retorna True se anexou, False caso contrário (com logs).
    """
    if not attachment_path:
        logging.warning("⚠️ attachment_path vazio; nenhum anexo será incluído.")
        return False

    if not os.path.exists(attachment_path):
        logging.error("❌ Anexo não encontrado: %s", attachment_path)
        return False

    try:
        file_name = os.path.basename(attachment_path)
        mime_type, _ = mimetypes.guess_type(file_name)
        maintype, subtype = ("application", "octet-stream")
        if mime_type:
            try:
                maintype, subtype = mime_type.split("/", 1)
            except Exception:
                pass

        with open(attachment_path, "rb") as f:
            msg.add_attachment(
                f.read(),
                maintype=maintype,
                subtype=subtype,
                filename=file_name
            )
        return True
    except Exception as e:
        logging.error("❌ Erro ao anexar o arquivo '%s': %s", attachment_path, e)
        return False


# ---------------------------
# API pública
# ---------------------------

def send_email_with_attachment(
    from_email: str,
    to_email: Union[str, List[str]],
    subject: str,
    body: str,
    app_password: str,
    attachment_path: str,
    body_html: Optional[str] = None,   # NOVO: HTML opcional
):
    """
    Envia e-mail com anexo.
    Compatível com a versão anterior: se destinatários vazios, apenas loga e não envia.
    Suporta HTML via parâmetro opcional body_html.
    """
    recipients = _normalize_recipients(to_email)

    msg = EmailMessage()
    msg["From"] = from_email
    msg["To"] = ", ".join(recipients) if recipients else ""
    msg["Subject"] = subject

    _set_bodies(msg, body_text=body, body_html=body_html)
    anexou = _attach_file(msg, attachment_path)

    if not recipients:
        logging.warning("⚠️ Nenhum destinatário em 'to_email'. E-mail NÃO será enviado. subject=%r", subject)
        return

    if not anexou:
        logging.warning("⚠️ E-mail não enviado porque o anexo falhou/ausente. subject=%r", subject)
        return

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(from_email, app_password)
            smtp.send_message(msg)
            logging.info("📧 E-mail com anexo enviado com sucesso para: %s", recipients)
    except Exception as e:
        logging.error("❌ Erro ao enviar e-mail com anexo: %s", e)


def send_simple_email(
    from_email: str,
    to_emails: Union[str, List[str]],
    subject: str,
    body: str,
    app_password: str,
    body_html: Optional[str] = None,   # NOVO: HTML opcional
):
    """
    Envia e-mail simples (sem anexo).
    Compatível com a versão anterior: se destinatários vazios, apenas loga e não envia.
    Suporta HTML via parâmetro opcional body_html.
    """
    recipients = _normalize_recipients(to_emails)

    msg = EmailMessage()
    msg["From"] = from_email
    msg["To"] = ", ".join(recipients) if recipients else ""
    msg["Subject"] = subject

    _set_bodies(msg, body_text=body, body_html=body_html)

    if not recipients:
        logging.warning("⚠️ Nenhum destinatário em 'to_emails'. E-mail simples NÃO será enviado. subject=%r", subject)
        return

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(from_email, app_password)
            smtp.send_message(msg)
            logging.info("📧 E-mail simples enviado com sucesso para: %s", recipients)
    except Exception as e:
        logging.error("❌ Erro ao enviar e-mail simples: %s", e)
