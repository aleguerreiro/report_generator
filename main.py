import time
import json
import logging
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from report_generator.schedule_handler import ask_schedule_execution, esperar_proxima_execucao
from report_generator.process_executor import executar_processo
from report_generator.email_sender import send_simple_email


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # 📧 Carrega credenciais do e-mail
    with open("email_credentials.json", "r") as f:
        email_config = json.load(f)
    FROM_EMAIL = email_config["email"]
    APP_PASSWORD = email_config["senha_app"]
    DESTINATARIOS = ["alexandre@zapform.com.br", "ismael@zapform.com.br", "raphael@zapform.com.br"]

    # 🔐 Logins da API Zapform
    logins = [
        {"username": "integracao-ale6", "password": "Zapform@06"},
        {"username": "integracao-ale5", "password": "Zapform@05"},
        {"username": "integracao-ale4", "password": "Zapform@04"},
    ]



    # 🔗 Conecta ao Google Sheets
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open("Report_Config")

    # ⏳ Pergunta o modo de execução
    modo = ask_schedule_execution()

    if isinstance(modo, dict) and modo.get("modo") == "diario":
        while True:
            esperar_proxima_execucao(modo["hora"])
            start_time = datetime.now()

            send_simple_email(
                from_email=FROM_EMAIL,
                to_emails=DESTINATARIOS,
                subject="☑️ Execução agendada INICIADA",
                body=f"Execução agendada iniciada em {start_time.strftime('%d/%m/%Y %H:%M:%S')}.",
                app_password=APP_PASSWORD
            )

            try:
                executar_processo(logins, spreadsheet)
                end_time = datetime.now()
                duration = end_time - start_time

                send_simple_email(
                    from_email=FROM_EMAIL,
                    to_emails=DESTINATARIOS,
                    subject="✅ Execução agendada FINALIZADA",
                    body=(
                        f"Execução finalizada com sucesso.\n\n"
                        f"☑️ Início: {start_time.strftime('%d/%m/%Y %H:%M:%S')}\n"
                        f"✅ Fim: {end_time.strftime('%d/%m/%Y %H:%M:%S')}\n"
                        f"⏱️ Duração: {duration}"
                    ),
                    app_password=APP_PASSWORD
                )

            except Exception as e:
                end_time = datetime.now()
                duration = end_time - start_time

                send_simple_email(
                    from_email=FROM_EMAIL,
                    to_emails=DESTINATARIOS,
                    subject="❌ Erro na execução agendada",
                    body=(
                        f"O script encontrou um erro.\n\n"
                        f"🕒 Início: {start_time.strftime('%d/%m/%Y %H:%M:%S')}\n"
                        f"🕔 Fim: {end_time.strftime('%d/%m/%Y %H:%M:%S')}\n"
                        f"⏱️ Duração: {duration}\n\n"
                        f"Erro: {str(e)}"
                    ),
                    app_password=APP_PASSWORD
                )
                raise

    else:
        start_time = datetime.now()

        send_simple_email(
            from_email=FROM_EMAIL,
            to_emails=DESTINATARIOS,
            subject="🟢 Execução manual INICIADA",
            body=f"Execução manual iniciada em {start_time.strftime('%d/%m/%Y %H:%M:%S')}.",
            app_password=APP_PASSWORD
        )

        try:
            executar_processo(logins, spreadsheet)
            end_time = datetime.now()
            duration = end_time - start_time

            send_simple_email(
                from_email=FROM_EMAIL,
                to_emails=DESTINATARIOS,
                subject="✅ Execução manual FINALIZADA",
                body=(
                    f"Execução finalizada com sucesso.\n\n"
                    f"🕒 Início: {start_time.strftime('%d/%m/%Y %H:%M:%S')}\n"
                    f"🕔 Fim: {end_time.strftime('%d/%m/%Y %H:%M:%S')}\n"
                    f"⏱️ Duração: {duration}"
                ),
                app_password=APP_PASSWORD
            )

        except Exception as e:
            end_time = datetime.now()
            duration = end_time - start_time

            send_simple_email(
                from_email=FROM_EMAIL,
                to_emails=DESTINATARIOS,
                subject="❌ Erro na execução manual",
                body=(
                    f"O script encontrou um erro.\n\n"
                    f"🕒 Início: {start_time.strftime('%d/%m/%Y %H:%M:%S')}\n"
                    f"🕔 Fim: {end_time.strftime('%d/%m/%Y %H:%M:%S')}\n"
                    f"⏱️ Duração: {duration}\n\n"
                    f"Erro: {str(e)}"
                ),
                app_password=APP_PASSWORD
            )
            raise


if __name__ == "__main__":
    main()
