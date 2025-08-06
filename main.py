# main.py

import time
import logging
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from report_generator.schedule_handler import ask_schedule_execution, esperar_proxima_execucao
from report_generator.process_executor import executar_processo

def main():
    # ⚙️ Configuração de logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # 🔐 Logins da API Zapform
    logins = [
        {"username": "integracao-ale4", "password": "Zapform@04"},
        {"username": "integracao-ale5", "password": "Zapform@05"},
        {"username": "integracao-ale6", "password": "Zapform@06"},
        {"username": "integracao-ale7", "password": "Zapform@07"},
        {"username": "integracao-ale8", "password": "Zapform@08"},
        {"username": "integracao-ale9", "password": "Zapform@09"},
        {"username": "integracao-ale10", "password": "Zapform@10"},
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
            executar_processo(logins, spreadsheet)
    else:
        executar_processo(logins, spreadsheet)

if __name__ == "__main__":
    main()
