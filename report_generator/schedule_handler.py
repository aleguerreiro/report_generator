# schedule_handler.py

import time
from datetime import datetime, timedelta

def ask_schedule_execution():
    """
    Pergunta ao usuário o modo de execução desejado:
    - agora
    - agendado
    - execução diária fixa

    Returns:
        str ou dict: modo de execução ou dicionário com hora diária.
    """
    option = input(
        "Deseja:\n"
        "1. Executar agora\n"
        "2. Agendar execução\n"
        "3. Executar automaticamente todos os dias em um horário fixo\n"
        "Escolha (1, 2 ou 3): "
    ).strip()

    if option == "2":
        now = datetime.now()
        hora_str = input("Digite a hora para executar o script (ex: 14:30): ").strip()
        try:
            scheduled_time = datetime.strptime(hora_str, "%H:%M").replace(
                year=now.year, month=now.month, day=now.day
            )
            if scheduled_time < now:
                scheduled_time += timedelta(days=1)
            wait_seconds = (scheduled_time - now).total_seconds()
            print(f"Aguardando até {scheduled_time.strftime('%H:%M')} para iniciar...")
            time.sleep(wait_seconds)
            return "agendado"
        except ValueError:
            print("⛔ Formato inválido. Executando agora.")
            return "agora"

    elif option == "3":
        hora_str = input("Digite a hora fixa diária (ex: 03:30): ").strip()
        try:
            exec_time = datetime.strptime(hora_str, "%H:%M").time()
            return {"modo": "diario", "hora": exec_time}
        except ValueError:
            print("⛔ Formato inválido. Usando 00:00 como padrão.")
            return {"modo": "diario", "hora": datetime.strptime("00:00", "%H:%M").time()}

    else:
        return "agora"

def esperar_proxima_execucao(hora_execucao):
    """
    Aguarda até a próxima hora de execução diária.

    Args:
        hora_execucao (datetime.time): hora alvo.
    """
    agora = datetime.now()
    proxima_execucao = agora.replace(hour=hora_execucao.hour, minute=hora_execucao.minute,
                                     second=0, microsecond=0)
    if agora >= proxima_execucao:
        proxima_execucao += timedelta(days=1)
    segundos = (proxima_execucao - agora).total_seconds()
    print(f"⏳ Aguardando até {proxima_execucao.strftime('%H:%M')}... ({int(segundos)}s)")
    time.sleep(segundos)
