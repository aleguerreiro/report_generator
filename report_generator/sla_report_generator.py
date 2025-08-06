from datetime import datetime
import pandas as pd
import pytz
from .utils.sla_utils import parse_sla_config, calculate_sla_deadline, calculate_working_time

# Fuso horário padrão do Brasil
TZ = pytz.timezone("America/Sao_Paulo")

def parse_datetime_safe(dt_str):
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.astimezone(TZ)
    except Exception:
        return None

def format_duration(seconds):
    if seconds is None:
        return "-"
    minutes, sec = divmod(int(seconds), 60)
    hours, min_ = divmod(minutes, 60)
    return f"{hours:02d}:{min_:02d} Horas"

def format_days(seconds):
    if seconds is None:
        return "-"
    return f"{round(seconds / 86400, 2)} dias"

def gerar_report_sla(raw_orders, config_id, workflow_cache):
    linhas = []

    sla_config = {}
    if config_id in workflow_cache:
        sla_config = parse_sla_config(workflow_cache[config_id])
        import json
        print(f"\n🔎 SLA extraído para config {config_id}: {json.dumps(sla_config, indent=2, default=str)}")

    for order_json in raw_orders:
        order_id = order_json.get("id", "")
        status_history = order_json.get("status_history", [])
        if not status_history:
            continue

        etapas = []
        last_code = None

        for entry in status_history:
            status = entry.get("status", {})
            code = status.get("code")
            title = status.get("status")
            time_created = parse_datetime_safe(entry.get("time_created"))
            user = entry.get("event_data", {}).get("user", "-")

            print(f"📌 Status encontrado na ordem {order_id}: {code} ({title})")

            if not code or not time_created or code == last_code:
                continue

            sla_info = sla_config.get(str(code))
            sla_segundos = None
            prazo_maximo = None

            if sla_info:
                try:
                    if "sla_time" in sla_info and sla_info["sla_time"]:
                        h, m = map(int, sla_info["sla_time"].split(":"))
                        sla_segundos = h * 3600 + m * 60
                        prazo_maximo = calculate_sla_deadline(time_created, sla_info)
                    else:
                        print(f"⚠️ SLA sem sla_time para status {code} na ordem {order_id}: {sla_info}")
                except Exception as e:
                    print(f"❌ Erro ao calcular SLA para status {code} na ordem {order_id}: {e}")
                    sla_segundos = None
                    prazo_maximo = None

            etapas.append({
                "Etapa": title,
                "Código": code,
                "Modificado por": user,
                "SLA (segundos)": sla_segundos,
                "Prazo Máximo": prazo_maximo,
                "Data Início Execução": time_created,
                "SLA config": sla_info
            })
            last_code = code

        for idx, etapa in enumerate(etapas):
            inicio = etapa["Data Início Execução"]
            fim = etapas[idx + 1]["Data Início Execução"] if idx + 1 < len(etapas) else None
            sla = etapa.get("SLA (segundos)")
            prazo_maximo = etapa.get("Prazo Máximo")
            sla_info = etapa.get("SLA config", {})

            duracao_total = (fim - inicio).total_seconds() if fim else None
            duracao_util = calculate_working_time(inicio, fim, sla_info) if fim and sla_info else None

            status_sla = "-"
            excedido_total = "-"
            excedido_util = "-"
            if isinstance(fim, datetime) and isinstance(prazo_maximo, datetime):
                try:
                    if fim <= prazo_maximo:
                        status_sla = "Dentro do SLA"
                        excedido_total = "0"
                        excedido_util = "0"
                    else:
                        status_sla = "Atrasado"
                        excedido_total = round((fim - prazo_maximo).total_seconds() / 60, 2)
                        excedido_util = round(calculate_working_time(prazo_maximo, fim, sla_info) / 60, 2)
                except Exception as e:
                    print(f"❌ Erro comparando SLA em ordem {order_id}: {e}")

            linhas.append({
                "ID da Ordem": order_id,
                "Etapa": etapa["Etapa"],
                "Modificado por": etapa["Modificado por"],
                "SLA": format_duration(sla),
                "Data Início Execução": inicio.strftime("%d/%m/%Y %H:%M") if inicio else "-",
                "Data Final Execução": fim.strftime("%d/%m/%Y %H:%M") if isinstance(fim, datetime) else "-",
                "Prazo Máximo": prazo_maximo.strftime("%d/%m/%Y %H:%M") if isinstance(prazo_maximo, datetime) else "-",
                "Duração Total": format_days(duracao_total),
                "Duração Útil": format_days(duracao_util),
                "Status SLA": status_sla,
                "Excedido Total (min)": excedido_total,
                "Excedido Útil (min)": excedido_util
            })

    if not linhas:
        linhas.append({
            "ID da Ordem": "Nenhuma ordem com histórico válido",
            "Etapa": "-",
            "Modificado por": "-",
            "SLA": "-",
            "Data Início Execução": "-",
            "Data Final Execução": "-",
            "Prazo Máximo": "-",
            "Duração Total": "-",
            "Duração Útil": "-",
            "Status SLA": "-",
            "Excedido Total (min)": "-",
            "Excedido Útil (min)": "-"
        })

    return pd.DataFrame(linhas)
