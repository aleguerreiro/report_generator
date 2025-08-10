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

def gerar_report_sla(raw_orders, config_id, workflow_cache, cutoff_by_order=None):
    """
    Gera o SLA:
      - aplica watermark por ordem (cutoff_by_order): só emite eventos > último processado
      - colapsa repetições consecutivas (A,A,B,A,B -> A,B,A,B)
      - mantém fuso America/Sao_Paulo
      - adiciona colunas granulares: 'Código do Status' e 'Data do Evento'
    """
    cutoff_by_order = cutoff_by_order or {}
    linhas = []

    sla_config = {}
    if config_id in workflow_cache:
        sla_config = parse_sla_config(workflow_cache[config_id])
        import json
        print(f"\n🔎 SLA extraído para config {config_id}: {json.dumps(sla_config, indent=2, default=str)}")

    for order_json in raw_orders or []:
        order_id = order_json.get("id", "")
        status_history = order_json.get("status_history", []) or []
        if not status_history:
            continue

        # ---- 1) Ordena por tempo e identifica último status <= cutoff
        def _ev_time(ev):
            # teu JSON usa 'time_created'
            return parse_datetime_safe(ev.get("time_created"))

        # ordena por data crescente
        status_history = sorted(status_history, key=_ev_time)

        # determina último status conhecido no cutoff (se houver)
        last_code_before_cutoff = None
        last_dt_processed = cutoff_by_order.get(str(order_id))
        if last_dt_processed is not None:
            # normaliza cutoff para TZ (se veio naive)
            if last_dt_processed.tzinfo is None:
                last_dt_processed = TZ.localize(last_dt_processed)
            for ev in status_history:
                t = _ev_time(ev)
                if t is None:
                    continue
                if t <= last_dt_processed:
                    st = ev.get("status", {}) or {}
                    last_code_before_cutoff = st.get("code")
                else:
                    break

        # ---- 2) Varre e COLAPSA repetições consecutivas; aplica cutoff
        etapas = []
        last_code_emitted = last_code_before_cutoff  # importante p/ colapso cruzando o cutoff

        for entry in status_history:
            status = entry.get("status", {}) or {}
            code = status.get("code")
            title = status.get("status")  # mantém teu campo
            time_created = _ev_time(entry)
            user = entry.get("event_data", {}).get("user", "-")

            if not code or not time_created:
                continue

            # aplica cutoff: só eventos estritamente DEPOIS do último processado
            if last_dt_processed is not None and time_created <= last_dt_processed:
                # atualiza o colapso mesmo assim, pra evitar repetir o primeiro pós-cutoff
                last_code_emitted = code
                continue

            # colapso: pula se mesmo status consecutivo
            if code == last_code_emitted:
                continue

            # --- SLA config (mantendo tua lógica atual)
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
                "Código do Status": code,  # <<< coluna granular p/ accumulator
                "Modificado por": user,
                "SLA (segundos)": sla_segundos,
                "Prazo Máximo": prazo_maximo,
                "Data Início Execução": time_created,
                "Data do Evento": time_created.strftime("%Y-%m-%d %H:%M:%S"),  # <<< granular
                "SLA config": sla_info
            })
            last_code_emitted = code

        # ---- 3) Linhas calculadas com base nas transições filtradas
        for idx, etapa in enumerate(etapas):
            inicio = etapa["Data Início Execução"]
            fim = etapas[idx + 1]["Data Início Execução"] if idx + 1 < len(etapas) else None
            sla = etapa.get("SLA (segundos)")
            prazo_maximo = etapa.get("Prazo Máximo")
            sla_info = etapa.get("SLA config", {})

            duracao_total = (fim - inicio).total_seconds() if isinstance(fim, datetime) else None
            duracao_util = calculate_working_time(inicio, fim, sla_info) if isinstance(fim, datetime) and sla_info else None

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
                "Código do Status": etapa["Código do Status"],     # <<< mantém
                "Modificado por": etapa["Modificado por"],
                "SLA": format_duration(sla),
                "Data do Evento": etapa["Data do Evento"],         # <<< mantém
                "Data Início Execução": inicio.strftime("%d/%m/%Y %H:%M") if isinstance(inicio, datetime) else "-",
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
            "Código do Status": "-",
            "Modificado por": "-",
            "SLA": "-",
            "Data do Evento": "-",
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
