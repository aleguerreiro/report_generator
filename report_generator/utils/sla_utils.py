from datetime import datetime, timedelta, time
import logging
import requests
import pytz

def parse_iso_datetime(dt_str):
    """Converte uma string ISO para datetime com timezone."""
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))

def parse_sla_config(workflow_data):
    """
    Extrai e organiza a configuração de SLA a partir dos dados do workflow (config).
    Inclui proteção contra ausência de dados ou estruturas inesperadas.
    """
    sla_config = {}

    try:
        sla_list = workflow_data.get("extra_data", {}).get("sla", [])
        for item in sla_list:
            status_id = item.get("statusId")
            sla_time = item.get("slaTime", "00:00")
            sla_type = item.get("slaType", "hours")

            # Dias ativos e turnos
            active_days = {}
            for entry in item.get("workWeek", []):
                if entry.get("isActive"):
                    day_index = _map_day_of_week(entry.get("dayOfWeek", ""))
                    if day_index >= 0:
                        active_days[day_index] = [
                            {
                                "start": shift.get("startTime", "00:00"),
                                "end": shift.get("endTime", "00:00")
                            }
                            for shift in entry.get("shifts", [])
                            if "startTime" in shift and "endTime" in shift
                        ]

            sla_config[str(status_id)] = {
                "sla_type": sla_type,
                "sla_time": sla_time,
                "active_days": active_days,
                "holidays": set(item.get("holidays", []))
            }
    except Exception as e:
        logging.error(f"❌ Erro ao interpretar SLA config: {e}")

    return sla_config


def _map_day_of_week(day_name):
    """Mapeia o nome do dia da semana para índice (0=segunda)."""
    mapping = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
        "saturday": 5,
        "sunday": 6,
    }
    return mapping.get(day_name.lower(), -1)

def calculate_sla_deadline(start_time, sla_info):
    """
    Calcula o prazo SLA considerando dias úteis, turnos e feriados, com timezone UTC-3 aplicado.
    """
    import pytz
    tz = pytz.timezone("America/Sao_Paulo")
    if start_time.tzinfo is None:
        start_time = tz.localize(start_time)
    else:
        start_time = start_time.astimezone(tz)

    sla_time = sla_info.get("sla_time", "00:00")
    hours, minutes = map(int, sla_time.split(":"))
    total_minutes = hours * 60 + minutes

    active_days = sla_info.get("active_days", {})
    holidays = sla_info.get("holidays", set())

    current = start_time
    remaining_minutes = total_minutes

    while remaining_minutes > 0:
        weekday = current.weekday()
        shifts = active_days.get(weekday, [])

        if not shifts or current.strftime("%Y-%m-%d") in holidays:
            current += timedelta(days=1)
            current = tz.localize(datetime.combine(current.date(), time.min))
            continue

        for shift in shifts:
            shift_start = tz.localize(datetime.combine(current.date(), time.fromisoformat(shift["start"])))
            shift_end = tz.localize(datetime.combine(current.date(), time.fromisoformat(shift["end"])))

            if current > shift_end:
                continue

            effective_start = max(current, shift_start)
            shift_minutes = int((shift_end - effective_start).total_seconds() / 60)

            if remaining_minutes <= shift_minutes:
                return effective_start + timedelta(minutes=remaining_minutes)

            remaining_minutes -= shift_minutes

        current += timedelta(days=1)
        current = tz.localize(datetime.combine(current.date(), time.min))

    return current


def fetch_sla_by_config(config_id, token, get_with_retry):
    """
    Busca na API os dados de SLA definidos para o workflow (config_id).
    Retorna um dicionário com statusId como chave e dados SLA como valor.
    """
    url = f"https://api.zapform.com.br/api/v2/workflow/{config_id}/"
    headers = {
        "accept": "application/json",
        "Authorization": f"Token {token}"
    }

    try:
        response = get_with_retry(url, headers=headers)
        if response and response.status_code == 200:
            workflow_data = response.json()
            return parse_sla_config(workflow_data)
        else:
            logging.warning(f"⚠️ Não foi possível buscar SLA da config {config_id}. Status: {response.status_code}")
            return {}
    except Exception as e:
        logging.error(f"❌ Erro ao buscar SLA da config {config_id}: {e}")
        return {}
    
def calcular_dias_uteis(start, end, sla_info):
    """
    Calcula o número de dias úteis (com base nos turnos e feriados) entre duas datas.
    """
    active_days = sla_info.get("active_days", {})
    holidays = sla_info.get("holidays", set())
    current = start
    total_minutes = 0

    while current < end:
        weekday = current.weekday()
        shifts = active_days.get(weekday, [])

        if not shifts or current.strftime("%Y-%m-%d") in holidays:
            current += timedelta(days=1)
            current = datetime.combine(current.date(), time.min, current.tzinfo)
            continue

        for shift in shifts:
            shift_start = datetime.combine(current.date(), time.fromisoformat(shift["start"]))
            shift_end = datetime.combine(current.date(), time.fromisoformat(shift["end"]))
            effective_start = max(current, shift_start)
            effective_end = min(end, shift_end)
            if effective_end > effective_start:
                total_minutes += int((effective_end - effective_start).total_seconds() / 60)

        current += timedelta(days=1)
        current = datetime.combine(current.date(), time.min, current.tzinfo)

    return round(total_minutes / 1440, 2)  # dias úteis (em minutos / 1440)
def calculate_working_time(start, end, sla_info):
    """
    Calcula o tempo útil (em segundos) entre duas datas, com base nos turnos e feriados.
    """
    if start >= end:
        return 0

    tz = pytz.timezone("America/Sao_Paulo")
    if start.tzinfo is None:
        start = tz.localize(start)
    if end.tzinfo is None:
        end = tz.localize(end)

    active_days = sla_info.get("active_days", {})
    holidays = sla_info.get("holidays", set())
    current = start
    total_seconds = 0

    while current < end:
        weekday = current.weekday()
        shifts = active_days.get(weekday, [])

        if not shifts or current.strftime("%Y-%m-%d") in holidays:
            current = tz.localize(datetime.combine((current + timedelta(days=1)).date(), time.min))
            continue

        for shift in shifts:
            shift_start = tz.localize(datetime.combine(current.date(), time.fromisoformat(shift["start"])))
            shift_end = tz.localize(datetime.combine(current.date(), time.fromisoformat(shift["end"])))

            effective_start = max(current, shift_start)
            effective_end = min(end, shift_end)

            if effective_end > effective_start:
                total_seconds += (effective_end - effective_start).total_seconds()

        current = tz.localize(datetime.combine((current + timedelta(days=1)).date(), time.min))

    return int(total_seconds)
