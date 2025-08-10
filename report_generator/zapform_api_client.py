# zapform_api_client.py

import time
import logging
import requests
from .data_utils import clean_url_params, input_with_timeout

def get_config_name(config_id, token):
    url = f"https://api.zapform.com.br/api/zc/config/{config_id}/"
    headers = {
        "accept": "application/json",
        "Authorization": f"Token {token}"
    }
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            return response.json().get("name", f"config_{config_id}")
        else:
            logging.warning(f"⚠️ Não foi possível obter o nome da config {config_id}. Usando ID no nome do arquivo.")
            return f"config_{config_id}"
    except Exception as e:
        logging.error(f"❌ Erro ao obter nome da config {config_id}: {e}")
        return f"config_{config_id}"

def fetch_orders_by_date(token, config_id, filters, get_with_retry):
    base_url = f"https://api.zapform.com.br/api/zc/{config_id}/order/"
    params = {
        k: f"{v}T00:00:00.000Z" if k.endswith("__gt") else
           f"{v}T23:59:59.999Z" if k.endswith("__lte") or k.endswith("__lt") else
           v
        for k, v in filters.items()
    }

    headers = {
        "accept": "application/json",
        "Authorization": f"Token {token}"
    }

    order_ids = []
    max_attempts = 30
    delay = 2
    url = base_url

    while url:
        for attempt in range(max_attempts):
            try:
                response = get_with_retry(url, headers=headers, timeout=30, params=params)
                if response.status_code == 200:
                    data = response.json()
                    orders = data.get('results', [])
                    order_ids.extend([o['id'] for o in orders])
                    url = clean_url_params(data.get('next')) if data.get('next') else None
                    logging.info(f"📥 Página processada. Total até agora: {len(order_ids)} ordens.")
                    break
                elif response.status_code in [500, 502, 503, 504]:
                    logging.warning(f"⚠️ Erro {response.status_code} - Tentativa {attempt + 1}/{max_attempts}")
                    time.sleep(delay)
                else:
                    logging.error(f"❌ Erro inesperado: {response.status_code} - {response.text}")
                    return order_ids
            except requests.exceptions.RequestException as e:
                logging.error(f"🌐 Erro de rede: {e}")
                time.sleep(delay)
        else:
            logging.error("⛔ Máximo de tentativas atingido. Abortando.")
            break

    return order_ids

def fetch_all_orders(token_manager, config_id, filters, get_with_retry):
    url = f"https://api.zapform.com.br/api/zc/{config_id}/order/"
    headers = {
        "accept": "application/json",
        "Authorization": f"Token {token_manager.get_token()}"
    }

    print(f"🟡 Iniciando busca inicial para config {config_id} com filtros: {filters}")
    response = get_with_retry(url, headers=headers, params=filters)
    
    if response is None:
        print("❌ Resposta inicial NULA.")
        return []

    print(f"🔵 Resposta inicial: status {response.status_code}")
    if response.status_code != 200:
        print(f"❌ Erro ao obter resposta inicial. Status: {response.status_code}")
        return []

    total = response.json().get('count', 0)
    print(f"📦 Total de ordens esperadas: {total}")

    order_ids = []
    attempt = 0
    max_attempts = 5

    data = response.json()
    page_results = data.get('results', [])
    print(f"📄 Primeira página: {len(page_results)} ordens")
    order_ids.extend([o['id'] for o in page_results])
    next_url = data.get('next')
    print(f"➡️ Próxima URL: {next_url}")

    while next_url and attempt < max_attempts:
        print(f"🔁 Paginação: tentando buscar {next_url}")
        try:
            response = get_with_retry(next_url, headers=headers)
            if response is None:
                print("⚠️ Resposta NULA em próxima página. Trocando token...")
                token_manager.rotate_login_on_error()
                headers["Authorization"] = f"Token {token_manager.get_token()}"
                attempt += 1
                time.sleep(2)
                continue

            print(f"✅ Status próxima página: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                page_results = data.get('results', [])
                print(f"📄 Página com {len(page_results)} ordens")
                order_ids.extend([o['id'] for o in page_results])
                next_url = data.get('next')
                print(f"➡️ Nova next: {next_url}")
                attempt = 0  # reset após sucesso
            else:
                print(f"⚠️ HTTP {response.status_code}. Tentando novamente...")
                attempt += 1
        except Exception as e:
            logging.error(f"❌ Erro inesperado durante paginação: {e}")
            attempt += 1
            time.sleep(2)

    print(f"✅ Total recuperado: {len(order_ids)} (esperado {total})")

    return order_ids



def fetch_order_data(config_id, order_id, token_manager, get_with_retry):
    url = f"https://api.zapform.com.br/api/zc/{config_id}/order/{order_id}/"
    headers = {
        "accept": "application/json",
        "Authorization": f"Token {token_manager.get_token()}"
    }

    for attempt in range(5):
        try:
            response = get_with_retry(url, headers=headers, timeout=30)
            if response and response.status_code == 200:
                token_manager.marcar_sucesso()
                return response.json()
            elif response and response.status_code in [429, 502, 503, 504, 403]:
                token_manager.rotate_login_on_error()
                headers["Authorization"] = f"Token {token_manager.get_token()}"
        except requests.exceptions.RequestException as e:
            logging.error(f"🌐 Erro de rede: {e}")
            token_manager.rotate_login_on_error()
            headers["Authorization"] = f"Token {token_manager.get_token()}"

        time.sleep(1)
    return None

def get_ultimo_usuario_humano(order_json):
    try:
        status_history = order_json.get("status_history", [])
        for s in sorted(status_history, key=lambda s: s.get("time_created", ""), reverse=True):
            user = s.get("event_data", {}).get("user", "")
            if user and not user.lower().startswith("integracao"):
                return user
    except Exception as e:
        return f"⚠️ Erro: {e}"
    return "-"

def get_data_ultima_alteracao_humana(order_json, format_date):
    try:
        status_history = order_json.get("status_history", [])
        for s in sorted(status_history, key=lambda s: s.get("time_created", ""), reverse=True):
            user = s.get("event_data", {}).get("user", "")
            if user and not user.lower().startswith("integracao"):
                return format_date(s.get("time_created", ""))
    except Exception as e:
        return f"⚠️ Erro: {e}"
    return "-"

def get_origem_ultima_acao_humana(order_json):
    try:
        status_history = order_json.get("status_history", [])
        for s in sorted(status_history, key=lambda s: s.get("time_created", ""), reverse=True):
            user = s.get("event_data", {}).get("user", "")
            if user and not user.lower().startswith("integracao"):
                return s.get("event_data", {}).get("source", "-")
    except Exception as e:
        return f"⚠️ Erro: {e}"
    return "-"
