# data_utils.py

import re
import sys
import select
import logging
from datetime import datetime
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def get_with_retry(url, headers=None, timeout=30, max_retries=3, params=None):
    session = requests.Session()
    retry = Retry(
        total=max_retries,
        backoff_factor=5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    try:
        response = session.get(url, headers=headers, timeout=timeout, params=params)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"🚨 Erro mesmo após {max_retries} tentativas: {e}")
        return None


def format_date(date_string):
    logging.debug(f"Recebendo data: {date_string}")
    if not date_string or date_string == "-":
        return "-"
    try:
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"):
            try:
                date_obj = datetime.strptime(date_string, fmt)
                return date_obj.strftime("%d/%m/%Y")
            except ValueError:
                continue
        logging.warning(f"Formato de data não reconhecido: {date_string}")
        return date_string
    except Exception as e:
        logging.error(f"Erro ao formatar data: {e}")
        return date_string

def parse_option_list(option_string):
    if isinstance(option_string, str) and option_string.startswith("OptionList;"):
        parts = option_string.split(";")
        try:
            index = int(parts[1])
            options = parts[2:]
            if index == 0 or index >= len(options) + 1:
                return "-"
            return options[index - 1]
        except (ValueError, IndexError):
            return "-"
    return option_string or "-"

def input_with_timeout(prompt, timeout=15, default="sim"):
    print(f"{prompt} (pressione Enter em até {timeout}s para responder, ou '{default}' será assumido): ", end='', flush=True)
    ready, _, _ = select.select([sys.stdin], [], [], timeout)
    if ready:
        return sys.stdin.readline().strip()
    else:
        print(f"\n⏰ Tempo esgotado. Assumindo '{default}'.")
        return default

def clean_illegal_chars(value):
    if isinstance(value, str):
        return re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F]', '', value)
    return value

def clean_url_params(url):
    """Remove parâmetros duplicados da URL, mantendo apenas o último valor de cada chave."""
    parsed = urlparse(url)
    query = parse_qsl(parsed.query, keep_blank_values=True)
    seen = {}
    for key, value in query:
        seen[key] = value
    new_query = urlencode(seen, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
