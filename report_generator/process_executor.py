import os
import json
import logging
from datetime import datetime, timezone
import pandas as pd
from tqdm import tqdm

from .zapform_api_client import (
    fetch_orders_by_date,
    fetch_all_orders,
    fetch_order_data,
    get_config_name
)
from .label_fetcher import fetch_labels_from_workflow
from .sheet_config_reader import (
    read_config_sheet,
    build_filters_from_sheet,
    extract_default_fields,
    extract_variable_fields,
    extract_email_list,
    extract_header_report_map,
    aplicar_filtro_incremental
)
from .email_sender import send_email_with_attachment
from .dashboard_executor import executar_dashboard_personalizado
from .data_utils import clean_illegal_chars, get_with_retry
from .zapform_auth import TokenManager
from .extractor import extract_data
from .sla_report_generator import gerar_report_sla  # ✅ novo
from .utils.sla_utils import fetch_sla_by_config  # ✅ para carregar SLAs por config_id


def executar_processo(logins, spreadsheet):
    from_email, app_password = _carregar_credenciais_email()
    token_manager = TokenManager(logins)
    workflow_cache = {}  # ✅ Agora está no lugar certo (dentro da função)


    for ws in spreadsheet.worksheets():
        if not ws.title.startswith("config"):
            continue

        config_id = ws.title.replace("config", "").strip()
        print(f"\n🔁 Processando aba: {ws.title} (config {config_id})")

        df = read_config_sheet(ws)
        if df is None or "orders" not in df.columns or not df["orders"].iloc[0].strip():
            print(f"⏭️ Pulando aba {ws.title}, 'orders' está vazio.")
            continue

        filtros = build_filters_from_sheet(df)
        filtros = aplicar_filtro_incremental(config_id, filtros)
        status_raw = filtros.get("status", "")
        status_list = [s.strip() for s in status_raw.split(",") if s.strip()] if status_raw else []

        usar_todas = df["orders"].iloc[0].strip().lower() == "all"

        # Campos e emails
        campos_padroes = extract_default_fields(df)
        campos_variaveis = extract_variable_fields(df)
        header_report_dict = extract_header_report_map(df)
        emails = extract_email_list(df)

        etiquetas_dict = fetch_labels_from_workflow(
            config_id,
            token_manager.get_token(),
            get_with_retry=get_with_retry
        )

        sla_config_dict = fetch_sla_by_config(
            config_id,
            token_manager.get_token(),
            get_with_retry=get_with_retry
        )
        if config_id not in workflow_cache:
            url = f"https://api.zapform.com.br/api/v2/workflow/{config_id}/"
            headers = {
                "accept": "application/json",
                "Authorization": f"Token {token_manager.get_token()}"
            }
            response = get_with_retry(url, headers=headers)
            if response and response.status_code == 200:
                workflow_cache[config_id] = response.json()
            else:
                workflow_cache[config_id] = {}  # fallback para evitar erro

        # Buscar ordens
        order_ids = []
        if usar_todas:
            print(f"🔍 Iniciando busca de ordens para config {config_id}...")
            logging.info(f"🔍 Iniciando busca de ordens para config {config_id}...")

            if status_list:
                for status in status_list:
                    f = {**filtros, "status": status}
                    ids = fetch_orders_by_date(
                        token_manager.get_token(),
                        config_id,
                        f,
                        get_with_retry=get_with_retry
                    )
                    order_ids.extend(ids)
            elif filtros:
                grupo_filtro = {}
                for prefixo in ["time_created", "time_last_updated", "time_status"]:
                    grupo = {k: v for k, v in filtros.items() if k.startswith(prefixo)}
                    if grupo:
                        grupo_filtro = grupo
                        break

                if not grupo_filtro:
                    print(f"⚠️ Nenhum filtro de data encontrado. Pulando config {config_id}.")
                    continue

                print(f"🔎 Buscando ordens com filtro: {grupo_filtro}")
                logging.info(f"🔎 Buscando ordens com filtro: {grupo_filtro}")
                order_ids = fetch_all_orders(
                    token_manager,
                    config_id,
                    grupo_filtro,
                    get_with_retry=get_with_retry
                )
                logging.info(f"✅ {len(order_ids)} ordens encontradas para a config {config_id}")
            else:
                print(f"🔎 Buscando todas as ordens da config {config_id} sem filtros")
                logging.info(f"🔎 Buscando todas as ordens da config {config_id} sem filtros")
                order_ids = fetch_all_orders(
                    token_manager,
                    config_id,
                    {},
                    get_with_retry=get_with_retry
                )
                logging.info(f"✅ {len(order_ids)} ordens encontradas para a config {config_id}")

            order_ids = list(set(order_ids))
        else:
            order_ids = [
                oid.strip()
                for oid in df["orders"].dropna().astype(str).tolist()
                if oid.strip().lower() != "all" and oid.strip() != ""
            ]

        # Buscar dados das ordens
        results = []
        raw_orders = []  # ✅ para o report SLA
        for order_id in tqdm(order_ids, desc=f"Config {config_id}"):
            order_data = fetch_order_data(
                config_id,
                order_id,
                token_manager,
                get_with_retry=get_with_retry
            )
            if order_data:
                raw_orders.append(order_data)  # ✅ coleta para o SLA
                if status_list:
                    code = order_data.get("status", {}).get("code", "").strip()
                    if code not in status_list:
                        continue
                data = extract_data(order_data, campos_variaveis, etiquetas_dict, header_report_dict, campos_padroes)
                results.append(pd.Series(data))

        df_result = pd.DataFrame(results)
        df_sla = gerar_report_sla(raw_orders, config_id, workflow_cache)


        config_name = get_config_name(config_id, token_manager.get_token())
        safe_name = "".join(c if c.isalnum() or c in "._-" else "_" for c in config_name)[:40]
        current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M")
        file_path = f"report_{safe_name}_{current_datetime}.xlsx"
        csv_acumulado_path = f"acumulado_config_{config_id}.csv"

        # Acumular e deduplicar
        if os.path.exists(csv_acumulado_path):
            try:
                df_antigo = pd.read_csv(csv_acumulado_path)
                df_final = pd.concat([df_antigo, df_result], ignore_index=True)
                df_final.drop_duplicates(subset="ID do Card", keep="last", inplace=True)
            except Exception as e:
                logging.warning(f"⚠️ Erro ao ler CSV antigo: {e}")
                df_final = df_result.copy()
        else:
            df_final = df_result.copy()

        # Limpeza de caracteres apenas em colunas de texto (evita deprecated warning)
        for col in df_final.select_dtypes(include="object").columns:
            df_final[col] = df_final[col].map(clean_illegal_chars)

        for col in df_sla.select_dtypes(include="object").columns:
            df_sla[col] = df_sla[col].map(clean_illegal_chars)


        # Executa dashboard
        executar_dashboard_personalizado(config_id, df_final, file_path)

        # Salva acumulado
        df_final.to_csv(csv_acumulado_path, index=False)

        # Salva Excel com duas abas
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            df_final.to_excel(writer, sheet_name="report", index=False)
            df_sla.to_excel(writer, sheet_name="report_SLA", index=False)

        # E-mail
        subject, body = _montar_email(config_name, config_id, current_datetime)
        to_email = ", ".join(emails)
        send_email_with_attachment(from_email, to_email, subject, body, app_password, file_path)

        # Salva data de última execução
        with open(f"last_run_config_{config_id}.json", "w") as f:
            json.dump({"last_updated": datetime.now(timezone.utc).isoformat()}, f)
            logging.info(f"📁 Última execução salva para config {config_id}")


def _carregar_credenciais_email():
    with open("email_credentials.json") as f:
        config = json.load(f)
    return config["email"], config["senha_app"]


def _montar_email(config_name, config_id, current_datetime):
    date_part, time_part = current_datetime.split("_")
    date_br = datetime.strptime(date_part, "%Y-%m-%d").strftime("%d/%m/%Y")
    time_hm = time_part.replace("-", ":")
    subject = f"{config_name} Report de {date_br} {time_hm}"
    body = (
        f"Olá!\n\n"
        f"Segue em anexo o relatório gerado automaticamente para a configuração {config_id} no dia {current_datetime}.\n\n"
        f"Atenciosamente,\nEquipe Zapform 😉"
    )
    return subject, body
