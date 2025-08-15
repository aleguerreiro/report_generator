import os
import json
import logging
from datetime import datetime, timezone
import pandas as pd
from tqdm import tqdm
from .data_utils import clean_illegal_chars

from .accumulator import acumular_relatorio_principal, acumular_report_sla
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
from .data_utils import get_with_retry
from .zapform_auth import TokenManager
from .extractor import extract_data
from .sla_report_generator import gerar_report_sla
from .utils.sla_utils import fetch_sla_by_config
import time


def executar_processo(logins, spreadsheet):
    from_email, app_password = _carregar_credenciais_email()
    token_manager = TokenManager(logins)
    workflow_cache = {}

    for ws in spreadsheet.worksheets():
        if not ws.title.startswith("config"):
            continue

        start_config_time = time.time()
        config_id = ws.title.replace("config", "").strip()
        print(f"\n🔁 Processando aba: {ws.title} (config {config_id})")

        df = read_config_sheet(ws)
        if df is None or "orders" not in df.columns or not str(df["orders"].iloc[0]).strip():
            print(f"⏭️ Pulando aba {ws.title}, 'orders' está vazio.")
            continue

        filtros = build_filters_from_sheet(df)
        filtros = aplicar_filtro_incremental(config_id, filtros)

        status_raw = filtros.get("status", "")
        status_list = [s.strip() for s in status_raw.split(",") if s.strip()] if status_raw else []

        usar_todas = str(df["orders"].iloc[0]).strip().lower() == "all"

        campos_padroes = extract_default_fields(df)
        campos_variaveis = extract_variable_fields(df)
        header_report_dict = extract_header_report_map(df)
        emails = extract_email_list(df)

        etiquetas_dict = fetch_labels_from_workflow(
            config_id,
            token_manager.get_token(),
            get_with_retry=get_with_retry
        )
        logging.info(f"🎯 {len(etiquetas_dict)} etiquetas carregadas para config {config_id}")

        sla_config_dict = fetch_sla_by_config(
            config_id,
            token_manager.get_token(),
            get_with_retry=get_with_retry
        )
        logging.info(f"📜 SLA config carregado para {config_id}")

        # cache do workflow
        if config_id not in workflow_cache:
            url = f"https://api.zapform.com.br/api/v2/workflow/{config_id}/"
            headers = {
                "accept": "application/json",
                "Authorization": f"Token {token_manager.get_token()}"
            }
            response = get_with_retry(url, headers=headers)
            workflow_cache[config_id] = response.json() if response and response.status_code == 200 else {}

        # ⚙️ Metadados e caminhos
        current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M")
        config_name = workflow_cache.get(config_id, {}).get("name") or get_config_name(
            config_id, token_manager.get_token()
        )
        safe_name = "".join(c if c.isalnum() or c in "._-" else "_" for c in config_name)[:40]
        file_path = f"report_{safe_name}_{current_datetime}.xlsx"
        csv_acumulado_latest = f"acumulado_config_{config_id}_latest.csv"
        csv_acumulado_sla_latest = f"acumulado_sla_config_{config_id}_latest.csv"

        # ============================
        # 1) Buscar IDs incrementais
        # ============================
        order_ids = []
        start_fetch_ids = time.time()
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
                order_ids = fetch_all_orders(
                    token_manager,
                    config_id,
                    grupo_filtro,
                    get_with_retry=get_with_retry
                )
            else:
                print(f"🔎 Buscando todas as ordens da config {config_id} sem filtros")
                order_ids = fetch_all_orders(
                    token_manager,
                    config_id,
                    {},
                    get_with_retry=get_with_retry
                )

            order_ids = list(dict.fromkeys(order_ids))  # dedup preservando ordem
            logging.info(f"✅ {len(order_ids)} ordens encontradas para a config {config_id}")
        else:
            order_ids = [
                oid.strip()
                for oid in df["orders"].dropna().astype(str).tolist()
                if oid.strip().lower() != "all" and oid.strip() != ""
            ]
            order_ids = list(dict.fromkeys(order_ids))  # dedup

        logging.info(f"⏱️ Tempo para buscar IDs: {round(time.time() - start_fetch_ids, 2)}s")
        logging.info(f"🧾 {len(order_ids)} IDs para processar em config {config_id}")

        # short-circuit sem mudanças
        
        if not order_ids:
            logging.info(f"🔕 Sem mudanças para config {config_id}. Reutilizando acumulados _latest e pulando API.")
            df_final = pd.read_csv(csv_acumulado_latest) if os.path.exists(csv_acumulado_latest) else pd.DataFrame()
            df_sla_final = (
                pd.read_csv(csv_acumulado_sla_latest, dtype={"ID da Ordem": str, "Etapa": str})
                if os.path.exists(csv_acumulado_sla_latest) else pd.DataFrame()
            )

            executar_dashboard_personalizado(config_id, df_final, file_path)
            if not df_final.empty or not df_sla_final.empty:
                with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                    if not df_final.empty:
                        df_final.to_excel(writer, sheet_name="report", index=False)
                    if not df_sla_final.empty:
                        df_sla_final.to_excel(writer, sheet_name="report_SLA", index=False)
            else:
                logging.info(f"⏭️ Nenhum dado para gerar Excel na config {config_id}, pulando.")
                continue


            subject, body = _montar_email(config_name, config_id, current_datetime)
            to_email = ", ".join(emails)
            send_email_with_attachment(from_email, to_email, subject, body, app_password, file_path)

            with open(f"last_run_config_{config_id}.json", "w") as f:
                json.dump({"last_updated": datetime.now().isoformat()}, f)

            logging.info(f"🏁 Config {config_id} concluída em {round(time.time() - start_config_time, 2)}s (sem mudanças)")
            continue

        # ============================
        # 2) Busca detalhada SOMENTE dos IDs incrementais
        # ============================
        results = []
        raw_orders = []
        start_fetch_orders = time.time()
        for order_id in tqdm(order_ids, desc=f"Config {config_id}"):
            order_data = fetch_order_data(
                config_id,
                order_id,
                token_manager,
                get_with_retry=get_with_retry
            )
            if not order_data:
                continue

            # filtro de status (se solicitado)
            code = str(order_data.get("status", {}).get("code", "")).strip()
            if status_list and code not in status_list:
                continue

            raw_orders.append(order_data)
            data = extract_data(order_data, campos_variaveis, etiquetas_dict, header_report_dict, campos_padroes)
            results.append(pd.Series(data))

        logging.info(f"📦 {len(raw_orders)} ordens detalhadas buscadas")
        logging.info(f"⏱️ Tempo para buscar ordens: {round(time.time() - start_fetch_orders, 2)}s")

        df_result = pd.DataFrame(results)

        # ============================
        # 3) Watermark por ordem (SLA incremental por evento)
        # ============================
        cutoff_by_order = {}
        if os.path.exists(csv_acumulado_sla_latest):
            try:
                df_sla_antigo = pd.read_csv(csv_acumulado_sla_latest, dtype={"ID da Ordem": str})
                if "ID da Ordem" in df_sla_antigo.columns and "Data do Evento" in df_sla_antigo.columns:
                    df_sla_antigo["ID da Ordem"] = df_sla_antigo["ID da Ordem"].astype(str).str.strip()
                    # parser tolerante
                    dt = pd.to_datetime(df_sla_antigo["Data do Evento"], errors="coerce", utc=False)
                    mask = ~dt.isna()
                    if mask.any():
                        df_sla_antigo = df_sla_antigo.loc[mask].copy()
                        df_sla_antigo["__dt__"] = dt.dt.tz_localize(None) if getattr(dt.dt, "tz", None) is not None else dt
                        cutoff_by_order = df_sla_antigo.groupby("ID da Ordem")["__dt__"].max().to_dict()
            except Exception as e:
                logging.warning(f"⚠️ Não consegui ler watermark do SLA antigo: {e}")

        # ============================
        # 4) Gera SLA APENAS para novos/alterados (com colapso A,A,B,A,B -> A,B,A,B)
        # ============================
        start_sla_new = time.time()
        ordens_por_id = {str(o["id"]): o for o in raw_orders}
        raw_orders_novos = [ordens_por_id[str(oid)] for oid in order_ids if str(oid) in ordens_por_id]

        # >>> Importante: sua função gerar_report_sla deve:
        # - aplicar cutoff_by_order (event_time > watermark);
        # - colapsar repetições consecutivas A,A,B,A,B -> A,B,A,B.
        df_sla_novos = gerar_report_sla(
            raw_orders_novos,
            config_id,
            workflow_cache,
            cutoff_by_order=cutoff_by_order  # <<< novo parâmetro
        )

        # limpeza leve
        for col in df_sla_novos.select_dtypes(include="object").columns:
            df_sla_novos[col] = df_sla_novos[col].map(clean_illegal_chars)

        df_sla_novos.to_csv(f"novos_sla_config_{config_id}.csv", index=False)
        logging.info(f"🆕 SLA (novos) gerado em {round(time.time() - start_sla_new, 2)}s")

        # ============================
        # 5) Acumular (sem reconsultar API para antigas)
        # ============================
        df_final = acumular_relatorio_principal(df_result, csv_acumulado_latest)
        df_sla_final = acumular_report_sla(df_sla_novos, csv_acumulado_sla_latest)

        # (Opcional) salvos adicionais datados — o accumulator já gera _latest + datado.
        # Mantidos aqui caso você queira versionamento extra com sufixo de config+timestamp:
        csv_acumulado_path = f"acumulado_config_{config_id}_{current_datetime}.csv"
        csv_acumulado_sla_path = f"acumulado_sla_config_{config_id}_{current_datetime}.csv"
        try:
            df_final.to_csv(csv_acumulado_path, index=False)
            df_sla_final.to_csv(csv_acumulado_sla_path, index=False)
        except Exception as e:
            logging.warning(f"⚠️ Falha ao salvar cópias datadas adicionais: {e}")

        # ============================
        # 6) Excel, dashboard e envio
        # ============================
        executar_dashboard_personalizado(config_id, df_final, file_path)
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            df_final.to_excel(writer, sheet_name="report", index=False)
            df_sla_final.to_excel(writer, sheet_name="report_SLA", index=False)

        subject, body = _montar_email(config_name, config_id, current_datetime)
        to_email = ", ".join(emails)
        send_email_with_attachment(from_email, to_email, subject, body, app_password, file_path)

        # registro execução
        with open(f"last_run_config_{config_id}.json", "w") as f:
            json.dump({"last_updated": datetime.now().isoformat()}, f)

        logging.info(f"🏁 Config {config_id} concluída em {round(time.time() - start_config_time, 2)}s")


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
