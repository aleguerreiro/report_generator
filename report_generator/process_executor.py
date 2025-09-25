import os
import json
import logging
from datetime import datetime
import pandas as pd
from tqdm import tqdm

from .data_utils import clean_illegal_chars, get_with_retry
from .accumulator import acumular_relatorio_principal, acumular_report_sla
from .zapform_api_client import (
    fetch_orders_by_date,
    fetch_all_orders,
    fetch_order_data,
    get_config_name,
)
from .label_fetcher import fetch_labels_from_workflow
from .sheet_config_reader import (
    read_config_sheet,
    build_filters_from_sheet,
    extract_default_fields,
    extract_variable_fields,
    extract_email_list,
    extract_header_report_map,
    aplicar_filtro_incremental,
)
from .email_sender import send_email_with_attachment
try:
    # se existir, envia multi-anexos em um único e-mail
    from .email_sender import send_email_with_attachments as _send_multi
except Exception:
    _send_multi = None

from .dashboard_executor import executar_dashboard_personalizado
from .zapform_auth import TokenManager
from .extractor import extract_data
from .sla_report_generator import gerar_report_sla
from .utils.sla_utils import fetch_sla_by_config
from .due_date_report_generator import generate_and_send_due_date_report
import time


REPORTS_OUTPUT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "all reports")
)


# ============================
# Helpers de DASH/arquivos
# ============================
def _has_dashboard_config(config_id: str) -> bool:
    """Verifica se existe um dash_config_{config_id}.py ao lado deste módulo."""
    base_dir = os.path.dirname(__file__)
    dash_py = os.path.join(base_dir, f"dash_config_{config_id}.py")
    return os.path.exists(dash_py)

def _exists_nonempty(path: str) -> bool:
    """Retorna True se o arquivo existir e tiver tamanho > 0."""
    try:
        return bool(path) and os.path.exists(path) and os.path.getsize(path) > 0
    except Exception:
        return False


def executar_processo(logins, spreadsheet):
    from_email, app_password = _carregar_credenciais_email()
    token_manager = TokenManager(logins)
    workflow_cache = {}
    os.makedirs(REPORTS_OUTPUT_DIR, exist_ok=True)

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
            config_id, token_manager.get_token(), get_with_retry=get_with_retry
        )
        logging.info(f"🎯 {len(etiquetas_dict)} etiquetas carregadas para config {config_id}")

        sla_config_dict = fetch_sla_by_config(
            config_id, token_manager.get_token(), get_with_retry=get_with_retry
        )
        logging.info(f"📜 SLA config carregado para {config_id}")

        # cache do workflow
        if config_id not in workflow_cache:
            url = f"https://api.zapform.com.br/api/v2/workflow/{config_id}/"
            headers = {"accept": "application/json", "Authorization": f"Token {token_manager.get_token()}"}
            response = get_with_retry(url, headers=headers)
            workflow_cache[config_id] = response.json() if response and response.status_code == 200 else {}

        # ⚙️ nomes de arquivos
        current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M")
        config_name = workflow_cache.get(config_id, {}).get("name") or get_config_name(
            config_id, token_manager.get_token()
        )
        safe_name = "".join(c if c.isalnum() or c in "._-" else "_" for c in config_name)[:40]
        report_filename = f"report_{safe_name}_{current_datetime}.xlsx"
        dash_filename = f"dash_report_{safe_name}_{current_datetime}.xlsx"
        report_path = os.path.join(REPORTS_OUTPUT_DIR, report_filename)
        dash_path = os.path.join(REPORTS_OUTPUT_DIR, dash_filename)

        csv_acumulado_latest     = f"acumulado_config_{config_id}_latest.csv"
        csv_acumulado_sla_latest = f"acumulado_sla_config_{config_id}_latest.csv"

        # ============================
        # 1) Buscar IDs
        # ============================
        order_ids = []
        t0 = time.time()
        if usar_todas:
            print(f"🔍 Iniciando busca de ordens para config {config_id}...")
            logging.info(f"🔍 Iniciando busca de ordens para config {config_id}...")
            if status_list:
                for status in status_list:
                    f = {**filtros, "status": status}
                    ids = fetch_orders_by_date(token_manager.get_token(), config_id, f, get_with_retry=get_with_retry)
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
                order_ids = fetch_all_orders(token_manager, config_id, grupo_filtro, get_with_retry=get_with_retry)
            else:
                print(f"🔎 Buscando todas as ordens da config {config_id} sem filtros")
                order_ids = fetch_all_orders(token_manager, config_id, {}, get_with_retry=get_with_retry)

            order_ids = list(dict.fromkeys(order_ids))
            logging.info(f"✅ {len(order_ids)} ordens encontradas para a config {config_id}")
        else:
            order_ids = [
                oid.strip()
                for oid in df["orders"].dropna().astype(str).tolist()
                if oid.strip().lower() != "all" and oid.strip() != ""
            ]
            order_ids = list(dict.fromkeys(order_ids))

        logging.info(f"⏱️ Tempo para buscar IDs: {round(time.time() - t0, 2)}s")
        logging.info(f"🧾 {len(order_ids)} IDs para processar em config {config_id}")

        # ============================
        # Sem mudanças → reusa acumulados, gera dash (se existir), envia anexos
        # ============================
        if not order_ids:
            logging.info(f"🔕 Sem mudanças para config {config_id}. Reutilizando acumulados _latest e pulando API.")
            df_final = pd.read_csv(csv_acumulado_latest) if os.path.exists(csv_acumulado_latest) else pd.DataFrame()
            df_sla_final = (
                pd.read_csv(csv_acumulado_sla_latest, dtype={"ID da Ordem": str, "Etapa": str})
                if os.path.exists(csv_acumulado_sla_latest) else pd.DataFrame()
            )

            # DASH EM ARQUIVO SEPARADO (somente se existir dash_config_{config_id}.py)
            dash_ok = False
            if _has_dashboard_config(config_id):
                logging.info(f"📊 Gerando dashboard personalizado (dash_config_{config_id}.py) em: {dash_path}")
                executar_dashboard_personalizado(config_id, df_final, dash_path)
                dash_ok = _exists_nonempty(dash_path)
            else:
                logging.info(f"ℹ️ Nenhum dashboard customizado para config {config_id} (dash_config_{config_id}.py não encontrado)")

            # REPORT (sem dash)
            if not df_final.empty or not df_sla_final.empty:
                for _df in (df_final, df_sla_final):
                    if _df is not None and not _df.empty:
                        for _c in _df.select_dtypes(include="object").columns:
                            _df[_c] = _df[_c].map(clean_illegal_chars)

                with pd.ExcelWriter(report_path, engine='openpyxl', mode='w') as writer:
                    if not df_final.empty:
                        df_final.to_excel(writer, sheet_name="report", index=False)
                    if not df_sla_final.empty:
                        df_sla_final.to_excel(writer, sheet_name="report_SLA", index=False)
            else:
                logging.info(f"⏭️ Nenhum dado para gerar Excel na config {config_id}, pulando.")
                continue

            subject, body = _montar_email(config_name, config_id, current_datetime, filtros)
            _enviar_email_duplo(
                from_email, ", ".join(emails), subject, body, app_password,
                report_path, dash_path if dash_ok else None
            )

            # DUE_DATE (somente e-mail)
            try:
                start_due = time.time()
                generate_and_send_due_date_report(
                    config=df, cards=df_final, from_email=from_email, app_password=app_password,
                    config_id=config_id, subject_base=subject, raw_orders=None,
                    api_token=token_manager.get_token(), do_email=True, do_accumulate=False,
                )
                logging.info(f"✅ due_date: concluído para config {config_id} | duração={round(time.time()-start_due,2)}s")
            except Exception as _e:
                logging.error(f"Falha ao enviar relatório de vencidos: {_e}")

            with open(f"last_run_config_{config_id}.json", "w") as f:
                json.dump({"last_updated": datetime.now().isoformat()}, f)

            logging.info(f"🏁 Config {config_id} concluída em {round(time.time() - start_config_time, 2)}s (sem mudanças)")
            continue

        # ============================
        # 2) Busca detalhada dos IDs
        # ============================
        results, raw_orders = [], []
        t1 = time.time()
        for order_id in tqdm(order_ids, desc=f"Config {config_id}"):
            order_data = fetch_order_data(config_id, order_id, token_manager, get_with_retry=get_with_retry)
            if not order_data:
                continue
            code = str(order_data.get("status", {}).get("code", "")).strip()
            if status_list and code not in status_list:
                continue
            raw_orders.append(order_data)
            data = extract_data(order_data, campos_variaveis, etiquetas_dict, header_report_dict, campos_padroes)
            results.append(pd.Series(data))
        logging.info(f"📦 {len(raw_orders)} ordens detalhadas buscadas")
        logging.info(f"⏱️ Tempo para buscar ordens: {round(time.time() - t1, 2)}s")

        df_result = pd.DataFrame(results)

        # DUE_DATE (acúmulo incremental)
        try:
            logging.info(f"🗂️ due_date: acumulando snapshot incremental (rows={len(df_result)})")
            generate_and_send_due_date_report(
                config=df, cards=df_result, from_email=from_email, app_password=app_password,
                config_id=config_id, subject_base=None, raw_orders=raw_orders,
                api_token=token_manager.get_token(), do_email=False, do_accumulate=True,
            )
        except Exception as _e:
            logging.error(f"Falha no acúmulo do due_date incremental: {_e}")

        # Watermark SLA
        cutoff_by_order = {}
        if os.path.exists(csv_acumulado_sla_latest):
            try:
                df_sla_antigo = pd.read_csv(csv_acumulado_sla_latest, dtype={"ID da Ordem": str})
                if "ID da Ordem" in df_sla_antigo.columns and "Data do Evento" in df_sla_antigo.columns:
                    df_sla_antigo["ID da Ordem"] = df_sla_antigo["ID da Ordem"].astype(str).str.strip()
                    dt = pd.to_datetime(df_sla_antigo["Data do Evento"], errors="coerce", utc=False)
                    mask = ~dt.isna()
                    if mask.any():
                        df_sla_antigo = df_sla_antigo.loc[mask].copy()
                        df_sla_antigo["__dt__"] = dt
                        cutoff_by_order = df_sla_antigo.groupby("ID da Ordem")["__dt__"].max().to_dict()
            except Exception as e:
                logging.warning(f"⚠️ Não consegui ler watermark do SLA antigo: {e}")

        # SLA somente novos/alterados
        start_sla_new = time.time()
        ordens_por_id = {str(o["id"]): o for o in raw_orders}
        raw_orders_novos = [ordens_por_id[str(oid)] for oid in order_ids if str(oid) in ordens_por_id]
        df_sla_novos = gerar_report_sla(raw_orders_novos, config_id, workflow_cache, cutoff_by_order=cutoff_by_order)

        for col in df_sla_novos.select_dtypes(include="object").columns:
            df_sla_novos[col] = df_sla_novos[col].map(clean_illegal_chars)

        df_sla_novos.to_csv(f"novos_sla_config_{config_id}.csv", index=False)
        logging.info(f"🆕 SLA (novos) gerado em {round(time.time() - start_sla_new, 2)}s")

        # Acumular
        df_final = acumular_relatorio_principal(df_result, csv_acumulado_latest)
        df_sla_final = acumular_report_sla(df_sla_novos, csv_acumulado_sla_latest)

        # cópias datadas opcionais
        try:
            df_final.to_csv(f"acumulado_config_{config_id}_{current_datetime}.csv", index=False)
            df_sla_final.to_csv(f"acumulado_sla_config_{config_id}_{current_datetime}.csv", index=False)
        except Exception as e:
            logging.warning(f"⚠️ Falha ao salvar cópias datadas adicionais: {e}")

        # ============================
        # 6) Gerar arquivos e enviar
        # ============================
        # DASH em arquivo separado (somente se existir dash_config_{config_id}.py)
        dash_ok = False
        if _has_dashboard_config(config_id):
            logging.info(f"📊 Gerando dashboard personalizado (dash_config_{config_id}.py) em: {dash_path}")
            executar_dashboard_personalizado(config_id, df_final, dash_path)
            dash_ok = _exists_nonempty(dash_path)
        else:
            logging.info(f"ℹ️ Nenhum dashboard customizado para config {config_id} (dash_config_{config_id}.py não encontrado)")

        # REPORT (sem dash)
        for _df in (df_final, df_sla_final):
            if _df is not None and not _df.empty:
                for _c in _df.select_dtypes(include="object").columns:
                    _df[_c] = _df[_c].map(clean_illegal_chars)

        with pd.ExcelWriter(report_path, engine='openpyxl', mode='w') as writer:
            df_final.to_excel(writer, sheet_name="report", index=False)
            df_sla_final.to_excel(writer, sheet_name="report_SLA", index=False)

        subject, body = _montar_email(config_name, config_id, current_datetime)
        subject = _ajustar_periodo_no_subject(subject, df_final)

        _enviar_email_duplo(
            from_email, ", ".join(emails), subject, body, app_password,
            report_path, dash_path if dash_ok else None
        )

        # DUE_DATE (envio final)
        try:
            start_due = time.time()
            logging.info(f"🕒 due_date: iniciando para config {config_id} | total_cards={len(df_final)}")
            generate_and_send_due_date_report(
                config=df, cards=df_final, from_email=from_email, app_password=app_password,
                config_id=config_id, subject_base=subject, raw_orders=raw_orders,
                api_token=token_manager.get_token(), do_email=True, do_accumulate=False,
            )
            logging.info(f"✅ due_date: concluído para config {config_id} | duração={round(time.time()-start_due,2)}s")
        except Exception as _e:
            logging.error(f"Falha ao enviar relatório de vencidos: {_e}")

        with open(f"last_run_config_{config_id}.json", "w") as f:
            json.dump({"last_updated": datetime.now().isoformat()}, f)

        logging.info(f"🏁 Config {config_id} concluída em {round(time.time() - start_config_time, 2)}s")


def _encontrar_coluna_data(df: pd.DataFrame):
    """Tenta achar a coluna de datas usada no dashboard."""
    if df is None or df.empty:
        return None
    candidatos = ["Data Registro", "Data de Registro", "Data de registro"]
    for c in candidatos:
        if c in df.columns:
            return c
    # fallback: 1ª coluna que vira datetime para pelo menos 1 linha
    for c in df.columns:
        s = pd.to_datetime(df[c], errors="coerce", dayfirst=True)
        if s.notna().any():
            return c
    return None


def _ajustar_periodo_no_subject(subject: str, df: pd.DataFrame) -> str:
    """
    Substitui (ou acrescenta) o trecho 'Período considerado X a Y' no assunto,
    usando min/max do DataFrame (mesma lógica do dashboard).
    """
    col = _encontrar_coluna_data(df)
    if not col:
        return subject
    s = pd.to_datetime(df[col], errors="coerce", dayfirst=True).dropna()
    if s.empty:
        return subject

    ini = s.min().strftime("%d/%m/%Y")
    fim = s.max().strftime("%d/%m/%Y")
    periodo_txt = f"Período considerado {ini} a {fim}"

    if "Período considerado" in subject:
        # corta a partir do texto e re-monta
        prefixo = subject.split("Período considerado")[0].rstrip(" |")
        subject = f"{prefixo} | {periodo_txt}"
    else:
        subject = f"{subject} | {periodo_txt}"
    return subject


def _enviar_email_duplo(from_email, to_email, subject, body, app_password, report_path, dash_path):
    """
    Envia e-mails conforme a disponibilidade de anexos:
      - Se apenas REPORT existir -> envia só o REPORT.
      - Se apenas DASH existir  -> envia só o DASH.
      - Se ambos existirem      -> envia dois e-mails (um para cada), com assuntos distintos.
    (Opcional) Você pode unificar em um e-mail com dois anexos se tiver _send_multi (descomentando bloco abaixo).
    """
    report_ok = _exists_nonempty(report_path)
    dash_ok = _exists_nonempty(dash_path) if dash_path else False

    if not report_ok and not dash_ok:
        logging.warning("⚠️ Nenhum anexo válido para enviar (report e dash ausentes ou vazios).")
        return

    # Assuntos

    subj_dash = subject  # mantém período

    # remove qualquer " | Período considerado..." e ajusta para Geral
    subj_report = subject.split(" | Período considerado")[0]
    subj_report = subj_report.replace("Relatório Dashboard", "Relatório Geral Zapform")
    subj_report = subj_report.replace("Dashboard", "Geral Zapform")
    if subj_report == subject:  # fallback
        subj_report = "Relatório Geral Zapform"


    # --- Envio opcional unificado (2 anexos em 1 e-mail) ---
    # if report_ok and dash_ok and _send_multi:
    #     try:
    #         _send_multi(
    #             from_email=from_email,
    #             to_email=to_email,
    #             subject=subj_report,  # usa o do report como principal
    #             body=body,
    #             app_password=app_password,
    #             attachment_paths=[report_path, dash_path],
    #         )
    #         logging.info("📧 1 e-mail enviado com dois anexos (report + dash).")
    #         return
    #     except Exception as e:
    #         logging.error(f"Erro ao enviar e-mail multi-anexo, fallback para e-mails separados: {e}")

    # Envio separado (garantido e compatível)
    if report_ok:
        send_email_with_attachment(
            from_email=from_email,
            to_email=to_email,
            subject=subj_report,
            body=body,
            app_password=app_password,
            attachment_path=report_path,
        )
        logging.info("📧 E-mail do REPORT enviado.")

    if dash_ok:
        send_email_with_attachment(
            from_email=from_email,
            to_email=to_email,
            subject=subj_dash,
            body=body,
            app_password=app_password,
            attachment_path=dash_path,
        )
        logging.info("📧 E-mail do DASH enviado.")


def _carregar_credenciais_email():
    with open("email_credentials.json") as f:
        config = json.load(f)
    return config["email"], config["senha_app"]


def _montar_email(config_name, config_id, current_datetime, filtros=None):
    """
    Assunto: 'Relatório Dashboard | {config_name} | Período considerado DD/MM/YY até DD/MM/YY'
    Se não houver filtros de data, usa a data atual para ambos.
    """
    # tenta pegar início/fim dos filtros (qualquer um dos grupos abaixo)
    ini_dt = fim_dt = None
    if filtros:
        def pick(prefix):
            after = next((filtros.get(k) for k in (f"{prefix}_after", f"{prefix}_from", f"{prefix}_gte") if filtros.get(k)), None)
            before = next((filtros.get(k) for k in (f"{prefix}_before", f"{prefix}_to", f"{prefix}_lte") if filtros.get(k)), None)
            return after, before

        for p in ("time_created", "time_last_updated", "time_status"):
            after, before = pick(p)
            if after or before:
                ini_dt = pd.to_datetime(after or before, errors="coerce")
                fim_dt = pd.to_datetime(before or after, errors="coerce")
                break

    # fallback: usa a data do arquivo para ambos
    if ini_dt is None or pd.isna(ini_dt) or fim_dt is None or pd.isna(fim_dt):
        base_dt = datetime.strptime(current_datetime.split("_")[0], "%Y-%m-%d")
        ini_dt = ini_dt if (ini_dt is not None and not pd.isna(ini_dt)) else base_dt
        fim_dt = fim_dt if (fim_dt is not None and not pd.isna(fim_dt)) else base_dt

    period_str = f"{ini_dt:%d/%m/%y} até {fim_dt:%d/%m/%y}"
    subject = f"Relatório Dashboard | {config_name} | Período considerado {period_str}"

    body = (
        "Olá!\n\n"
        f"Segue em anexo o relatório gerado automaticamente para a configuração {config_id}.\n\n"
        "Atenciosamente,\nEquipe Zapform 😉"
    )
    return subject, body
