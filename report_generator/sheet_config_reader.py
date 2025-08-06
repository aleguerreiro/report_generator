import pandas as pd
from datetime import datetime
import json
import os
import logging


def parse_date_ptbr(date_str):
    try:
        return datetime.strptime(date_str.strip(), "%d/%m/%Y").date()
    except:
        return None


def format_param_date(raw_date, tipo="inicio"):
    try:
        if raw_date:
            data_base = datetime.strptime(raw_date.strip(), "%d/%m/%Y")
            return data_base.strftime("%Y-%m-%dT00:00:00.000Z") if tipo == "inicio" else data_base.strftime("%Y-%m-%dT23:59:59.999Z")
    except:
        pass
    return None


def build_filters_from_sheet(df):
    filtros = {}
    if "parâmetros" in df.columns and "valor" in df.columns:
        parametros = dict(zip(df["parâmetros"], df["valor"]))

        filtros = {
            "time_created__gt": format_param_date(parametros.get("time_created_start:"), "inicio"),
            "time_created__lte": format_param_date(parametros.get("time_created_finish:"), "fim"),
            "time_last_updated__gt": format_param_date(parametros.get("time_last_updated_start:"), "inicio"),
            "time_last_updated__lte": format_param_date(parametros.get("time_last_updated_finish:"), "fim"),
            "time_status__gt": format_param_date(parametros.get("time_status_start:"), "inicio"),
            "time_status__lte": format_param_date(parametros.get("time_status_finish:"), "fim"),
        }

        status = parametros.get("status:", "").strip()
        if status:
            filtros["status"] = status

        filtros = {k: v for k, v in filtros.items() if v}  # remove nulos

    return filtros


def aplicar_filtro_incremental(config_id, filtros):
    """Adiciona o filtro incremental de time_last_updated__gt se não estiver presente nos filtros da planilha."""
    last_run_file = f"last_run_config_{config_id}.json"
    if os.path.exists(last_run_file):
        try:
            with open(last_run_file, "r") as f:
                last_data = json.load(f)
                last_updated = last_data.get("last_updated")
                if last_updated and "time_last_updated__gt" not in filtros:
                    filtros["time_last_updated__gt"] = last_updated
                    logging.info(f"🕓 Filtro incremental aplicado: time_last_updated__gt = {last_updated}")
        except Exception as e:
            logging.warning(f"⚠️ Erro ao carregar filtro incremental de {last_run_file}: {e}")
    return filtros


def extract_default_fields(df):
    colunas = ["header_default", "ordem", "tag_default", "show_default"]
    if all(c in df.columns for c in colunas):
        df_default = df[colunas].dropna()
        df_default = df_default[df_default["show_default"].str.strip().str.lower() == "yes"]
        df_default["ordem"] = pd.to_numeric(df_default["ordem"], errors="coerce")
        df_default = df_default.sort_values(by="ordem")
        return list(zip(df_default["tag_default"], df_default["header_default"]))
    return []


def extract_variable_fields(df):
    if "tag_form" in df.columns and "type" in df.columns:
        return list(zip(df["tag_form"].tolist(), df["type"].tolist()))
    return []


def extract_email_list(df):
    if "Emails" in df.columns:
        return df["Emails"].dropna().unique().tolist()
    return []


def extract_header_report_map(df):
    if "tag_form" in df.columns and "header_report" in df.columns:
        return dict(zip(df["tag_form"], df["header_report"]))
    return {}


def read_config_sheet(ws):
    data = ws.get_all_values()
    if not data or len(data) < 2:
        return None

    df = pd.DataFrame(data[1:], columns=data[0])
    df = df.loc[:, ~df.columns.duplicated()]  # remove colunas duplicadas
    return df
