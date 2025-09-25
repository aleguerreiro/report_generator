import os
import logging
from datetime import datetime
from typing import Optional

import pandas as pd

from .data_utils import clean_illegal_chars


def _salvar_acumulado_com_versionamento(df_final, base_path):
    """Salva o DataFrame em três arquivos: o principal, um _latest e um com timestamp."""
    base, ext = os.path.splitext(base_path)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")

    if base.endswith('_latest'):
        base = base.rsplit('_latest', 1)[0]

    main_path = f"{base}{ext}"
    latest_path = f"{base}_latest{ext}"
    dated_path = f"{base}_{timestamp}{ext}"

    try:
        df_final.to_csv(main_path, index=False)
        df_final.to_csv(latest_path, index=False)
        df_final.to_csv(dated_path, index=False)
        logging.info(
            f"✅ Arquivos salvos: {os.path.basename(main_path)}, "
            f"{os.path.basename(latest_path)}, {os.path.basename(dated_path)}"
        )
    except Exception as e:
        logging.error(f"❌ Erro ao salvar arquivos CSV: {e}")


def _read_csv_safe(path, dtype=None):
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=dtype)
    except Exception as e:
        logging.warning(f"⚠️ Erro ao ler CSV '{path}': {e}")
        return pd.DataFrame()


def _clean_strings(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].map(clean_illegal_chars)
    return df


# =========================
# Report Principal
# =========================
def acumular_relatorio_principal(df_result, csv_path):
    """Acumula e deduplica o report principal por 'ID do Card' (fallback: 'ID da Ordem')."""
    df_result = df_result.copy()

    # coluna-chave
    key_col = "ID do Card" if "ID do Card" in df_result.columns else "ID da Ordem"
    if key_col not in df_result.columns:
        # se não houver nenhuma, apenas salva/retorna
        logging.warning("⚠️ Nenhuma coluna de chave ('ID do Card' ou 'ID da Ordem') encontrada no report principal.")
        df_final = _clean_strings(df_result)
        _salvar_acumulado_com_versionamento(df_final, csv_path)
        return df_final

    df_result[key_col] = df_result[key_col].astype(str).str.strip()

    df_antigo = _read_csv_safe(csv_path)
    if not df_antigo.empty and key_col in df_antigo.columns:
        df_antigo[key_col] = df_antigo[key_col].astype(str).str.strip()
        df_final = pd.concat([df_antigo, df_result], ignore_index=True)
        df_final.drop_duplicates(subset=key_col, keep="last", inplace=True)
    else:
        df_final = df_result.copy()

    df_final = _clean_strings(df_final)
    _salvar_acumulado_com_versionamento(df_final, csv_path)
    return df_final


# =========================
# Report SLA (granular por evento)
# =========================
def acumular_report_sla(df_sla, csv_path):
    """
    Acumula e deduplica o SLA por chave GRANULAR (evento), priorizando:
      1) ["ID da Ordem","Etapa","Código do Status","Data do Evento"]
      2) ["ID da Ordem","Etapa","Código do Status"]
      3) ["ID da Ordem","Etapa"]
    Faz migração automática do CSV antigo para criar colunas ausentes.
    """
    import datetime as _dt_local

    def _try_parse_dt(s):
        if pd.isna(s):
            return None
        s = str(s).strip()
        # tenta formatos mais comuns
        for fmt in ("%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return _dt_local.datetime.strptime(s, fmt)
            except Exception:
                pass
        # fallback tolerante
        try:
            v = pd.to_datetime(s, dayfirst=True, errors="coerce")
            return None if pd.isna(v) else v.to_pydatetime()
        except Exception:
            return None

    df_sla = df_sla.copy()
    for col in df_sla.select_dtypes(include="object").columns:
        df_sla[col] = df_sla[col].astype(str).str.strip()

    CANDIDATAS = [
        ["ID da Ordem", "Etapa", "Código do Status", "Data do Evento"],
        ["ID da Ordem", "Etapa", "Código do Status"],
        ["ID da Ordem", "Etapa"],
    ]
    key_cols_new = next((ks for ks in CANDIDATAS if all(k in df_sla.columns for k in ks)), None)
    if key_cols_new is None:
        raise ValueError(
            "Não foi possível determinar colunas-chave do SLA no dataframe novo. "
            "Esperado ao menos ['ID da Ordem','Etapa'] e, idealmente, 'Código do Status' e 'Data do Evento'."
        )

    # garante "Código do Status" no df novo (caso ainda venha como 'Código')
    if "Código do Status" in key_cols_new and "Código do Status" not in df_sla.columns and "Código" in df_sla.columns:
        df_sla["Código do Status"] = df_sla["Código"].astype(str).str.strip()

    # cria chave no novo
    df_sla["__chave__"] = df_sla[key_cols_new].astype(str).agg("|".join, axis=1)

    # carrega acumulado antigo
    dtypes = {k: "string" for k in set(sum(CANDIDATAS, []))}
    df_antigo = _read_csv_safe(csv_path, dtype=dtypes)

    # ---- MIGRAÇÃO DO ANTIGO (se necessário) ----
    if not df_antigo.empty:
        for col in df_antigo.select_dtypes(include="object").columns:
            df_antigo[col] = df_antigo[col].astype(str).str.strip()

        # cria 'Código do Status' a partir de 'Código'
        if "Código do Status" not in df_antigo.columns and "Código" in df_antigo.columns:
            df_antigo["Código do Status"] = df_antigo["Código"].astype(str).str.strip()

        # cria 'Data do Evento' a partir de colunas de data existentes
        if "Data do Evento" not in df_antigo.columns:
            candidato_data = None
            for c in ["Data do Evento", "Data Início Execução", "Data Inicio Execucao", "Data Início", "Data Inicio"]:
                if c in df_antigo.columns:
                    candidato_data = c
                    break
            if candidato_data:
                parsed = df_antigo[candidato_data].apply(_try_parse_dt)
                df_antigo["Data do Evento"] = parsed.apply(
                    lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if isinstance(x, _dt_local.datetime) else ""
                )

        # remove linhas sem ID/Etapa
        for base_col in ["ID da Ordem", "Etapa"]:
            if base_col in df_antigo.columns:
                df_antigo = df_antigo[df_antigo[base_col].astype(str).str.strip() != ""]

        # agora tentamos usar a MESMA key do novo; se faltar algo, caímos para uma menos granular
        def _pick_key_for_old():
            for ks in CANDIDATAS:
                if all(k in df_antigo.columns for k in ks):
                    return ks
            return None

        key_cols_old = _pick_key_for_old()

        if key_cols_old is None:
            logging.warning("⚠️ CSV SLA antigo não tinha colunas mínimas; inicia acumulado do zero.")
            df_antigo = pd.DataFrame()
        else:
            df_antigo["__chave__"] = df_antigo[key_cols_old].astype(str).agg("|".join, axis=1)

            inter = [c for c in key_cols_old if c in key_cols_new]
            if inter:
                df_sla["__chave_inter__"] = df_sla[inter].astype(str).agg("|".join, axis=1)
                df_antigo["__chave_inter__"] = df_antigo[inter].astype(str).agg("|".join, axis=1)
                df_antigo = df_antigo[~df_antigo["__chave_inter__"].isin(set(df_sla["__chave_inter__"]))]
                df_antigo.drop(columns=[c for c in ["__chave_inter__"] if c in df_antigo.columns], inplace=True, errors="ignore")
                df_sla.drop(columns=[c for c in ["__chave_inter__"] if c in df_sla.columns], inplace=True, errors="ignore")

    # ---- MERGE INCREMENTAL ----
    if not df_antigo.empty and "__chave__" in df_antigo.columns:
        chaves_novas = set(df_sla["__chave__"].unique())
        df_antigo_filtrado = df_antigo[~df_antigo["__chave__"].isin(chaves_novas)]
        df_sla_final = pd.concat([df_antigo_filtrado, df_sla], ignore_index=True)
    elif not df_antigo.empty:
        inter = [c for c in ["ID da Ordem","Etapa","Código do Status","Data do Evento"] if c in df_antigo.columns and c in df_sla.columns]
        if inter:
            df_tmp = pd.concat([df_antigo, df_sla], ignore_index=True)
            df_sla_final = df_tmp.drop_duplicates(subset=inter, keep="last")
        else:
            df_sla_final = pd.concat([df_antigo, df_sla], ignore_index=True)
    else:
        df_sla_final = df_sla.copy()

    # limpeza
    for col in df_sla_final.select_dtypes(include="object").columns:
        if col != "__chave__":
            df_sla_final[col] = df_sla_final[col].map(clean_illegal_chars)
    if "__chave__" in df_sla_final.columns:
        df_sla_final.drop(columns="__chave__", inplace=True, errors="ignore")

    _salvar_acumulado_com_versionamento(df_sla_final, csv_path)
    return df_sla_final


# ---------------------------------------------------------------------------
# ACUMULADOR DE DUE_DATE  (por "ID da Ordem"; last-write-wins)
# Gera/acrescenta um CSV acumulado de due_date e salva:
#   base.csv, base_latest.csv e base_<YYYY-MM-DD_HH-MM>.csv
# ---------------------------------------------------------------------------

def _norm_date_str(v: Optional[str]) -> str:
    """Normaliza datas para 'YYYY-MM-DD' (string). Aceita ISO, dd/mm/yyyy, yyyy-mm-dd etc."""
    if v is None:
        return ""
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none"}:
        return ""
    # já está no formato desejado?
    try:
        _dt.datetime.strptime(s[:10], "%Y-%m-%d")
        return s[:10]
    except Exception:
        pass
    # remove 'Z' do ISO
    s2 = s.replace("Z", "").replace("z", "")
    # tenta ISO completo
    try:
        return _dt.datetime.fromisoformat(s2).date().strftime("%Y-%m-%d")
    except Exception:
        pass
    # tenta formatos comuns
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y/%m/%d", "%Y-%m-%d"):
        try:
            return _dt.datetime.strptime(s, fmt).date().strftime("%Y-%m-%d")
        except Exception:
            continue
    return ""

def _norm_status_code(v) -> str:
    """Normaliza o status_code para string numérica (ex.: '2', '3'; 'S' permanece 'S')."""
    if v is None:
        return ""
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none"}:
        return ""
    try:
        if "." in s:
            f = float(s)
            if f.is_integer():
                return str(int(f))
        if s.isdigit():
            return str(int(s))
    except Exception:
        pass
    return s

def _ensure_due_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["ID da Ordem", "due_date", "status_code", "order_summary", "Link do contrato"]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    out = df[cols].copy()
    # normalizações
    out["ID da Ordem"] = out["ID da Ordem"].astype(str).str.strip()
    out = out[out["ID da Ordem"] != ""]
    out["due_date"] = out["due_date"].map(_norm_date_str)
    out["status_code"] = out["status_code"].map(_norm_status_code)
    for c in ["order_summary", "Link do contrato"]:
        out[c] = out[c].fillna("").astype(str).str.strip()
    # remove duplicados do snapshot (mantém a última)
    out = out.drop_duplicates(subset=["ID da Ordem"], keep="last")
    return out

def acumular_report_due_dates(df_snapshot: pd.DataFrame, csv_latest_path: str) -> pd.DataFrame:
    """
    Acumula o snapshot de due_date no CSV _latest, atualizando linhas por 'ID da Ordem'.
    Regras:
      - Se o ID não existir no acumulado, adiciona.
      - Se existir, atualiza due_date se vier preenchido no snapshot.
      - Preenche/atualiza status_code, order_summary e Link do contrato quando vierem não vazios.
      - Mantém sempre a versão mais recente por ID.
    Também grava uma cópia datada se o caminho contiver '_latest'.
    """
    # 1) normaliza o snapshot
    snap = _ensure_due_columns(df_snapshot)

    # 2) carrega acumulado atual (se existir) e normaliza
    if os.path.exists(csv_latest_path):
        try:
            acc = pd.read_csv(csv_latest_path, dtype=str)
        except Exception:
            acc = pd.DataFrame(columns=["ID da Ordem", "due_date", "status_code", "order_summary", "Link do contrato"])
    else:
        acc = pd.DataFrame(columns=["ID da Ordem", "due_date", "status_code", "order_summary", "Link do contrato"])

    acc = _ensure_due_columns(acc)

    # 3) merge manual por ID
    acc_dict = {row["ID da Ordem"]: dict(row) for _, row in acc.iterrows()}

    for _, row in snap.iterrows():
        oid = row["ID da Ordem"]
        if oid not in acc_dict:
            acc_dict[oid] = dict(row)
            continue

        cur = acc_dict[oid]
        if row["due_date"]:
            cur["due_date"] = row["due_date"]
        if row["status_code"]:
            cur["status_code"] = row["status_code"]
        if row["order_summary"]:
            cur["order_summary"] = row["order_summary"]
        if row["Link do contrato"]:
            cur["Link do contrato"] = row["Link do contrato"]
        acc_dict[oid] = cur

    # 4) dataframe final (ordenado por ID para estabilidade)
    final_df = pd.DataFrame(acc_dict.values())
    final_df = final_df[["ID da Ordem", "due_date", "status_code", "order_summary", "Link do contrato"]]
    final_df = final_df.sort_values("ID da Ordem", key=lambda s: s.astype(str), ignore_index=True)

    # 5) persiste _latest
    final_df.to_csv(csv_latest_path, index=False)

    # cópia datada, se for um _latest
    if "_latest" in os.path.basename(csv_latest_path):
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
        dated_path = csv_latest_path.replace("_latest", f"_{ts}")
        try:
            final_df.to_csv(dated_path, index=False)
        except Exception:
            pass

    return final_df
