import os
import logging
import pandas as pd
from datetime import datetime
from .data_utils import clean_illegal_chars


def _salvar_acumulado_com_versionamento(df_final, base_path):
    """Salva o DataFrame em três arquivos: o principal, um com sufixo _latest, e um com timestamp."""
    base, ext = os.path.splitext(base_path)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    
    # Garante que o base_path não tenha sufixos como _latest
    if base.endswith('_latest'):
        base = base.rsplit('_latest', 1)[0]

    main_path = f"{base}{ext}"
    latest_path = f"{base}_latest{ext}"
    dated_path = f"{base}_{timestamp}{ext}"

    try:
        df_final.to_csv(main_path, index=False)
        df_final.to_csv(latest_path, index=False)
        df_final.to_csv(dated_path, index=False)
        logging.info(f"✅ Arquivos salvos: {os.path.basename(main_path)}, {os.path.basename(latest_path)}, {os.path.basename(dated_path)}")
    except Exception as e:
        logging.error(f"❌ Erro ao salvar arquivos CSV: {e}")


def acumular_relatorio_principal(df_result, csv_path):
    """Acumula e deduplica o report principal."""
    if os.path.exists(csv_path):
        try:
            df_antigo = pd.read_csv(csv_path)
            df_final = pd.concat([df_antigo, df_result], ignore_index=True)
            df_final.drop_duplicates(subset="ID do Card", keep="last", inplace=True)
        except Exception as e:
            logging.warning(f"⚠️ Erro ao ler CSV antigo: {e}")
            df_final = df_result.copy()
    else:
        df_final = df_result.copy()

    for col in df_final.select_dtypes(include="object").columns:
        df_final[col] = df_final[col].map(clean_illegal_chars)

    _salvar_acumulado_com_versionamento(df_final, csv_path)
    return df_final


def acumular_report_sla(df_sla, csv_path):
    """Acumula e deduplica o report SLA por (ID da Ordem + Etapa)."""
    df_sla["ID da Ordem"] = df_sla["ID da Ordem"].astype(str).str.strip()
    df_sla["Etapa"] = df_sla["Etapa"].astype(str).str.strip()

    if os.path.exists(csv_path):
        try:
            df_sla_antigo = pd.read_csv(csv_path, dtype={"ID da Ordem": str, "Etapa": str})
            df_sla_antigo["ID da Ordem"] = df_sla_antigo["ID da Ordem"].astype(str).str.strip()
            df_sla_antigo["Etapa"] = df_sla_antigo["Etapa"].astype(str).str.strip()

            # Cria a chave para os dataframes novo e antigo
            df_sla["__chave__"] = df_sla["ID da Ordem"] + "|" + df_sla["Etapa"]
            df_sla_antigo["__chave__"] = df_sla_antigo["ID da Ordem"] + "|" + df_sla_antigo["Etapa"]

            # Filtra o dataframe antigo, mantendo apenas as chaves que não estão no novo
            chaves_atuais = df_sla["__chave__"].unique()
            df_sla_antigo_filtrado = df_sla_antigo[~df_sla_antigo["__chave__"].isin(chaves_atuais)]

            # Concatena o antigo filtrado com o novo, sem a coluna de chave
            df_sla_final = pd.concat(
                [df_sla_antigo_filtrado, df_sla],
                ignore_index=True
            )
            
            # Remove a coluna de chave do resultado final
            if "__chave__" in df_sla_final.columns:
                df_sla_final.drop(columns="__chave__", inplace=True)

        except Exception as e:
            logging.warning(f"⚠️ Erro ao processar CSV SLA acumulado: {e}")
            df_sla_final = df_sla.copy()
    else:
        df_sla_final = df_sla.copy()

    for col in df_sla_final.select_dtypes(include="object").columns:
        if col != '__chave__':
            df_sla_final[col] = df_sla_final[col].map(clean_illegal_chars)

    _salvar_acumulado_com_versionamento(df_sla_final, csv_path)
    return df_sla_final
