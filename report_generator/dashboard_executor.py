# dashboard_executor.py

import os
import logging
import importlib.util

def executar_dashboard_personalizado(config_id, df_final, file_path):
    """
    Tenta executar o dashboard personalizado para a configuração.

    Args:
        config_id (str): ID da configuração.
        df_final (pd.DataFrame): Dados consolidados.
        file_path (str): Caminho de saída para o arquivo Excel.
    """
    dashboard_module_name = f"dash_config_{config_id}"
    dashboard_filename = f"{dashboard_module_name}.py"

    if os.path.exists(dashboard_filename):
        try:
            logging.info(f"📊 Executando dashboard específico: {dashboard_filename}")
            spec = importlib.util.spec_from_file_location(dashboard_module_name, dashboard_filename)
            dashboard_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(dashboard_module)

            if hasattr(dashboard_module, "gerar_dashboard_excel"):
                dashboard_module.gerar_dashboard_excel(df_final, file_path)
                logging.info("✅ Dashboard personalizado executado com sucesso.")
            else:
                logging.warning(f"⚠️ Função 'gerar_dashboard_excel' não encontrada em {dashboard_filename}")
                fallback_excel(df_final, file_path)
        except Exception as e:
            logging.error(f"❌ Erro ao executar dashboard personalizado: {e}")
            fallback_excel(df_final, file_path)
    else:
        logging.info(f"ℹ️ Nenhum dashboard customizado encontrado para config {config_id}.")
        fallback_excel(df_final, file_path)

def fallback_excel(df_final, file_path):
    try:
        df_final.to_excel(file_path, index=False)
        logging.info(f"✅ Excel padrão salvo em: {file_path}")
    except Exception as e:
        logging.error(f"❌ Erro ao salvar Excel padrão: {e}")
