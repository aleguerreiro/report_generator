# dashboard_executor.py
import os
import sys
import logging
import importlib.util
from typing import List

def _candidate_paths(config_id: str) -> List[str]:
    fname = f"dash_config_{config_id}.py"

    cwd = os.getcwd()
    this_dir = os.path.dirname(os.path.abspath(__file__))                 # .../report_generator/report_generator
    project_root = os.path.dirname(this_dir)                              # .../report_generator
    # pasta do script principal (main.py)
    try:
        main_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    except Exception:
        main_dir = cwd

    candidates = [
        os.path.join(cwd, fname),
        os.path.join(main_dir, fname),
        os.path.join(project_root, fname),
        os.path.join(this_dir, fname),
    ]
    # remove duplicados preservando ordem
    seen = set()
    unique = []
    for p in candidates:
        p = os.path.normpath(p)
        if p not in seen:
            unique.append(p)
            seen.add(p)
    return unique

def _load_module_from_path(mod_name: str, path: str):
    spec = importlib.util.spec_from_file_location(mod_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Não foi possível criar spec para {path}")
    mod = importlib.util.module_from_spec(spec)
    # Ajuda módulos que fazem imports simples (sem ser relativos) a achar coisas no mesmo dir
    mod_dir = os.path.dirname(path)
    if mod_dir not in sys.path:
        sys.path.insert(0, mod_dir)
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    return mod

def executar_dashboard_personalizado(config_id, df_final, file_path):
    """
    Tenta executar o dashboard personalizado para a configuração.

    Args:
        config_id (str): ID da configuração.
        df_final (pd.DataFrame): Dados consolidados.
        file_path (str): Caminho de saída para o arquivo Excel.
    """
    candidates = _candidate_paths(config_id)
    logging.info("🔎 Procurando dash personalizado nesses caminhos (na ordem):")
    for p in candidates:
        logging.info(f"   • {p}")

    dashboard_module_name = f"dash_config_{config_id}"
    dashboard_path = next((p for p in candidates if os.path.exists(p)), None)

    if dashboard_path:
        try:
            logging.info(f"📊 Executando dashboard específico: {os.path.basename(dashboard_path)}")
            dashboard_module = _load_module_from_path(dashboard_module_name, dashboard_path)

            if hasattr(dashboard_module, "gerar_dashboard_excel"):
                dashboard_module.gerar_dashboard_excel(df_final, file_path)
                logging.info("✅ Dashboard personalizado executado com sucesso.")
            else:
                logging.warning(f"⚠️ Função 'gerar_dashboard_excel' não encontrada em {dashboard_path}")
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
