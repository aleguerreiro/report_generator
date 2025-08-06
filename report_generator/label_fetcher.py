# label_fetcher.py

import logging
from report_generator.data_utils import get_with_retry

def fetch_labels_from_workflow(config_id, token, get_with_retry=get_with_retry):
    """
    Busca as etiquetas (labels) associadas a um workflow da Zapform.

    Args:
        config_id (str): ID do workflow/configuração.
        token (str): Token de autenticação.
        get_with_retry (function, optional): Função para requisição GET com retry.
            Se não fornecida, será usada a função padrão importada.

    Returns:
        dict: Dicionário {label_id: label_title}.
    """
    url = f"https://api.zapform.com.br/api/v2/workflow/{config_id}/"
    headers = {
        "accept": "application/json",
        "Authorization": f"Token {token}"
    }

    try:
        response = get_with_retry(url, headers=headers)
        if response and response.status_code == 200:
            workflow_data = response.json()
            labels = workflow_data.get("extra_data", {}).get("labels", [])
            etiquetas_dict = {label["id"]: label["title"] for label in labels}
            logging.info(f"🎯 {len(etiquetas_dict)} etiquetas carregadas do workflow {config_id}")
            return etiquetas_dict
        else:
            logging.warning(f"⚠️ Falha ao buscar workflow {config_id}. Status: {response.status_code if response else 'sem resposta'}")
            return {}
    except Exception as e:
        logging.error(f"❌ Erro ao processar etiquetas do workflow {config_id}: {e}")
        return {}
