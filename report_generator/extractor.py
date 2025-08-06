# extractor.py

import json
import logging
from .data_utils import format_date, parse_option_list

def extract_data(order_json, campos_variaveis, etiquetas_dict, header_report_dict, campos_padroes):
    def get_label_title(label_id):
        return etiquetas_dict.get(label_id, "-")

    def handle_media(valor):
        if isinstance(valor, dict):
            tipo = valor.get("_type")
            if tipo == "image":
                return valor.get("url", "-")
            elif tipo == "location":
                lat = valor.get("lat")
                lng = valor.get("lng")
                if lat and lng:
                    return f"https://www.google.com/maps/search/?api=1&query={lat},{lng}"
                return "-"
            elif tipo in ["file", "document", "video"]:
                return valor.get("url", "-")
            else:
                return json.dumps(valor)
        return valor

    try:
        # 🏷️ Etiquetas
        etiqueta_raw = order_json["order"].get("Etiquetas", "")
        if isinstance(etiqueta_raw, str) and "," in etiqueta_raw:
            etiqueta_ids = [eid.strip() for eid in etiqueta_raw.split(",") if eid.strip()]
            etiqueta_title = ", ".join([get_label_title(eid) for eid in etiqueta_ids])
        else:
            etiqueta_title = get_label_title(etiqueta_raw)

        # 🔺 Prioridade
        priority_map = {0: "Sem prioridade", 1: "Baixa", 2: "Média", 3: "Alta"}
        priority_value = order_json.get("priority_ordering", 0)
        priority_text = priority_map.get(priority_value, "Sem prioridade")

        # 🧩 Campos padrão
        data = {}
        for tag, header in campos_padroes:
            try:
                if tag == "unidade":
                    valor = order_json["location"].get("name", "-")
                elif tag == "cliente":
                    valor = order_json["client"].get("name", "-")
                elif tag == "cliente_numero":
                    valor = order_json["client"].get("number", "-")
                elif tag == "status":
                    valor = order_json["status"].get("status", "-")
                elif tag == "status_code":
                    valor = order_json["status"].get("code", "-")
                else:
                    valor = order_json.get(tag, "-")

                if "time_" in tag:
                    valor = format_date(valor)

                data[header] = valor
            except Exception as e:
                data[header] = f"⚠️ Erro: {e}"

        if "etiquetas" in [t for t, _ in campos_padroes]:
            data["Etiquetas"] = etiqueta_title
        if "priority" in [t for t, _ in campos_padroes]:
            data["Prioridade"] = priority_text

        # Campos variáveis
        for expressao, tipo in campos_variaveis:
            tipo = tipo.lower().strip()
            try:
                valor = order_json["order"].get(expressao, "-")
                valor = handle_media(valor)
                if tipo == "lista de opções":
                    valor = parse_option_list(valor)
                elif tipo == "customizado":
                    valor = eval(expressao)
            except Exception as e:
                valor = f"⚠️ Erro: {e}"

            header = header_report_dict.get(expressao, expressao)
            data[header] = valor

        logging.info(f"✅ Dados extraídos da ordem {order_json.get('id', '-')}")
        return data

    except Exception as e:
        logging.error(f"❌ Erro ao extrair dados da ordem: {e}")
        return {key: "-" for key in data.keys()}
