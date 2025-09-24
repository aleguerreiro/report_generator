# extractor.py

import json
import logging
from .data_utils import format_date, parse_option_list


def extract_data(order_json, campos_variaveis, etiquetas_dict, header_report_dict, campos_padroes):
    """
    Mantém a assinatura e o comportamento geral do extractor original.
    Ajustes mínimos e resilientes:
      - Normalização leve de order_json (se vier tupla (payload, status))
      - Normalização de etiquetas_dict (se vier tupla (payload, status) ou lista/tupla de pares)
      - Etiquetas: aceita string CSV, lista de IDs, lista de dicts, único ID/dict
      - Tolerância a campos_variaveis como (expressao, tipo) ou dict equivalente
      - Avaliação 'customizado' com ambiente restrito
      - Retorno estável mesmo em caso de erro
    """

    # ---------------------
    # Helpers mínimos
    # ---------------------
    def _ensure_order_dict(x):
        # Se alguma camada passou a retornar (payload, status), usa o payload quando for dict
        if isinstance(x, tuple) and x and isinstance(x[0], dict):
            return x[0]
        return x

    def _to_str_id(x):
        # Converte ID/code de etiqueta para string chaveável no dict de lookup
        try:
            return str(x).strip()
        except Exception:
            return None

    def _build_label_lookup(etiquetas):
        """
        Cria um lookup tolerante a:
          - dict simples {id: titulo}
          - lista de dicts com chaves 'id'/'code' -> 'title'/'name'
        Mantém o comportamento antigo se etiquetas_dict já for {id: titulo}.
        """
        if isinstance(etiquetas, dict):
            # já está no formato esperado (id -> título)
            return { _to_str_id(k): (v if isinstance(v, str) else str(v)) for k, v in etiquetas.items() }

        # fallback: se alguém passar lista de dicts de etiquetas
        lookup = {}
        try:
            for item in (etiquetas or []):
                if isinstance(item, dict):
                    k = _to_str_id(item.get("id") or item.get("code"))
                    v = item.get("title") or item.get("name") or "-"
                    if k:
                        lookup[k] = v
        except Exception:
            pass
        return lookup

    def get_label_title(label_id):
        sid = _to_str_id(label_id)
        if sid is None:
            return "-"
        return etiquetas_lookup.get(sid, "-")

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
                # mantém compatibilidade anterior
                return json.dumps(valor, ensure_ascii=False)
        return valor

    def _normalize_variavel(field):
        """
        Aceita tupla (expressao, tipo) como antes, ou dict semelhante:
          {'expressao': '...', 'tipo': '...'} ou {'tag_form': '...', 'type': '...'}
        Retorna (expressao, tipo) normalizado.
        """
        if isinstance(field, (list, tuple)) and len(field) >= 2:
            return field[0], field[1]
        if isinstance(field, dict):
            expressao = field.get("expressao") or field.get("tag_form") or field.get("expr") or field.get("field_name")
            tipo = field.get("tipo") or field.get("type") or field.get("Tipo")
            return expressao, tipo
        # último recurso
        return field, None

    # ---------------------
    # Início do processamento
    # ---------------------
    data = {}  # garante existência para o retorno em caso de erro
    try:
        # Normaliza order_json
        order_json = _ensure_order_dict(order_json)

        # Normaliza etiquetas_dict para garantir dict {id: titulo}
        # - Se vier como (payload, status), usa só o payload
        if isinstance(etiquetas_dict, tuple) and etiquetas_dict and isinstance(etiquetas_dict[0], (dict, list, tuple)):
            etiquetas_dict = etiquetas_dict[0]
        # - Se vier como lista/tupla de pares [(id, title), ...], converte para dict
        if not isinstance(etiquetas_dict, dict):
            try:
                etiquetas_dict = dict(etiquetas_dict)
            except Exception:
                etiquetas_dict = {}

        # Lookup de etiquetas robusto (id -> título)
        etiquetas_lookup = _build_label_lookup(etiquetas_dict)

        # 🏷️ Etiquetas (aceita diversos formatos)
        # - String CSV: "abc, def"
        # - Lista de IDs: ["abc","def"]
        # - Lista de dicts: [{"id":"abc","title":"X"}, ...]
        # - Único ID/dict
        etiqueta_raw = order_json.get("order", {}).get("Etiquetas", "")

        etiqueta_titles = []

        if isinstance(etiqueta_raw, str):
            parts = [p.strip() for p in etiqueta_raw.split(",")] if ("," in etiqueta_raw) else [etiqueta_raw.strip()]
            etiqueta_titles = [get_label_title(p) for p in parts if p]
        elif isinstance(etiqueta_raw, (list, tuple)):
            for item in etiqueta_raw:
                if isinstance(item, dict):
                    # tenta por id/code; se já vier com title, usa-o direto
                    tid = item.get("id") or item.get("code")
                    title = item.get("title") or get_label_title(tid)
                    etiqueta_titles.append(title or "-")
                else:
                    etiqueta_titles.append(get_label_title(item))
        elif isinstance(etiqueta_raw, dict):
            tid = etiqueta_raw.get("id") or etiqueta_raw.get("code")
            title = etiqueta_raw.get("title") or get_label_title(tid)
            etiqueta_titles.append(title or "-")
        else:
            etiqueta_titles.append(get_label_title(etiqueta_raw))

        etiqueta_title = ", ".join([t for t in etiqueta_titles if t]) if etiqueta_titles else "-"

        # 🔺 Prioridade
        priority_map = {0: "Sem prioridade", 1: "Baixa", 2: "Média", 3: "Alta"}
        priority_value = order_json.get("priority_ordering", 0)
        priority_text = priority_map.get(priority_value, "Sem prioridade")

        # 🧩 Campos padrão (mantido como no seu código)
        tags_padrao = [t for t, _ in campos_padroes]  # para checagens abaixo
        for tag, header in campos_padroes:
            try:
                if tag == "unidade":
                    valor = order_json.get("location", {}).get("name", "-")
                elif tag == "cliente":
                    valor = order_json.get("client", {}).get("name", "-")
                elif tag == "cliente_numero":
                    valor = order_json.get("client", {}).get("number", "-")
                elif tag == "status":
                    valor = order_json.get("status", {}).get("status", "-")
                elif tag == "status_code":
                    valor = order_json.get("status", {}).get("code", "-")
                else:
                    valor = order_json.get(tag, "-")

                if isinstance(tag, str) and "time_" in tag:
                    valor = format_date(valor)

                data[header] = valor
            except Exception as e:
                data[header] = f"⚠️ Erro: {e}"

        if "etiquetas" in tags_padrao:
            data["Etiquetas"] = etiqueta_title
        if "priority" in tags_padrao:
            data["Prioridade"] = priority_text

        # 🧩 Campos variáveis (mantém (expressao, tipo); aceita dict equivalente)
        for raw_field in campos_variaveis:
            expressao, tipo = _normalize_variavel(raw_field)
            if not expressao:
                # ignora linhas vazias ou malformadas
                continue
            tipo = (tipo or "").lower().strip()

            try:
                # padrão antigo: buscar direto na ordem
                valor = order_json.get("order", {}).get(expressao, "-")
                valor = handle_media(valor)

                if tipo == "lista de opções":
                    valor = parse_option_list(valor)
                elif tipo == "customizado":
                    # Ambiente restrito para eval (mínimo necessário)
                    env = {
                        "order_json": order_json,
                        "order": order_json.get("order", {}),
                        "get": dict.get,   # para usar em expressões tipo foo.get("bar", "-")
                        "format_date": format_date,
                        "parse_option_list": parse_option_list,
                        "json": json,
                    }
                    valor = eval(expressao, {"__builtins__": {}}, env)

            except Exception as e:
                valor = f"⚠️ Erro: {e}"

            header = header_report_dict.get(expressao, expressao)
            data[header] = valor

        logging.info(f"✅ Dados extraídos da ordem {order_json.get('id', '-')}")
        return data

    except Exception as e:
        logging.error(f"❌ Erro ao extrair dados da ordem: {e}")
        # garante retorno com as mesmas colunas já populadas até aqui (se houver)
        return {k: data.get(k, "-") for k in data.keys() or []}
