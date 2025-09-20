import datetime
import logging
import os
import importlib
from time import time
from typing import Any, Dict, List, Optional, Union, Tuple
from collections import defaultdict
import html
import re

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd  # kept only because your project likely lists pandas; unused here

from .accumulator import acumular_report_due_dates  # kept for backward-compat imports
try:
    from .email_sender import send_simple_email
except Exception:
    from .email_sender import send_simple_email  # type: ignore


# ------------------------
# Helpers
# ------------------------

def _normalize_list(value: Union[str, List[str], None]) -> List[str]:
    """
    Aceita:
      - string com vírgulas/semicolons
      - lista de strings (cada uma pode ter vírgulas/semicolons)
      - None
    Retorna lista deduplicada, preservando ordem, sem vazios.
    """
    def _explode(s: str) -> List[str]:
        s = s.replace(";", ",")
        return [p.strip() for p in s.split(",") if p and p.strip()]

    if value is None:
        return []
    if isinstance(value, list):
        out: List[str] = []
        for item in value:
            if isinstance(item, str):
                out.extend(_explode(item))
        seen, res = set(), []
        for x in out:
            if x not in seen:
                seen.add(x)
                res.append(x)
        return res
    if isinstance(value, str):
        out = _explode(value)
        seen, res = set(), []
        for x in out:
            if x not in seen:
                seen.add(x)
                res.append(x)
        return res
    return []


def _sanitize_emails(emails: Union[str, List[str], None]) -> List[str]:
    """
    Limpa e valida superficialmente e-mails:
      - divide por vírgula/semicolon quando vier em string
      - remove espaços e quebras de linha/tabs
      - descarta vazios e entradas sem '@'
    (Validação leve para evitar SMTP 5.5.2 por to=[] ou itens vazios.)
    """
    raw = _normalize_list(emails)
    cleaned: List[str] = []
    for e in raw:
        e2 = e.replace("\r", "").replace("\n", "").replace("\t", "").strip()
        if e2 and "@" in e2:
            cleaned.append(e2)
    seen, res = set(), []
    for x in cleaned:
        if x not in seen:
            seen.add(x)
            res.append(x)
    return res


def _extract_config(config_obj: Any) -> Dict[str, Any]:
    if isinstance(config_obj, dict):
        return config_obj
    try:
        if hasattr(config_obj, "columns"):
            colmap = {c.strip().lower(): c for c in config_obj.columns}
            pkey = "parâmetros" if "parâmetros" in colmap else "parametros" if "parametros" in colmap else None
            vkey = "valor" if "valor" in colmap else None
            if pkey and vkey:
                pcol, vcol = colmap[pkey], colmap[vkey]
                params: Dict[str, Any] = {}
                for p, v in zip(config_obj[pcol], config_obj[vcol]):
                    key = str(p).strip()
                    if not key:
                        continue
                    if key.endswith(":"):
                        key = key[:-1]
                    params[key] = v
                return params
            if len(config_obj) > 0:
                return config_obj.iloc[0].to_dict()
    except Exception:
        pass
    return {}


def _build_canonical_link(config_id: str, order_id: Optional[str]) -> str:
    if config_id and order_id:
        return f"https://app.zapform.com.br/c/{config_id}/workflow/{order_id}"
    return ""


# -----------------------------
# zapform_auth first
# -----------------------------

def _import_zapform_auth():
    for name in (".zapform_auth", "zapform_auth"):
        try:
            return importlib.import_module(name, package=__package__)
        except Exception:
            continue
    return None


def _mount_retries(session: requests.Session):
    retry = Retry(
        total=5,
        read=5,
        connect=5,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST", "PUT", "PATCH", "DELETE"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)


def _get_session(api_token: Optional[str], csrf_token: Optional[str], cookie: Optional[str]) -> requests.Session:
    # Prefer zapform_auth
    za = _import_zapform_auth()
    if za:
        for cand in ("get_authorized_session", "get_session", "session", "login", "create_session", "make_session"):
            fn = getattr(za, cand, None)
            if callable(fn):
                try:
                    s = fn()  # assume credenciais já configuradas internamente/envs
                    _mount_retries(s)
                    return s
                except TypeError:
                    kwargs = {}
                    user = os.getenv("ZAPFORM_EMAIL") or os.getenv("ZAPFORM_USER") or os.getenv("ZAPFORM_USERNAME")
                    pwd  = os.getenv("ZAPFORM_PASSWORD") or os.getenv("ZAPFORM_SENHA")
                    tok  = os.getenv("ZAPFORM_API_TOKEN") or api_token
                    csrf = os.getenv("ZAPFORM_CSRF_TOKEN") or csrf_token
                    ck   = os.getenv("ZAPFORM_COOKIE") or cookie
                    if user: kwargs["email"] = user
                    if pwd:  kwargs["password"] = pwd
                    if tok:  kwargs["api_token"] = tok
                    if csrf: kwargs["csrf_token"] = csrf
                    if ck:   kwargs["cookie"] = ck
                    try:
                        s = fn(**kwargs)
                        _mount_retries(s)
                        return s
                    except Exception:
                        continue
                except Exception:
                    continue
    # Legacy: build simple session with token/csrf/cookie
    s = requests.Session()
    headers = {"accept": "application/json"}
    if api_token:
        headers["Authorization"] = f"Token {api_token}"
    if csrf_token:
        headers["X-CSRFToken"] = csrf_token
    if cookie:
        headers["Cookie"] = cookie
    s.headers.update(headers)
    _mount_retries(s)
    return s


# --------------------------------
# Core: query API by status +/or due_date__lte (server-side filtering)
# --------------------------------

def _iter_orders(session: requests.Session, base_url: str, params: Dict[str, str]) -> List[dict]:
    """Fetches all pages by following 'next' links (DRF-style)."""
    results: List[dict] = []
    url = base_url
    while url:
        r = session.get(
            url,
            params=params if url == base_url else None,
            timeout=float(os.getenv("DUE_DATE_API_PER_REQUEST_TIMEOUT", "10")),
        )
        if r.status_code != 200:
            logging.warning("⚠️ [DUE] HTTP %s em %s", r.status_code, r.url)
            break
        data = r.json()
        results.extend(data.get("results", []))
        next_url = data.get("next")
        if next_url and isinstance(next_url, str) and next_url.strip():
            url = next_url
            params = {}  # já vem codado em next_url
        else:
            url = None
    return results


# --------------------------------
# Workflow status filtering (status_type == "O")
# --------------------------------

def _fetch_allowed_status_codes(session: requests.Session, config_id: str) -> List[str]:
    """
    Lê /api/v2/workflow/{config_id}/ e retorna os 'code' dos status cujo status_type == 'O'.
    """
    url = f"https://api.zapform.com.br/api/v2/workflow/{config_id}/"
    try:
        r = session.get(url, timeout=float(os.getenv("DUE_DATE_API_PER_REQUEST_TIMEOUT", "10")))
        if r.status_code != 200:
            logging.warning("⚠️ [DUE] HTTP %s ao buscar workflow %s", r.status_code, config_id)
            return []
        data = r.json() or {}
        out: List[str] = []
        for st in (data.get("status") or []):
            code = str(st.get("code") or "").strip()
            stype = str(st.get("status_type") or "").strip().upper()
            if code and stype == "O":
                out.append(code)
        # dedup preservando ordem
        seen, res = set(), []
        for x in out:
            if x not in seen:
                seen.add(x)
                res.append(x)
        logging.info("✅ [DUE] allowed_status=%s", res)
        return res
    except Exception:
        logging.exception("❌ [DUE] Erro ao buscar workflow %s", config_id)
        return []


def _get_order_status_code(order: dict) -> str:
    st = order.get("status") or {}
    return str(st.get("code") or "").strip()


# --------------------------------
# Location lookup (cached)
# --------------------------------

def _extract_location_id(order: dict) -> Optional[int]:
    """
    Aceita:
      - order["location"] = 123 | "123" | {"id": 123, ...}
    """
    loc = order.get("location")
    if loc is None:
        return None
    if isinstance(loc, dict):
        val = loc.get("id")
    else:
        val = loc
    try:
        return int(val)
    except Exception:
        return None


def _get_location_info(session: requests.Session, location_id: int, *, _cache: Dict[int, Tuple[str, List[str]]]) -> Tuple[str, List[str]]:
    """
    Retorna (name, addresses[]) da location, com cache.
    GET /api/v2/location/{id}/ -> lê 'name' e 'address' (pode ter vários e-mails separados por vírgula/semicolon).
    Em erro, retorna ("", []).
    """
    if location_id in _cache:
        return _cache[location_id]

    base = "https://api.zapform.com.br/api/v2/location/"
    url = f"{base}{location_id}/"
    try:
        r = session.get(url, timeout=float(os.getenv("DUE_DATE_API_PER_REQUEST_TIMEOUT", "10")))
        if r.status_code != 200:
            logging.info("ℹ️ [DUE] HTTP %s ao buscar location %s", r.status_code, location_id)
            _cache[location_id] = ("", [])
            return _cache[location_id]
        data = r.json() or {}
        name = str(data.get("name") or "").strip()
        addr_raw = data.get("address")
        addrs = _sanitize_emails(addr_raw)
        _cache[location_id] = (name, addrs)
        return _cache[location_id]
    except Exception:
        logging.exception("❌ [DUE] Erro ao buscar location %s", location_id)
        _cache[location_id] = ("", [])
        return _cache[location_id]


# --------------------------------
# Email bodies (texto + HTML bonitão)
# --------------------------------

def _safe(o, *path, default="-"):
    cur = o
    for p in path:
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return default
    return cur if cur not in (None, "", []) else default


def _order_id_from_any(o: dict) -> Optional[int]:
    for k in ("id", "order_id", "pk", "orderId"):
        if k in o and o[k]:
            try:
                return int(str(o[k]).strip())
            except Exception:
                pass
    if isinstance(o.get("order"), dict):
        for k in ("id", "order_id", "pk"):
            v = o["order"].get(k)
            if v:
                try:
                    return int(str(v).strip())
                except Exception:
                    pass
    return None


def _parse_due_dt(o: dict) -> datetime.datetime:
    s = o.get("due_date")
    try:
        s2 = s.replace("Z", "+00:00") if isinstance(s, str) and s.endswith("Z") else s
        return datetime.datetime.fromisoformat(s2)
    except Exception:
        return datetime.datetime.max.replace(tzinfo=datetime.timezone.utc)


def _group_key(o: dict) -> str:
    st = (o.get("status") or {})
    code = str(st.get("code") or "").strip()
    name = str(st.get("status") or "").strip()
    return f"{code} - {name}" if code and name else (name or code or "-")


def _build_email_bodies_for_orders(orders: List[dict], config_id: str) -> Tuple[str, str]:
    """
    Retorna (body_text, body_html) com colunas:
    Prazo | Destaque | Cliente | Número | Link do Card

    Regras:
      - Cliente vazio/null -> "-"
      - Número 0/null/vazio -> "-"
      - Número formatado como +DDI (DD) XXXXX-XXXX quando possível
      - Larguras: Prazo=72px, Cliente=150px, Número=150px, Link=60px, Destaque=min 150px e cresce (auto)
    """
    import re
    import html as _h
    import datetime as _dt
    from collections import defaultdict
    from typing import Any, Dict, List, Optional, Tuple

    # ---------- helpers ----------
    def _clean_client(v: Any) -> str:
        s = "" if v is None else str(v).strip()
        return "-" if s == "" or s.lower() in {"null", "none"} else s

    def _format_br_phone(digits: str) -> str:
        if len(digits) < 10:
            return "-"
        ddi = digits[0:2]
        ddd = digits[2:4]
        rest = digits[4:]
        if len(rest) == 9:
            return f"+{ddi} ({ddd}) {rest[:5]}-{rest[5:]}"
        elif len(rest) == 8:
            return f"+{ddi} ({ddd}) {rest[:4]}-{rest[4:]}"
        else:
            return f"+{ddi} ({ddd}) {rest[:-4]}-{rest[-4:]}"

    def _clean_number(v: Any) -> str:
        if v is None:
            return "-"
        s = str(v).strip()
        if s == "" or s.lower() in {"null", "none"} or s == "0":
            return "-"
        digits = re.sub(r"\D", "", s)
        return _format_br_phone(digits) if digits else "-"

    def _one_line(s: str) -> str:
        return " ".join((s or "").replace("\r", "").replace("\n", " ").split())

    def _parse_due_dt(o: dict) -> _dt.datetime:
        s = o.get("due_date")
        try:
            s2 = s.replace("Z", "+00:00") if isinstance(s, str) and s.endswith("Z") else s
            return _dt.datetime.fromisoformat(s2)
        except Exception:
            return _dt.datetime.max.replace(tzinfo=_dt.timezone.utc)

    def _order_id_from_any(o: dict) -> Optional[int]:
        for k in ("id", "order_id", "pk", "orderId"):
            if k in o and o[k]:
                try:
                    return int(str(o[k]).strip())
                except Exception:
                    pass
        if isinstance(o.get("order"), dict):
            for k in ("id", "order_id", "pk"):
                v = o["order"].get(k)
                if v:
                    try:
                        return int(str(v).strip())
                    except Exception:
                        pass
        return None

    # ---------- agrupamento e ordenação ----------
    groups: Dict[str, List[dict]] = defaultdict(list)
    for o in orders:
        st = (o.get("status") or {})
        code = str(st.get("code") or "").strip()
        name = str(st.get("status") or "").strip()
        gkey = f"{code} - {name}" if code and name else (name or code or "-")
        groups[gkey].append(o)
    for k in groups:
        groups[k].sort(key=_parse_due_dt)

    # ---------- texto (fallback) ----------
    lines: List[str] = []
    total_itens = sum(len(v) for v in groups.values())
    lines.append("Os seguintes cards tiveram o prazo estourado, favor verificar:\n")
    lines.append(f"Total de Cards Atrasados: {total_itens}\n")
    header_txt = "Prazo | Destaque | Cliente | Número | Link do Card"
    sep_txt = "-" * len(header_txt)

    for gname in sorted(groups.keys()):
        itens = groups[gname]
        lines.append(f"\n— {gname} ({len(itens)})")
        lines.append(header_txt)
        lines.append(sep_txt)
        for o in itens:
            try:
                due_str = _parse_due_dt(o).astimezone(_dt.timezone.utc).strftime("%d/%m/%y")
            except Exception:
                due_str = "-"
            title_single = _one_line(str(o.get("order_summary") or ""))
            cli = _clean_client((o.get("client") or {}).get("name") if isinstance(o.get("client"), dict) else o.get("client"))
            num = _clean_number((o.get("client") or {}).get("number") if isinstance(o.get("client"), dict) else None)
            oid = _order_id_from_any(o)
            link = f"https://app.zapform.com.br/c/{config_id}/workflow/{oid}" if (config_id and oid) else "-"
            lines.append(f"{due_str} | {title_single} | {cli} | {num} | {link}")

    body_text = "\n".join(lines)

    # ---------- HTML (principal) ----------
    parts: List[str] = []
    parts.append('<div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;font-size:14px;line-height:1.4;">')
    parts.append("<p>Os seguintes cards tiveram o prazo estourado, favor verificar:</p>")
    parts.append(f"<p><strong>Total de Cards Atrasados:</strong> {total_itens}</p>")

    # Estilos base
    table_style = (
        "border-collapse:collapse;"
        "width:100%;"
        "font-size:13px;"
        "table-layout:fixed;"
    )
    th_style = "text-align:left;border:1px solid #ddd;padding:6px;background:#f5f5f5;"
    td_base  = "border:1px solid #ddd;padding:6px;vertical-align:top;"

    # Overrides por coluna
    td_prazo   = td_base + "white-space:nowrap;text-align:center;"
    td_title   = td_base + "word-break:break-word;overflow-wrap:anywhere;white-space:normal;"
    td_client  = td_base + "word-break:break-word;overflow-wrap:anywhere;white-space:normal;"
    td_number  = td_base + "white-space:nowrap;"
    td_link    = td_base + "white-space:nowrap;text-align:center;"

    # Colunas: Prazo 72 | Destaque min 150 (auto) | Cliente 150 | Número 150 | Link 60
    colgroup = (
        "<colgroup>"
        '<col style="min-width:72px;width:72px;">'          # Prazo (dd/mm/yy)
        '<col style="min-width:150px;width:auto;">'         # Destaque (cresce, min 150)
        '<col style="min-width:150px;width:150px;">'        # Cliente
        '<col style="min-width:150px;width:150px;">'        # Número
        '<col style="min-width:60px;width:60px;">'          # Link
        "</colgroup>"
    )

    for gname in sorted(groups.keys()):
        itens = groups[gname]
        parts.append(f'<h3 style="margin:16px 0 6px;">{_h.escape(gname)} ({len(itens)})</h3>')
        parts.append(f'<table style="{table_style}">{colgroup}')
        parts.append(
            "<thead><tr>"
            f'<th style="{th_style}">Prazo</th>'
            f'<th style="{th_style}">Destaque</th>'
            f'<th style="{th_style}">Cliente</th>'
            f'<th style="{th_style}">Número</th>'
            f'<th style="{th_style}">Link do Card</th>'
            "</tr></thead><tbody>"
        )
        for o in itens:
            try:
                due_str = _parse_due_dt(o).astimezone(_dt.timezone.utc).strftime("%d/%m/%y")
            except Exception:
                due_str = "-"

            title_single = _one_line(str(o.get("order_summary") or ""))
            cli = _clean_client((o.get("client") or {}).get("name") if isinstance(o.get("client"), dict) else o.get("client"))
            num = _clean_number((o.get("client") or {}).get("number") if isinstance(o.get("client"), dict) else None)

            oid = _order_id_from_any(o)
            link = f"https://app.zapform.com.br/c/{config_id}/workflow/{oid}" if (config_id and oid) else ""
            link_html = f'<a href="{_h.escape(link)}">abrir</a>' if link else "-"

            parts.append(
                "<tr>"
                f'<td style="{td_prazo}">{_h.escape(due_str)}</td>'
                f'<td style="{td_title}">{_h.escape(title_single)}</td>'
                f'<td style="{td_client}">{_h.escape(cli)}</td>'
                f'<td style="{td_number}">{_h.escape(num)}</td>'
                f'<td style="{td_link}">{link_html}</td>'
                "</tr>"
            )
        parts.append("</tbody></table>")
    parts.append("</div>")

    body_html = "".join(parts)
    return body_text, body_html

# --------------------------------
# Principal
# --------------------------------

def generate_and_send_due_date_report(
    config,
    cards,  # ignorado – mantido por compatibilidade
    from_email,
    app_password,
    *,
    config_id: str = "",
    subject_base: Optional[str] = None,
    # auth (usado só pra fallback; preferimos zapform_auth)
    api_token: Optional[str] = None,
    csrf_token: Optional[str] = None,
    cookie: Optional[str] = None,
    do_email: bool = True,
    **kwargs,
):
    """
    Dois tipos de report:
      - type_report_due_date = 'emails_due_date' (padrão): um e-mail p/ lista padrão
      - type_report_due_date = 'location'       : 1 e-mail por location (address da API)
    """
    t0 = time()
    cfg = _extract_config(config)
    if not cfg:
        logging.info("⚠️ [DUE] config inválida, abortando.")
        return

    raw_flag = str(cfg.get("send_report_due_date", cfg.get("send_report_due_date:", ""))).strip().lower()
    if raw_flag not in {"yes", "sim", "y", "true", "1"}:
        logging.info("ℹ️ [DUE] envio desabilitado | send_report_due_date='%s'", raw_flag)
        return

    # Tipo do report
    report_type = str(cfg.get("type_report_due_date", cfg.get("type_report_due_date:", "emails_due_date"))).strip().lower()
    if report_type not in {"emails_due_date", "location"}:
        logging.info("ℹ️ [DUE] tipo desconhecido '%s' (usando emails_due_date)", report_type)
        report_type = "emails_due_date"

    # E-mails padrão (usados apenas no tipo emails_due_date)
    emails_destino = _sanitize_emails(cfg.get("emails_due_date", cfg.get("emails_due_date:", "")))
    config_name = str(cfg.get("config_name", cfg.get("config_name:", ""))).strip()

    # status: planilha ou env DUE_DATE_STATUS
    status_codes_from_cfg = _normalize_list(cfg.get("status_due_date", cfg.get("status_due_date:", ""))) or \
                            _normalize_list(os.getenv("DUE_DATE_STATUS"))

    # cutoff: hoje 23:59:59Z + offset (ex.: -1 => ontem 23:59:59Z) — ainda usado no dashboard/planilha
    offset_days = int(os.getenv("DUE_CUTOFF_DAYS_OFFSET", "0"))
    utc_now = datetime.datetime.utcnow()
    target_day = (utc_now + datetime.timedelta(days=offset_days)).date()
    cutoff_utc = datetime.datetime(target_day.year, target_day.month, target_day.day, 23, 59, 59, tzinfo=datetime.timezone.utc)
    cutoff_str = cutoff_utc.isoformat().replace("+00:00", "Z")

    session = _get_session(api_token=api_token, csrf_token=csrf_token, cookie=cookie)

    base_url = f"https://api.zapform.com.br/api/zc/{config_id}/order/"

    # limitar aos status cujo status_type == "O"
    allowed_status = _fetch_allowed_status_codes(session, config_id)

    # Decide lista final de status a consultar server-side
    if status_codes_from_cfg:
        final_status_list = [s for s in status_codes_from_cfg if (not allowed_status or s in allowed_status)]
        if not final_status_list and allowed_status:
            logging.info("ℹ️ [DUE] Nenhum dos status configurados está permitido (status_type=='O'). Abortando busca.")
            return
    else:
        final_status_list = allowed_status  # pode vir vazio -> cai no ramo "date only"

    all_orders: List[dict] = []
    per_status_count: Dict[str, int] = {}

    if final_status_list:
        for code in final_status_list:
            params = {"status": code, "due_date__lte": cutoff_str}
            items = _iter_orders(session, base_url, params)
            per_status_count[code] = len(items)
            all_orders.extend(items)
    else:
        params = {"due_date__lte": cutoff_str}
        items = _iter_orders(session, base_url, params)
        per_status_count["*DATE_ONLY*"] = len(items)
        if allowed_status:
            items = [o for o in items if _get_order_status_code(o) in allowed_status]
        all_orders.extend(items)

    logging.info("📥 [DUE] fetch concluído | total_itens=%s | por_status=%s", len(all_orders), per_status_count)

    if not all_orders:
        logging.info("ℹ️ [DUE] nenhum card retornado pela API para os filtros fornecidos.")
        return

    # assunto base
    if subject_base:
        subject_prefix = f"Atenção: Cards com Prazo Vencido | {subject_base}"
    else:
        timestamp = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
        subject_prefix = f"Atenção: Cards com Prazo Vencido | {config_name or 'Config'} Report de {timestamp}"

    # ------------------------------
    # Tipo 1: emails_due_date
    # ------------------------------
    if report_type == "emails_due_date":
        if not emails_destino:
            logging.info("ℹ️ [DUE] sem destinatários 'emails_due_date' (tipo emails_due_date). Abortando envio.")
            return

        body_text, body_html = _build_email_bodies_for_orders(all_orders, config_id)
        if not do_email:
            logging.info("✅ [DUE] fim (sem envio) | total=%s | duração=%.2fs", len(all_orders), time() - t0)
            return

        try:
            send_simple_email(
                from_email=from_email,
                to_emails=emails_destino,
                subject=subject_prefix,
                body=body_text,        # fallback texto
                body_html=body_html,   # versão HTML rica
                app_password=app_password,
            )
            logging.info("✅ [DUE] E-mail enviado! | total=%s | por_status=%s | duração=%.2fs",
                         len(all_orders), per_status_count, time() - t0)
        except Exception:
            logging.exception("❌ [DUE] Falha ao enviar e-mail (emails_due_date)")
        finally:
            logging.info("🔚 [DUE] Fim | duração total=%.2fs", time() - t0)
        return

    # ------------------------------
    # Tipo 2: location (um e-mail por location)
    # ------------------------------
    groups_by_loc: Dict[int, List[dict]] = defaultdict(list)
    for o in all_orders:
        loc_id = _extract_location_id(o)
        if loc_id is not None:
            groups_by_loc[loc_id].append(o)
        else:
            logging.debug("ℹ️ [DUE] ordem %s sem location definida; ignorando no modo 'location'", o.get("id"))

    if not groups_by_loc:
        logging.info("ℹ️ [DUE] nenhuma ordem com location encontrada.")
        return

    loc_cache: Dict[int, Tuple[str, List[str]]] = {}
    total_sent = 0
    total_skipped_no_email = 0

    for loc_id, orders in groups_by_loc.items():
        name, addr_list = _get_location_info(session, loc_id, _cache=loc_cache)
        if not addr_list:
            total_skipped_no_email += 1
            logging.info("ℹ️ [DUE] location %s ('%s') sem e-mail 'address'; pulando envio.", loc_id, name)
            continue

        body_text, body_html = _build_email_bodies_for_orders(orders, config_id)
        subject = f"{subject_prefix} | Responsável: {name or loc_id}"

        if not do_email:
            logging.info("✅ [DUE] (sem envio) location=%s '%s' | itens=%s | destinatarios=%s",
                         loc_id, name, len(orders), addr_list)
            total_sent += 1
            continue

        try:
            send_simple_email(
                from_email=from_email,
                to_emails=addr_list,   # suporta múltiplos e-mails
                subject=subject,
                body=body_text,        # fallback texto
                body_html=body_html,   # versão HTML rica
                app_password=app_password,
            )
            total_sent += 1
            logging.info("✅ [DUE] Enviado p/ location=%s '%s' <%s> | itens=%s",
                         loc_id, name, ",".join(addr_list), len(orders))
        except Exception:
            logging.exception("❌ [DUE] Falha ao enviar p/ location=%s '%s' <%s>", loc_id, name, ",".join(addr_list))

    logging.info("🔚 [DUE] Fim | sent=%s | skipped_no_email=%s | duração total=%.2fs",
                 total_sent, total_skipped_no_email, time() - t0)
