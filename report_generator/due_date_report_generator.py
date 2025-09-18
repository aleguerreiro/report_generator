
import datetime
import logging
import os
import importlib
from time import time
from typing import Any, Dict, Iterable, List, Optional, Union, Callable
from collections import defaultdict

import requests
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
    if value is None:
        return []
    if isinstance(value, list):
        out: List[str] = []
        for item in value:
            if isinstance(item, str):
                out.extend([p.strip() for p in item.split(",") if p.strip()])
        # dedup preserving order
        seen, res = set(), []
        for x in out:
            if x not in seen:
                seen.add(x)
                res.append(x)
        return res
    if isinstance(value, str):
        out = [p.strip() for p in value.split(",") if p.strip()]
        seen, res = set(), []
        for x in out:
            if x not in seen:
                seen.add(x)
                res.append(x)
        return res
    return []


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


def _get_session(api_token: Optional[str], csrf_token: Optional[str], cookie: Optional[str]) -> requests.Session:
    # Prefer zapform_auth
    za = _import_zapform_auth()
    if za:
        for cand in ("get_authorized_session", "get_session", "session", "login", "create_session", "make_session"):
            fn = getattr(za, cand, None)
            if callable(fn):
                try:
                    return fn()  # assume credenciais já configuradas internamente/envs
                except TypeError:
                    # tenta com envs comuns se a assinatura precisar
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
                        return fn(**kwargs)
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
    return s


# --------------------------------
# Core: query API by status + due_date__lte (server-side filtering)
# --------------------------------

def _iter_orders(session: requests.Session, base_url: str, params: Dict[str, str]) -> List[dict]:
    """Fetches all pages by following 'next' links (DRF-style)."""
    results: List[dict] = []
    url = base_url
    while url:
        r = session.get(url, params=params if url == base_url else None, timeout=float(os.getenv("DUE_DATE_API_PER_REQUEST_TIMEOUT", "10")))
        if r.status_code != 200:
            logging.warning("⚠️ [DUE] HTTP %s em %s", r.status_code, r.url)
            break
        data = r.json()
        results.extend(data.get("results", []))
        next_url = data.get("next")
        if next_url and isinstance(next_url, str) and next_url.strip():
            url = next_url
            params = {}  # already encoded in next_url
        else:
            url = None
    return results


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
    Nova versão simplificada:
      - NÃO usa acumulado/CSV, raw_orders, nem fallback por item.
      - Faz 1 chamada por status diretamente à API com filtros server-side:
            /api/zc/{config_id}/order/?status=<CODE>&due_date__lte=<UTC_ISO>
      - Pagina automaticamente seguindo 'next' até o final.
      - Monta e envia e-mail agrupado por status.
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

    emails_destino = _normalize_list(cfg.get("emails_due_date", cfg.get("emails_due_date:", "")))
    if not emails_destino:
        logging.info("ℹ️ [DUE] sem destinatários 'emails_due_date'")
        return
    config_name = str(cfg.get("config_name", cfg.get("config_name:", ""))).strip()

    # status: aceita na planilha (status_due_date) ou via env DUE_DATE_STATUS
    status_codes = _normalize_list(cfg.get("status_due_date", cfg.get("status_due_date:", ""))) or \
                   _normalize_list(os.getenv("DUE_DATE_STATUS"))
    if not status_codes:
        logging.info("⚠️ [DUE] nenhum status fornecido (status_due_date/DUE_DATE_STATUS). Abortando.")
        return

    # cutoff padrão: hoje 23:59:59Z + offset (ex.: -1 => ontem 23:59:59Z)
    offset_days = int(os.getenv("DUE_CUTOFF_DAYS_OFFSET", "0"))
    utc_now = datetime.datetime.utcnow()
    target_day = (utc_now + datetime.timedelta(days=offset_days)).date()
    cutoff_utc = datetime.datetime(target_day.year, target_day.month, target_day.day, 23, 59, 59, tzinfo=datetime.timezone.utc)
    cutoff_str = cutoff_utc.isoformat().replace("+00:00", "Z")

    session = _get_session(api_token=api_token, csrf_token=csrf_token, cookie=cookie)

    base_url = f"https://api.zapform.com.br/api/zc/{config_id}/order/"
    all_orders: List[dict] = []
    per_status_count = {}

    for code in status_codes:
        params = {"status": code, "due_date__lte": cutoff_str}
        items = _iter_orders(session, base_url, params)
        per_status_count[code] = len(items)
        all_orders.extend(items)

    logging.info("📥 [DUE] fetch concluído | total_itens=%s | por_status=%s", len(all_orders), per_status_count)

    if not all_orders:
        logging.info("ℹ️ [DUE] nenhum card retornado pela API para os filtros fornecidos.")
        return

    # Agrupa por "code - status"
    def _group_key(o: dict) -> str:
        st = (o.get("status") or {})
        code = str(st.get("code") or "").strip()
        name = str(st.get("status") or "").strip()
        if code and name:
            return f"{code} - {name}"
        return name or code or "-"

    groups: Dict[str, List[dict]] = defaultdict(list)
    for o in all_orders:
        groups[_group_key(o)].append(o)

    # ordena cada grupo por due asc
    def _parse_due(o: dict) -> datetime.datetime:
        s = o.get("due_date")
        try:
            # compat: aceita '...Z' ou offset
            s2 = s.replace("Z", "+00:00") if isinstance(s, str) and s.endswith("Z") else s
            return datetime.datetime.fromisoformat(s2)  # pode incluir tzinfo
        except Exception:
            return datetime.datetime.max.replace(tzinfo=datetime.timezone.utc)

    for k in groups:
        groups[k].sort(key=_parse_due)

    # monta e-mail
    linhas: List[str] = []
    linhas.append("Os seguintes cards tiveram o prazo estourado, favor verificar:\n")
    total_itens = sum(len(v) for v in groups.values())
    linhas.append(f"Total de Cards Atrasados: {total_itens}\n")
    
    
    for gname in sorted(groups.keys()):
        itens = groups[gname]
        linhas.append(f"\n— {gname} ({len(itens)})")
        for o in itens:
            due_dt = _parse_due(o)
            try:
                # exibir em DD/MM/YY no tz local se tiver
                due_str = due_dt.astimezone(datetime.timezone.utc).strftime("%d/%m/%y")
            except Exception:
                due_str = "-"
            title = str(o.get("order_summary") or "-").strip()
            link = _build_canonical_link(config_id, str(o.get("id")) if o.get("id") else None)
            linhas.append(f"{due_str} - {title}{(' - ' + link) if link else ''}")

    corpo_email = "\n".join(linhas)

    # assunto
    if subject_base:
        assunto = f"Atenção: Cards com Prazo Vencido | {subject_base}"
    else:
        timestamp = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
        assunto = f"Atenção: Cards com Prazo Vencido | {config_name or 'Config'} Report de {timestamp}"

    if not do_email:
        logging.info("✅ [DUE] fim (sem envio) | total=%s | duracao=%.2fs", total_itens, time() - t0)
        return

    try:
        send_simple_email(
            from_email=from_email,
            to_emails=emails_destino,
            subject=assunto,
            body=corpo_email,
            app_password=app_password,
        )
        logging.info("✅ [DUE] E-mail enviado! | total=%s | por_status=%s | duração=%.2fs",
                     total_itens, per_status_count, time() - t0)
    except Exception:
        logging.exception("❌ [DUE] Falha ao enviar e-mail")
    finally:
        logging.info("🔚 [DUE] Fim | duração total=%.2fs", time() - t0)
