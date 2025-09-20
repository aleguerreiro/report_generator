import os
from datetime import datetime
import re
import unicodedata
from collections import Counter

import pandas as pd
import xlsxwriter


# =========================
# Helpers p/ mapear Carteira
# =========================
CARTEIRA_RE = re.compile(r"carteira\s*[-:]?\s*(\d{1,3})", re.I)

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s.strip().lower())
    return s

def _slug_sheet(name: str, used: set) -> str:
    """Sanitiza para nome de planilha (<=31 chars, sem []:*?/\\). Dedup se já usado."""
    s = re.sub(r"[\[\]\:\*\?\/\\]", " ", str(name or "")).strip()
    s = re.sub(r"\s+", " ", s)
    if not s:
        s = "Sheet"
    s = s[:31]
    base = s
    i = 2
    while s in used:
        suffix = f"_{i}"
        s = (base[:31 - len(suffix)] + suffix) if len(base) + len(suffix) > 31 else base + suffix
        i += 1
    used.add(s)
    return s

DEPARA_NOME_PARA_CARTEIRA = {
    "carteira40": "carteira40",
    "jairo": "carteira82",
    "regina": "carteira31",
    "ivan": "carteira26",
    "timbo-carteira 07": "carteira07",
    "timbo-carteira 35": "carteira35",
    "sharlene": "carteira33",
    "jessica": "carteira36",
    "gilmara": "carteira27",
    "dennis": "carteira39",
    "filial gaspar-carteira 44 - eder": "carteira44",
    "caciane": "carteira07",
    "filial blumenau-carteira 41 - felipe": "carteira41",
    "filial gaspar-carteira 54 - eduardo": "carteira54",
    "filial blumenau-carteira 39 - dennis": "carteira39",
    "filial gaspar-carteira 50 - jhonatan": "carteira50",
    "filial digital-carteira 03 - natalia": "carteira03",
    "arthur": "carteira38",
    "natalia": "carteira03",
    "filial blumenau-carteira 39": "carteira39",
    "digital-carteira 82 - jairo": "carteira82",
    "rejane": "carteira32",
    "michele/caciane": "carteira07",
    "raquel": "carteira83",
    "rio do sul-carteira 26": "carteira26",
    "adriana": "carteira08",
    "digital-carteira 82": "carteira82",
    "filial digital-carteira 82": "carteira82",
    "digital-carteira 82- jairo": "carteira82",
    "- hilta": "carteira14",
    "hilta": "carteira14",
    "- - jairo": "carteira82",
    "rio do sul-carteira 83": "carteira83",
    "digital-carteira 82 - jairo": "carteira82",
    "digital-carteira 82 -* jairo": "carteira82",
    "digital-carteira 82 -  aline": "carteira82",
    "rio do sul-carteira 31": "carteira31",
    "indaial-carteira 32": "carteira32",
    "filial digital-carteira 03": "carteira03",
    "filial gaspar-carteira 50": "carteira50",
    "filial blumenau-carteira 38 - pierre": "carteira38",
    "- gilmara": "carteira27",
}

def to_carteira(valor: str) -> str:
    """
    1) se houver 'carteira NN' no texto, usa essa NN
    2) senão, aplica de-para normalizado
    3) senão, devolve o texto original (para identificar casos novos)
    """
    txt = str(valor or "")
    m = CARTEIRA_RE.search(txt)
    if m:
        return f"carteira{m.group(1).zfill(2)}"
    key = _norm(txt)
    if key in DEPARA_NOME_PARA_CARTEIRA:
        return DEPARA_NOME_PARA_CARTEIRA[key]
    last = key.split()[-1] if key else ""
    return DEPARA_NOME_PARA_CARTEIRA.get(last, txt)


# =========================
# Dashboard
# =========================
def gerar_dashboard_excel(df_final: pd.DataFrame, caminho_excel: str):
    """
    Cria:
      - Dashboard (GERAL) + Dados (GERAL)
      - Para cada "Responsável": Dados_<slug> + Dash_<slug>
    """
    if df_final is None or df_final.empty:
        print("❌ DataFrame vazio. Nenhum dashboard gerado.")
        return

    df = df_final.copy()
    nome_arquivo = os.path.basename(caminho_excel)
    print(f"📊 Adicionando dashboards ao arquivo: {nome_arquivo}")

    caminho_temporario = caminho_excel.replace(".xlsx", "_tmp.xlsx")

    # ---- localizar colunas principais
    def _find_col(possiveis):
        for c in df.columns:
            if _norm(c) in [_norm(p) for p in possiveis]:
                return c
        return None

    col_responsavel = _find_col(["Responsável", "Responsavel"])
    col_status      = _find_col(["Status"])
    col_data_reg    = _find_col(["Data Registro", "Data de Registro", "Data de registro"])
    col_cidade      = _find_col(["Cidade"])
    col_produto     = _find_col(["Produto"])
    col_carteira_bruta = _find_col([">>Carteira:", "Carteira"])

    # limpeza leve do texto do responsável (remove "Agente:" etc.)
    def _pre_clean_responsavel(txt: str) -> str:
        s = str(txt or "")
        s = re.sub(r"^\s*agente\s*:\s*", "", s, flags=re.I)
        return s.strip()

    # Coluna Carteira derivada a partir do Responsável (fallback para coluna existente)
    if col_responsavel:
        df["__Responsavel__"] = df[col_responsavel].map(_pre_clean_responsavel)
        df["Carteira"] = df["__Responsavel__"].apply(to_carteira).astype(str)
    elif col_carteira_bruta:
        df["__Responsavel__"] = df[col_carteira_bruta].astype(str)
        df["Carteira"] = df[col_carteira_bruta].apply(to_carteira).astype(str)
    else:
        df["__Responsavel__"] = ""
        df["Carteira"] = ""

    df["Carteira"] = df["Carteira"].str.replace(r"carteira\s+(\d+)", r"carteira\1", regex=True)

    # Flags
    has_status = bool(col_status)
    has_data_reg = bool(col_data_reg)
    has_cidade = bool(col_cidade)
    has_produto = bool(col_produto)

    # ===== Função: escreve 1 dashboard no worksheet dado =====
    def _render_one_dashboard(writer, workbook, ws_name: str, df_base: pd.DataFrame, dados_sheet: str):
        ws = workbook.add_worksheet(ws_name)
        writer.sheets[ws_name] = ws
        df_base.to_excel(writer, sheet_name=dados_sheet, index=False)

        # formats
        bold              = workbook.add_format({'bold': True})
        title_format      = workbook.add_format({'bold': True, 'font_size': 14})
        header_format     = workbook.add_format({'bold': True, 'bg_color': '#DDEBF7'})
        section_separator = workbook.add_format({'bold': True, 'font_color': '#808080'})

        # larguras & layout
        ws.set_column('A:A', 50)
        ws.set_column('B:B', 12)

        # período
        periodo_texto = "Período não disponível"
        if has_data_reg and col_data_reg in df_base.columns:
            dtmp = pd.to_datetime(df_base[col_data_reg], errors='coerce', dayfirst=True)
            if dtmp.notna().any():
                data_min = dtmp.min()
                data_max = dtmp.max()
                periodo_texto = f"Período considerado: {data_min.strftime('%d/%m/%Y')} a {data_max.strftime('%d/%m/%Y')}"

        # header + índice
        ws.write('A1', '📊 DASHBOARD DE ANÁLISE DE CARDS', title_format)
        ws.write('A2', periodo_texto)
        ws.write('A3', '📋 ÍNDICE DO DASHBOARD', title_format)
        indice = [
            'A) Indicadores Gerais',
            'B) Distribuição por Status',
            'C) Resumo de Cadastros por Carteira',
            'D) Volume por Carteira (exclui duplicado/cancelado)',
            'E) Distribuição por Cidade',
            'F) Distribuição por Produto'
        ]
        for i, item in enumerate(indice):
            ws.write(f'A{i+4}', item)

        linha = len(indice) + 6
        def sep():
            nonlocal linha
            ws.write(f'A{linha}', '', section_separator)
            linha += 1

        # ---------------- A) INDICADORES
        ws.write(f'A{linha}', 'A) Indicadores Gerais', title_format)
        linha += 1

        hoje = datetime.today()
        semana_passada = hoje - pd.Timedelta(days=7)

        total = len(df_base)
        total_semana = 0
        if has_data_reg and col_data_reg in df_base.columns:
            dtmp = pd.to_datetime(df_base[col_data_reg], errors='coerce', dayfirst=True)
            total_semana = int((dtmp >= semana_passada).sum())

        aprovados    = df_base[col_status].astype(str).str.contains("Aprovado",   case=False, na=False).sum() if has_status else 0
        reprovados   = df_base[col_status].astype(str).str.contains("Reprovado",  case=False, na=False).sum() if has_status else 0
        desistencias = df_base[col_status].astype(str).str.contains("Desist",     case=False, na=False).sum() if has_status else 0
        pendentes    = max(total - (aprovados + reprovados + desistencias), 0)

        def pct(n): return round((n/total*100), 1) if total > 0 else 0

        indicadores = [
            ["Indicador", "Valor"],
            ["Total de cadastros na semana", total_semana],
            ["Total geral de cadastros", total],
            ["% Aprovados", pct(aprovados)],
            ["% Reprovados", pct(reprovados)],
            ["% Desistências", pct(desistencias)],
            ["% Pendentes", pct(pendentes)],
        ]
        for i, row in enumerate(indicadores):
            ws.write_row(linha - 1 + i, 0, row, header_format if i == 0 else None)

        # Gráfico de percentuais
        chart_ind = workbook.add_chart({'type': 'column'})
        cat_first = (linha - 1) + 3
        cat_last  = (linha - 1) + 6
        chart_ind.add_series({
            'name': 'Indicadores %',
            'categories': [ws_name, cat_first, 0, cat_last, 0],
            'values':     [ws_name, cat_first, 1, cat_last, 1],
        })
        chart_ind.set_title({'name': 'Percentuais das Ordens'})
        chart_ind.set_x_axis({'name': 'Categoria'})
        chart_ind.set_y_axis({'name': '%', 'major_gridlines': {'visible': False}})
        ws.insert_chart('E6', chart_ind)  # posição fixa para evitar sobreposição

        linha += len(indicadores) + 1
        sep()

        # ---------------- B) STATUS
        ws.write(f'A{linha}', 'B) Distribuição por Status', title_format)
        linha += 1

        start_status_tbl = linha
        if has_status and col_status in df_base.columns:
            status_count = df_base[col_status].astype(str).value_counts().reset_index()
            status_count.columns = ['Status', 'Quantidade']

            ws.write_row(linha - 1, 0, status_count.columns.tolist(), header_format)
            for i, r in enumerate(status_count.itertuples(index=False), start=linha):
                ws.write_row(i - 1, 0, [r.Status, int(r.Quantidade)])

            start = linha
            end   = linha + len(status_count) - 1
            chart_status = workbook.add_chart({'type': 'pie'})
            chart_status.add_series({
                'name':       'Distribuição por Status',
                'categories': [ws_name, start - 1, 0, end - 1, 0],
                'values':     [ws_name, start - 1, 1, end - 1, 1],
                'data_labels': {'percentage': True}  # mostra %
            })
            chart_status.set_title({'name': 'Status das Ordens'})
            ws.insert_chart('E26', chart_status)

            linha += len(status_count) + 1
        else:
            ws.write(f'A{linha}', 'Sem coluna "Status" para agrupar.')
            linha += 1

        sep()

        # ---------------- C) RESUMO POR CARTEIRA
        ws.write(f'A{linha}', 'C) Resumo de Cadastros por Carteira', title_format)
        linha += 1

        if 'Carteira' in df_base.columns:
            resumo_carteira = (
                df_base['Carteira']
                .astype(str)
                .replace({"carteira 82": "carteira82", "carteira 07": "carteira07"})
                .value_counts()
                .rename_axis('Carteira')
                .reset_index(name='Total de Cards')
                .sort_values('Carteira', kind='stable')
                .reset_index(drop=True)
            )

            ws.write_row(linha - 1, 0, ['Carteira', 'Total de Cards'], header_format)
            for i, r in enumerate(resumo_carteira.itertuples(index=False), start=linha):
                # acessar por posição para evitar erro _2
                ws.write_row(i - 1, 0, [r.Carteira, int(r[1])])

            start = linha
            end   = linha + len(resumo_carteira) - 1
            chart_resumo = workbook.add_chart({'type': 'column'})
            chart_resumo.add_series({
                'name':       'Cards por Carteira',
                'categories': [ws_name, start - 1, 0, end - 1, 0],
                'values':     [ws_name, start - 1, 1, end - 1, 1],
            })
            chart_resumo.set_title({'name': 'Resumo por Carteira'})
            chart_resumo.set_x_axis({'name': 'Carteira'})
            chart_resumo.set_y_axis({'name': 'Qtd'})
            ws.insert_chart('E46', chart_resumo)

            linha += len(resumo_carteira) + 1
        else:
            ws.write(f'A{linha}', 'Sem coluna "Carteira" para agrupar.')
            linha += 1

        sep()

        # ---------------- D) VOLUME POR CARTEIRA (filtra cancel/duplicado)
        ws.write(f'A{linha}', 'D) Volume por Carteira (exclui duplicado/cancelado)', title_format)
        linha += 1

        if has_status and 'Carteira' in df_base.columns:
            filtro_cancel = df_base[col_status].astype(str).str.contains("Cancelado|Chamado Duplicado", case=False, na=False)
            filtro_ok = ~filtro_cancel

            carteiras = (
                df_base.loc[filtro_ok, 'Carteira']
                .astype(str)
                .replace({"carteira 82": "carteira82", "carteira 07": "carteira07"})
                .dropna()
                .unique()
            )

            for cart in sorted(carteiras, key=lambda s: s.lower()):
                ws.write(f'A{linha}', f'🏷️ {cart}', bold)
                linha += 1

                df_cart = df_base[(df_base['Carteira'].astype(str).str.lower() == str(cart).lower()) & filtro_ok]
                if df_cart.empty:
                    ws.write(f'A{linha}', 'Sem status para esta carteira.')
                    linha += 2
                    continue

                status_cart = df_cart[col_status].astype(str).value_counts().reset_index()
                status_cart.columns = ['Status', 'Qtd']
                ws.write_row(linha - 1, 0, status_cart.columns.tolist(), header_format)
                for i, r in enumerate(status_cart.itertuples(index=False), start=linha):
                    ws.write_row(i - 1, 0, [r.Status, int(r.Qtd)])
                total_cart = int(status_cart['Qtd'].sum())
                ws.write_row(linha - 1 + len(status_cart), 0, ['Total', total_cart], header_format)
                linha += len(status_cart) + 2
        else:
            ws.write(f'A{linha}', 'Sem colunas necessárias.')
            linha += 1

        sep()

        # ---------------- E) CIDADE
        ws.write(f'A{linha}', 'E) Distribuição por Cidade (exclui duplicado/cancelado)', title_format)
        linha += 1

        if has_cidade and col_cidade in df_base.columns:
            filtro_cancel = df_base[col_status].astype(str).str.contains("Cancelado|Chamado Duplicado", case=False, na=False) if has_status else pd.Series(False, index=df_base.index)
            filtro_ok = ~filtro_cancel
            cidades_unicas = df_base.loc[filtro_ok, col_cidade].dropna().astype(str).unique()
            for cidade in sorted(cidades_unicas, key=lambda s: s.lower()):
                ws.write(f'A{linha}', f'🏙️ {cidade}', bold)
                linha += 1
                df_cidade = df_base[(df_base[col_cidade].astype(str) == cidade) & filtro_ok]
                if df_cidade.empty or not has_status:
                    ws.write(f'A{linha}', 'Sem status para esta cidade.')
                    linha += 2
                    linha += 1
                    continue
                status_cidade = df_cidade[col_status].astype(str).value_counts().reset_index()
                status_cidade.columns = ['Status','Qtd']
                ws.write_row(linha - 1, 0, status_cidade.columns.tolist(), header_format)
                for i, r in enumerate(status_cidade.itertuples(index=False), start=linha):
                    ws.write_row(i - 1, 0, [r.Status, int(r.Qtd)])
                linha += len(status_cidade) + 2
        else:
            ws.write(f'A{linha}', 'Sem coluna "Cidade".')
            linha += 1

        sep()

        # ---------------- F) PRODUTO
        ws.write(f'A{linha}', 'F) Distribuição por Produto (exclui duplicado/cancelado)', title_format)
        linha += 1

        if has_produto and col_produto in df_base.columns:
            filtro_cancel = df_base[col_status].astype(str).str.contains("Cancelado|Chamado Duplicado", case=False, na=False) if has_status else pd.Series(False, index=df_base.index)
            filtro_ok = ~filtro_cancel
            produtos_unicos = df_base.loc[filtro_ok, col_produto].dropna().astype(str).unique()
            for produto in sorted(produtos_unicos, key=lambda s: s.lower()):
                ws.write(f'A{linha}', f'📦 {produto}', bold)
                linha += 1
                df_prod = df_base[(df_base[col_produto].astype(str) == produto) & filtro_ok]
                if df_prod.empty or not has_status:
                    ws.write(f'A{linha}', 'Sem status para este produto.')
                    linha += 2
                    linha += 1
                    continue
                status_prod = df_prod[col_status].astype(str).value_counts().reset_index()
                status_prod.columns = ['Status','Qtd']
                ws.write_row(linha - 1, 0, status_prod.columns.tolist(), header_format)
                for i, r in enumerate(status_prod.itertuples(index=False), start=linha):
                    ws.write_row(i - 1, 0, [r.Status, int(r.Qtd)])
                linha += len(status_prod) + 2
        else:
            ws.write(f'A{linha}', 'Sem coluna "Produto".')
            linha += 1

    # ===== criar arquivo e renderizar
    with pd.ExcelWriter(caminho_temporario, engine='xlsxwriter') as writer:
        workbook = writer.book
        used_names = set()

        # ---- Geral
        dash_geral = _slug_sheet("Dashboard", used_names)
        dados_geral = _slug_sheet("Dados", used_names)
        _render_one_dashboard(writer, workbook, dash_geral, df, dados_geral)

        # ---- Por responsável
        if "__Responsavel__" in df.columns and df["__Responsavel__"].astype(str).str.strip().any():
            responsaveis = (
                df["__Responsavel__"]
                .astype(str).fillna("").map(lambda s: re.sub(r"^\s*agente\s*:\s*", "", s, flags=re.I).strip())
            )
            for resp in sorted(r for r in responsaveis.unique() if r):
                df_resp = df[responsaveis == resp].copy()
                ws_name = _slug_sheet(f"Dash_{resp}", used_names)
                dados_name = _slug_sheet(f"Dados_{resp}", used_names)
                _render_one_dashboard(writer, workbook, ws_name, df_resp, dados_name)

    os.replace(caminho_temporario, caminho_excel)
    print(f"✅ Dashboards (geral + por responsável) adicionados com sucesso ao arquivo: {caminho_excel}")
