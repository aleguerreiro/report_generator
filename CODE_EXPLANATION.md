# Explicação do Projeto `report_generator`

### Visão Geral

O projeto `report_generator` é um sistema automatizado para gerar relatórios a partir de dados da API Zapform. Ele é configurado através de uma planilha do Google Sheets e pode ser executado de forma agendada ou manual. O sistema busca dados de "ordens" da API, processa-os, gera um relatório em Excel (incluindo um dashboard personalizado), envia o relatório por e-mail para uma lista de destinatários e salva o estado para execuções futuras.

### Fluxo de Execução

1.  **Inicialização (`main.py`)**:
    *   O programa começa configurando o logging e as credenciais para a API Zapform.
    *   Ele se conecta ao Google Sheets usando as credenciais de uma conta de serviço.
    *   O usuário é perguntado se deseja executar o processo uma vez ou agendá-lo para ser executado diariamente em um horário específico.

2.  **Processamento de Configuração (`process_executor.py`)**:
    *   O sistema itera por todas as planilhas no arquivo Google Sheets que começam com o nome "config". Cada uma dessas planilhas representa uma configuração de relatório diferente.
    *   Para cada configuração, ele lê a planilha para extrair os parâmetros do relatório.

3.  **Leitura da Configuração (`sheet_config_reader.py`)**:
    *   Este módulo é responsável por ler a planilha de configuração e extrair as seguintes informações:
        *   **Filtros**: Critérios para buscar as ordens na API da Zapform (por exemplo, por status, data de criação, etc.).
        *   **Campos**: Quais campos de dados devem ser incluídos no relatório (campos padrão e campos variáveis).
        *   **Lista de E-mails**: Para quem o relatório deve ser enviado.
        *   **Mapeamento de Cabeçalho**: Como as colunas do relatório devem ser nomeadas.

4.  **Busca de Dados (`zapform_api_client.py` e `label_fetcher.py`)**:
    *   Com base nos filtros da configuração, o sistema busca as ordens da API da Zapform.
    *   Ele também busca as "etiquetas" (labels) do fluxo de trabalho para enriquecer os dados.
    *   O sistema gerencia os tokens de autenticação para a API.

5.  **Extração e Processamento de Dados (`extractor.py` e `data_utils.py`)**:
    *   Para cada ordem retornada pela API, o sistema extrai os dados relevantes com base nos campos definidos na configuração.
    *   Os dados são limpos e formatados para serem apresentados no relatório.
    *   O sistema acumula os dados de execuções anteriores e remove duplicatas para garantir que o relatório contenha apenas os dados mais recentes.

6.  **Geração do Relatório (`dashboard_executor.py`)**:
    *   Os dados processados são usados para gerar um arquivo Excel.
    *   Um dashboard personalizado é criado no Excel para visualizar os dados de forma mais amigável.

7.  **Envio de E-mail (`email_sender.py`)**:
    *   O relatório em Excel é anexado a um e-mail.
    *   O e-mail é enviado para a lista de destinatários definida na configuração.

8.  **Salvamento de Estado**:
    *   O sistema salva a data e a hora da última execução para cada configuração, para que possa buscar apenas os dados mais recentes na próxima vez que for executado.
    *   Ele também salva um arquivo CSV com os dados acumulados para evitar a necessidade de buscar todos os dados novamente a cada execução.

### Arquivos Principais

*   `main.py`: O ponto de entrada do programa.
*   `process_executor.py`: O orquestrador principal do processo de geração de relatórios.
*   `sheet_config_reader.py`: Lê e interpreta as configurações da planilha do Google Sheets.
*   `zapform_api_client.py`: Interage com a API da Zapform para buscar dados.
*   `extractor.py`: Extrai e formata os dados brutos da API.
*   `dashboard_executor.py`: Cria o relatório em Excel com o dashboard.
*   `email_sender.py`: Envia o e-mail com o relatório.
*   `schedule_handler.py`: Lida com o agendamento da execução.
