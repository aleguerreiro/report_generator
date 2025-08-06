# zapform_auth.py

import logging
import requests

def get_auth_token(username, password):
    url = "https://api.zapform.com.br/api/auth/login/"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }
    data = {
        "username": username,
        "email": "user@example.com",  # Pode ser removido se não for exigido pela API
        "password": password
    }
    try:
        response = requests.post(url, headers=headers, json=data, timeout=10)
        if response.status_code == 200:
            return response.json().get('key')
        else:
            logging.warning(f"⚠️ Falha ao autenticar com {username}: {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"❌ Erro de autenticação com {username}: {e}")
        return None

def get_valid_token(login_list):
    for login in login_list:
        token = get_auth_token(login["username"], login["password"])
        if token:
            logging.info(f"🔑 Token obtido com sucesso usando {login['username']}")
            return token, login
    raise Exception("⛔ Nenhum token pôde ser obtido com as credenciais fornecidas.")

class TokenManager:
    def __init__(self, login_list, revezamento_intervalo=100):
        self.login_list = login_list
        self.revezamento_intervalo = revezamento_intervalo
        self.current_index = 0
        self.token = None
        self.sucesso_count = 0
        self.refresh_token()

    def refresh_token(self):
        tentativas = 0
        while tentativas < len(self.login_list):
            login = self.login_list[self.current_index]
            token = get_auth_token(login["username"], login["password"])
            if token:
                self.token = token
                self.sucesso_count = 0
                logging.info(f"🔐 Novo token ativo: {login['username']}")
                return
            else:
                logging.warning(f"❌ Falha ao autenticar com {login['username']}")
                self.current_index = (self.current_index + 1) % len(self.login_list)
                tentativas += 1
        raise Exception("⛔ Nenhum token válido disponível.")

    def get_token(self):
        return self.token

    def marcar_sucesso(self):
        self.sucesso_count += 1
        if self.sucesso_count >= self.revezamento_intervalo:
            logging.info(f"🔄 Revezamento: atingido limite de {self.revezamento_intervalo} requisições. Alternando token.")
            self.rotate_login_on_error()

    def rotate_login_on_error(self):
        self.current_index = (self.current_index + 1) % len(self.login_list)
        self.refresh_token()
