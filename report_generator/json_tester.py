import pandas as pd
import json

# caminho completo do arquivo
caminho = "/home/ale/Empresas B2 Dropbox/Pasta da equipe Empresas B2/[Brother2]/[Desenvolvimento]/report_generator/numeros_tojoson.csv"

# lê o CSV sem cabeçalho
df = pd.read_csv(caminho, header=None, names=["numero"])

# cria o dicionário no formato {numero: "-"}
data = {str(row["numero"]).strip(): "-" for _, row in df.iterrows()}

# salva em JSON
with open("saida.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

print("JSON gerado com sucesso → saida.json")
