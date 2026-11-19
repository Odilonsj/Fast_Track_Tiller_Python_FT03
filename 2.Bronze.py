# Databricks notebook source
# MAGIC %pip install openpyxl xlrd
# MAGIC import re
# MAGIC import os
# MAGIC import time
# MAGIC import unicodedata
# MAGIC import requests as req
# MAGIC import pandas as pd
# MAGIC import json
# MAGIC from functools import reduce
# MAGIC from pyspark.sql import functions as F

# COMMAND ----------

def baixar_json(url, tentativas=5, espera=5):
    """Busca um JSON com retentativas: o portal às vezes devolve uma página de erro transitória (não-JSON)."""
    for tentativa in range(tentativas):
        r = req.get(url, headers={"Accept": "application/json"}, timeout=60)
        if r.status_code == 200 and "json" in r.headers.get("Content-Type", "").lower():
            return r.json()
        print(f"tentativa {tentativa + 1}/{tentativas}: resposta inesperada (status {r.status_code}), repetindo em {espera}s")
        time.sleep(espera)
    raise RuntimeError(f"O portal não devolveu JSON para {url} (último status: {r.status_code})")


def normalizar_colunas(cols):
    """Padroniza nomes de colunas: 'Razão Social' -> 'RAZAO_SOCIAL'."""
    norm = []
    for c in cols:
        c = str(c).strip().upper()
        c = unicodedata.normalize("NFKD", c).encode("ascii", "ignore").decode("ascii")
        c = re.sub(r"[^0-9A-Z]+", "_", c).strip("_")
        norm.append(c)
    return norm


# COMMAND ----------

# DBTITLE 1,Acampamentos turísticos dos últimos 3 anos
volume_acamp_tur = "/Volumes/workspace/turismo_brasil/bronze/acampamento_turistico/arquivos"
os.makedirs(volume_acamp_tur, exist_ok=True)

# Acampamentos turísticos dos últimos 3 anos 
ANO_MINIMO = 2024

# Lista os recursos do dataset e filtra os períodos recentes
package_id = "acampamento-turistico"
url = f"https://dados.turismo.gov.br/api/3/action/package_show?id={package_id}"
pkg = baixar_json(url)["result"]

recursos_recentes = []
for r in pkg["resources"]:
    m = re.search(r"\d{4}", r["name"])
    if m and int(m.group(0)) >= ANO_MINIMO:
        recursos_recentes.append({
            "id": r["id"],
            "periodo": r["name"],
            "url": r["url"],
            "extensao": r["url"].split(".")[-1].lower(),
        })

print(f"Recursos de {ANO_MINIMO} até hoje: {len(recursos_recentes)}")
for rec in recursos_recentes:
    print(f"  {rec['periodo']:<28} {rec['extensao']}")

# Download dos arquivos recentes para o volume bronze

dfs = []
falhas = []
for rec in recursos_recentes:
    try:
        destino = os.path.join(volume_acamp_tur, f"{rec['id']}.{rec['extensao']}")
        if not os.path.exists(destino):
            conteudo = req.get(rec["url"], timeout=120).content
            with open(destino, "wb") as f:
                f.write(conteudo)

        ano = int(re.search(r"\d{4}", rec["periodo"]).group(0))

        if rec["extensao"] == "csv":
            df = (spark.read
                  .option("header", True)
                  .option("sep", ";")
                  .option("encoding", "ISO-8859-1")
                  .csv(destino))
            df = df.toDF(*normalizar_colunas(df.columns))
        else:  # xls / xlsx
            pdf = pd.read_excel(destino)
            pdf.columns = normalizar_colunas(pdf.columns)
            pdf = pdf.apply(lambda col: col.map(lambda v: None if pd.isna(v) else str(v)))
            df = spark.createDataFrame(pdf)

        dfs.append(df
                    .withColumn("PERIODO_REFERENCIA", F.lit(rec["periodo"]))
                    .withColumn("ANO", F.lit(ano)))
    except Exception as e:
        falhas.append((rec["periodo"], type(e).__name__, str(e)[:150]))

print(f"\nArquivos lidos: {len(dfs)} | Falhas: {len(falhas)}")
for f in falhas:
    print("FALHA:", f)

# Une tudo em um único DataFrame (colunas ausentes em um arquivo ficam nulas)
acampamento = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)

print("Total de registros (2024-2026):", acampamento.count())
display(acampamento.orderBy("ANO", "PERIODO_REFERENCIA"))

# COMMAND ----------

volume_mapa_tur = "/Volumes/workspace/turismo_brasil/bronze/mapa_turismo/arquivos"
os.makedirs(volume_mapa_tur, exist_ok=True)

# Lista os recursos do dataset
package_id = "emendas-parlamentares"
url = url = f"https://dados.turismo.gov.br/api/3/action/package_show?id={package_id}"
pkg = baixar_json(url)["result"]

recursos_recentes = []
for r in pkg["resources"]:
    m = re.search(r"\d{4}", r["name"])
    recursos_recentes.append({
        "id": r["id"],
        "periodo": r["name"],
        "url": r["url"],
        "extensao": r["url"].split(".")[-1].lower(),
    })

for rec in recursos_recentes:
    print(f"    {rec['periodo']:<28} {rec['extensao']}")

# Download dos arquivos recentes para o volume bronze

dfs = []
falhas = []
for rec in recursos_recentes:
    destino = os.path.join(volume_mapa_tur, f"{rec['id']}.{rec['extensao']}")
    if not os.path.exists(destino):
        conteudo = req.get(rec["url"], timeout=120).content
        with open(destino, "wb") as f:
            f.write(conteudo)

    try:
        if rec["extensao"] == "csv":
            df = (spark.read
                  .option("header", True)
                  .option("sep", ";")
                  .option("encoding", "ISO-8859-1")
                  .csv(destino))
            df = df.toDF(*normalizar_colunas(df.columns))
            dfs.append(df)
        else:  # xls / xlsx
            pdf = pd.read_excel(destino)
            pdf.columns = normalizar_colunas(pdf.columns)
            pdf = pdf.apply(lambda col: col.map(lambda v: None if pd.isna(v) else str(v)))
            df = spark.createDataFrame(pdf)
            dfs.append(df)
    except Exception as e:
        falhas.append(f"{rec['periodo']}: {e}")

print(f"\nArquivos lidos: {len(dfs)} | Falhas: {len(falhas)}")
for f in falhas:
    print("FALHA:", f)

# Une tudo em um único DataFrame (colunas ausentes em um arquivo ficam nulas)
mapa_turismo = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)

print("Total de registros :", mapa_turismo.count())
display(mapa_turismo)

# COMMAND ----------

# DBTITLE 1,População residente por município e estado (Censo 2022 – IBGE)
# População residente por município e estado — Censo Demográfico 2022 (IBGE / Sidra)
# Tabela 4714, variável 93 (População residente), nível territorial N6 (municípios).
# OBS: o portal servicodados.ibge.gov.br está inacessível nesta rede; a API do Sidra funciona.
URL_POPULACAO = "https://api.sidra.ibge.gov.br/values/t/4714/n6/all/v/93/p/2022?format=json"

# Baixa os dados brutos para o volume bronze
volume_pop_ibge = "/Volumes/workspace/turismo_brasil/bronze/populacao_ibge/arquivos"
os.makedirs(volume_pop_ibge, exist_ok=True)
arquivo_bruto = os.path.join(volume_pop_ibge, "populacao_municipios_censo2022.json")

linhas = baixar_json(URL_POPULACAO)
with open(arquivo_bruto, "w", encoding="utf-8") as f:
    json.dump(linhas, f, ensure_ascii=False)
print(f"Arquivo bruto salvo: {arquivo_bruto} ({len(linhas)} linhas)")

# Leitura distribuída com Spark: a 1ª linha do JSON é apenas o cabeçalho (metadados), descartada
brutos = (spark.read
          .option("multiLine", True)
          .json(arquivo_bruto))

dados = brutos.where(F.col("D1C").rlike("^\\d+$"))  # mantém somente linhas com código de município

# DataFrame final: município, estado e população residente
df_populacao_brasil = (dados
    .withColumn("COD_MUNICIPIO", F.col("D1C").cast("int"))
    .withColumn("MUNICIPIO", F.expr("substring_index(D1N, ' - ', 1)"))
    .withColumn("UF", F.expr("substring_index(D1N, ' - ', -1)"))
    .withColumn("POPULACAO_RESIDENTE", F.col("V").cast("long"))
    .withColumn("ANO", F.col("D3C").cast("int"))
    .select("COD_MUNICIPIO", "MUNICIPIO", "UF", "POPULACAO_RESIDENTE", "ANO")
    )

print("Municípios carregados:", df_populacao_brasil.count())
print("População total (Censo 2022):", df_populacao_brasil.agg(F.sum("POPULACAO_RESIDENTE")).collect()[0][0])

display(df_populacao_brasil)