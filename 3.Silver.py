# Databricks notebook source
# DBTITLE 1,Imports
# MAGIC %pip install openpyxl xlrd
# MAGIC
# MAGIC import re
# MAGIC import os
# MAGIC import unicodedata
# MAGIC import pandas as pd
# MAGIC from functools import reduce
# MAGIC from collections.abc import Iterable
# MAGIC from pyspark.sql import Column, DataFrame
# MAGIC from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Funções de leitura e limpeza
# --- Funções de leitura e limpeza da camada silver ---

PASTA_BRONZE = "/Volumes/workspace/turismo_brasil/bronze"
SCHEMA_DESTINO = "workspace.turismo_brasil"

# Valores que significam "sem informação" nas fontes (viram NULL na silver)
PLACEHOLDERS = {"", "-", "--", "null", "n/d", "nd", "na",
                "não informado", "nao informado", "não informada", "nao informada",
                "sem informação", "sem informacao", "não se aplica", "nao se aplica"}


def normalizar_colunas(cols: Iterable[str]) -> list[str]:
    """Padroniza nomes de colunas: 'Razão Social' -> 'RAZAO_SOCIAL'."""
    norm = []
    for c in cols:
        c = str(c).strip().upper()
        c = unicodedata.normalize("NFKD", c).encode("ascii", "ignore").decode("ascii")
        c = re.sub(r"[^0-9A-Z]+", "_", c).strip("_")
        norm.append(c)
    return norm


def sem_acento(col: Column | str) -> Column:
    """Padroniza texto para chave geográfica: maiúsculas e sem acentos
    ('Vespasiano Corrêa' -> 'VESPIANO CORREA'). Aceita nome de coluna ou
    expressão. Usada em MUNICIPIO e UF das duas tabelas que alimentam o join."""
    return F.translate(F.upper(col), "ÁÀÂÃÄÅÉÊÈÍÏÓÔÕÖÚÛÜÇ", "AAAAAEEEIIOOOOUUUC")


def ler_arquivo_bronze(caminho: str) -> DataFrame:
    """Lê um arquivo bronze em um Spark DataFrame com colunas normalizadas.
    CSVs do portal usam ';' como separador e codificação ISO-8859-1."""
    if caminho.endswith(".csv"):
        df = (spark.read
              .option("header", True)
              .option("sep", ";")
              .option("encoding", "ISO-8859-1")
              .csv(caminho))
        return df.toDF(*normalizar_colunas(df.columns))
    # xls / xlsx: leitura via pandas (igual à camada bronze), valores como texto
    pdf = pd.read_excel(caminho)
    pdf.columns = normalizar_colunas(pdf.columns)
    pdf = pdf.apply(lambda col: col.map(lambda v: None if pd.isna(v) else str(v)))
    return spark.createDataFrame(pdf)


def unir_arquivos(caminhos: list[str]) -> DataFrame:
    """Lê vários arquivos e une tudo em um único DataFrame (colunas ausentes em
    um arquivo ficam nulas). Cada arquivo é identificado em ARQUIVO_FONTE."""
    dfs = []
    for caminho in caminhos:
        df = ler_arquivo_bronze(caminho).withColumn("ARQUIVO_FONTE", F.lit(os.path.basename(caminho)))
        dfs.append(df)
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)


def limpar_texto(df: DataFrame) -> DataFrame:
    """Remove espaços extras e converte valores 'sem informação' em NULL
    em todas as colunas de texto (menos ARQUIVO_FONTE)."""
    for campo in df.columns:
        if campo == "ARQUIVO_FONTE":
            continue
        c = F.trim(F.regexp_replace(F.col(campo), r'^"(.*)"$', "$1"))
        df = df.withColumn(campo, F.when(c.isNull() | F.lower(c).isin(*PLACEHOLDERS), None).otherwise(c))
    return df


def para_moeda(nome: str) -> Column:
    """Converte valor monetário em decimal(18,2) — e não float, que perde
    precisão com dinheiro. Aceita os dois formatos que coexistem nos arquivos:
    '217500.0' (ponto decimal) e '1.100.000' (ponto de milhar)."""
    texto = F.trim(F.col(nome))
    milhar = texto.rlike(r"^-?\d{1,3}(\.\d{3})+$")   # 1.100.000
    simples = texto.rlike(r"^-?\d+(\.\d+)?$")        # 217500.0 / 49002.63
    return (F.when(milhar, F.regexp_replace(texto, r"\.", "").cast("decimal(18,2)"))
             .when(simples, texto.cast("decimal(18,2)")))


def para_inteiro(nome: str, tipo: str = "long") -> Column:
    """Converte texto em inteiro ('int' para códigos curtos, 'long'/bigint para
    os grandes, como CPF e CODIGO_PROGRAMA). Não numérico vira NULL."""
    texto = F.trim(F.col(nome))
    return F.when(texto.rlike(r"^-?\d+$"), texto.cast(tipo))


def para_data(nome: str) -> Column:
    """Converte datas aceitando d/M/yyyy e M/d/yyyy (os arquivos misturam os dois
    formatos) e os timestamps ISO que o pandas grava nos arquivos Excel."""
    return F.coalesce(F.expr(f"try_to_date(`{nome}`, 'd/M/yyyy')"),
                      F.expr(f"try_to_date(`{nome}`, 'M/d/yyyy')"),
                      F.expr(f"try_to_date(`{nome}`, 'yyyy-MM-dd HH:mm:ss.SSSSSS')"),
                      F.expr(f"try_to_date(`{nome}`, 'yyyy-MM-dd HH:mm:ss')"))


def salvar_tabela(df: DataFrame, nome_tabela: str) -> None:
    """Salva o DataFrame como Delta table, substituindo a versão anterior."""
    destino = f"{SCHEMA_DESTINO}.{nome_tabela}"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(destino)
    print(f"Tabela salva: {destino} ({df.count()} linhas)")

# COMMAND ----------

# DBTITLE 1,Acampamentos turísticos
# --- 1. Acampamentos turísticos -> acampamento_turistico ---
# Cada arquivo é um retrato do cadastro em um período (os layouts mudaram ao
# longo dos anos), então a união é feita por nome de coluna: campos ausentes ficam nulos.
pasta = f"{PASTA_BRONZE}/acampamento_turistico/arquivos"
caminhos = [os.path.join(pasta, f) for f in sorted(os.listdir(pasta))
            if f.lower().endswith((".csv", ".xls", ".xlsx"))]

# 1. Ler e unir os arquivos (a leitura não aplica nenhuma tratativa extra)
acampamento = unir_arquivos(caminhos)

# 2. Limpeza específica: LINGUAS e SEGMENTOS_TURISTICOS vêm com artefatos de
#    exportação do portal (aspas e parênteses). Aplicada ANTES da limpeza geral
#    para que a lista vazia da fonte, uma vez despida dos artefatos, vire NULL
for campo in ["LINGUAS", "SEGMENTOS_TURISTICOS"]:
    if campo in acampamento.columns:
        acampamento = acampamento.withColumn(campo, F.regexp_replace(F.col(campo), r'["()]', ""))

# 3. Limpeza geral (espaços extras e valores 'sem informação' -> NULL)
acampamento = limpar_texto(acampamento)

# Normalização das chaves geográficas: MUNICIPIO e UF em maiúsculas e sem
# acento, no mesmo padrão da populacao_ibge, para o join entre as duas tabelas
for campo in ["MUNICIPIO", "UF"]:
    if campo in acampamento.columns:
        acampamento = acampamento.withColumn(campo, sem_acento(campo))

# 4. Tipagem: datas (d/M/yyyy e os ISO dos Excel), inteiros, CPF e a lista de CNAEs
for campo in ["DATA_INICIO_OPERACAO", "DATA_DE_ABERTURA", "VALIDADE_DO_CERTIFICADO"]:
    if campo in acampamento.columns:
        acampamento = acampamento.withColumn(campo, para_data(campo))
for campo in ["AREA_MONTAGEM", "CAPACIDADE"]:
    if campo in acampamento.columns:
        acampamento = acampamento.withColumn(campo, para_inteiro(campo, "int"))
if "CPF" in acampamento.columns:
    # 11 dígitos: estoura o limite do int (2.147.483.647) -> bigint.
    # CPFs com zeros à esquerda ('00501268901') perdem os zeros no número.
    acampamento = acampamento.withColumn("CPF", para_inteiro("CPF"))
if "CNAE_S_RELACIONADOS_A_ATIVIDADE" in acampamento.columns:
    # Não é um número só: é uma lista separada por '|' ('5590602|5510801|...'),
    # então vira array<int> em vez de int simples
    acampamento = acampamento.withColumn(
        "CNAE_S_RELACIONADOS_A_ATIVIDADE",
        F.transform(F.filter(F.split("CNAE_S_RELACIONADOS_A_ATIVIDADE", r"\|"),
                             lambda x: x.rlike(r"^\d+$")),
                    lambda x: x.cast("int")))

# 5. Salvar na Delta table
salvar_tabela(acampamento, "acampamento_turistico")
display(acampamento.orderBy("ARQUIVO_FONTE").limit(20))

# COMMAND ----------

# DBTITLE 1,Mapa do turismo (emendas parlamentares)
# --- 2. Mapa do turismo (emendas parlamentares) -> mapa_turismo ---
# A pasta bronze/mapa_turismo reúne também arquivos de outros assuntos
# (regionalização turística, PDFs e um CSV com cabeçalho corrompido):
# só os arquivos de emenda parlamentar entram na tabela; o resto é ignorado.
pasta = f"{PASTA_BRONZE}/mapa_turismo/arquivos"
emendas, ignorados = [], []

for f in sorted(os.listdir(pasta)):
    caminho = os.path.join(pasta, f)
    if not f.lower().endswith((".csv", ".xls", ".xlsx")):
        if f.lower().endswith(".pdf"):
            ignorados.append((f, "extensão não suportada"))
        continue
    try:
        cols = ler_arquivo_bronze(caminho).columns
    except Exception as e:
        ignorados.append((f, f"falha na leitura: {type(e).__name__}"))
        continue
    if "N_CONVENIO" in cols or "NO_CONVENIO" in cols:
        emendas.append(caminho)
    else:
        ignorados.append((f, "outro assunto (não é emenda parlamentar)"))

print(f"Arquivos de emenda: {len(emendas)} | Ignorados: {len(ignorados)}")
for nome, motivo in ignorados:
    print(f"  IGNORADO {nome}: {motivo}")

# 1. Ler e unir os arquivos de emenda
df_emendas = unir_arquivos(emendas)

# 2. Limpeza geral (espaços extras e valores 'sem informação' -> NULL)
df_emendas = limpar_texto(df_emendas)

# 3. Tipagem: códigos (int/bigint), valores (decimal, não float) e datas
for campo in ["N_CONVENIO", "N_EMENDA", "UG"]:
    if campo in df_emendas.columns:
        df_emendas = df_emendas.withColumn(campo, para_inteiro(campo, "int"))
if "CODIGO_PROGRAMA" in df_emendas.columns:
    # 13 dígitos ('5400020180005'): não cabe em int -> bigint
    df_emendas = df_emendas.withColumn("CODIGO_PROGRAMA", para_inteiro("CODIGO_PROGRAMA"))
for campo in ["VALOR_GLOBAL", "VALOR_REPASSE", "VALOR_DESEMBOLSADO"]:
    if campo in df_emendas.columns:
        df_emendas = df_emendas.withColumn(campo, para_moeda(campo))
if "DATA_ULTIMO_DESEMBOLSO" in df_emendas.columns:
    df_emendas = df_emendas.withColumn("DATA_ULTIMO_DESEMBOLSO", para_data("DATA_ULTIMO_DESEMBOLSO"))

# 4. Salvar na Delta table
salvar_tabela(df_emendas, "mapa_turismo")
display(df_emendas.orderBy("VALOR_GLOBAL", ascending=False))

# COMMAND ----------

# DBTITLE 1,População residente (Censo 2022)
# --- 3. População residente (Censo 2022, IBGE/Sidra) -> populacao_ibge ---
arquivo = f"{PASTA_BRONZE}/populacao_ibge/arquivos/populacao_municipios_censo2022.json"
brutos = spark.read.option("multiLine", True).json(arquivo)

# A 1ª linha do JSON é apenas o cabeçalho (metadados): mantém só linhas com
# código de município e separa o sufixo ' - UF' do nome ('Ariquemes - RO').
# MUNICIPIO e UF já saem normalizados (maiúsculas, sem acento), no mesmo padrão
# do acampamento, para o join entre as duas tabelas.
populacao = (brutos
    .where(F.col("D1C").rlike(r"^\d+$"))
    .select(
        F.col("D1C").cast("int").alias("COD_MUNICIPIO"),
        sem_acento(F.expr("substring_index(D1N, ' - ', 1)")).alias("MUNICIPIO"),
        sem_acento(F.expr("substring_index(D1N, ' - ', -1)")).alias("UF"),
        F.col("V").cast("long").alias("POPULACAO_RESIDENTE"),
        F.col("D3C").cast("int").alias("ANO")))

salvar_tabela(populacao, "populacao_ibge")
print("População total (Censo 2022):",
      populacao.agg(F.sum("POPULACAO_RESIDENTE")).collect()[0][0])
display(populacao.limit(20))

# COMMAND ----------

# DBTITLE 1,Acampamentos + população (join)
# --- 4. Join acampamento + população (Censo 2022) -> acampamento_populacao ---
# Chave do join: MUNICIPIO + UF (a UF é parte da chave porque nomes de município
# se repetem entre estados — 232 nomes existem em mais de uma UF). As duas
# tabelas já saem da silver com MUNICIPIO/UF normalizados (maiúsculas, sem
# acento), então a chave é a própria coluna. A população por UF não existe
# pronta no Censo municipal: é somada da própria populacao_ibge. Localidades
# que não são municípios (distritos como 'ILHA GRANDE') não constam no Censo.
# INNER JOIN: a tabela final traz apenas o que casou — nenhuma linha com
# população NULL; linhas sem município no cadastro ou fora do Censo caem.
# 'site': usamos a coluna SITE (o cadastro também tem WEBSITE).
populacao_por_municipio = populacao.select(
    F.col("MUNICIPIO").alias("CHAVE_MUNICIPIO"),
    F.col("UF").alias("CHAVE_UF"),
    F.col("POPULACAO_RESIDENTE").alias("POPULACAO_MUNICIPIO"))

populacao_por_uf = (populacao
    .groupBy(F.col("UF").alias("CHAVE_UF"))
    .agg(F.sum("POPULACAO_RESIDENTE").alias("POPULACAO_ESTADUAL")))

acampamento_populacao = (acampamento
    .withColumn("CHAVE_MUNICIPIO", F.col("MUNICIPIO"))
    .withColumn("CHAVE_UF", F.col("UF"))
    .join(populacao_por_municipio, ["CHAVE_MUNICIPIO", "CHAVE_UF"], "inner")
    .join(populacao_por_uf, "CHAVE_UF", "left")
    .select(
        F.col("MUNICIPIO").alias("CIDADE"),
        F.col("UF").alias("ESTADO"),
        F.col("NOME_FANTASIA"),
        F.col("SITE"),
        F.col("POPULACAO_MUNICIPIO"),
        F.col("POPULACAO_ESTADUAL")))

salvar_tabela(acampamento_populacao, "acampamento_populacao")
print("Linhas:", acampamento_populacao.count(),
      "| com população municipal:",
      acampamento_populacao.where(F.col("POPULACAO_MUNICIPIO").isNotNull()).count())
display(acampamento_populacao.limit(20))