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

# --- Controle de taxa (rate-limiting) e retentativas para as chamadas de API ---

# Intervalo mínimo (segundos) entre duas chamadas consecutivas: evita estourar o
# limite da API (429 Too Many Requests). Ajuste conforme o limite de cada fonte.
INTERVALO_MINIMO = 1.0
_ultima_chamada = {"t": 0.0}

# Sessão reaproveita conexões TCP: mais rápida e mais gentil com o servidor
_sessao = req.Session()
_sessao.headers.update({"Accept": "application/json"})

# Status que valem nova tentativa: 429 (limite de taxa) e erros transitórios
# 5xx — incluindo os do Cloudflare (520-524), como o 522 (timeout entre o
# Cloudflare e o servidor de origem), comum em portais governamentais
_STATUS_RETENTAVEL = {429, 500, 502, 503, 504, 520, 521, 522, 523, 524}


def _aguardar_taxa() -> None:
    """Rate-limiting no cliente: espera o restante do intervalo mínimo entre chamadas."""
    decorrido = time.time() - _ultima_chamada["t"]
    if decorrido < INTERVALO_MINIMO:
        time.sleep(INTERVALO_MINIMO - decorrido)
    _ultima_chamada["t"] = time.time()


def _calcular_espera(tentativa: int, espera_base: int, resposta: req.Response | None = None) -> float:
    """Backoff exponencial (5s, 10s, 20s...), honrando o header Retry-After quando a API o envia."""
    if resposta is not None and "Retry-After" in resposta.headers:
        try:
            return max(float(resposta.headers["Retry-After"]), espera_base)
        except ValueError:
            pass  # Retry-After em formato de data HTTP: ignora e usa o backoff
    return espera_base * (2 ** tentativa)


def baixar_json(url: str, tentativas: int = 5, espera: int = 5) -> dict | list:
    """Busca um JSON com rate-limiting, backoff exponencial e retentativas.

    - Respeita INTERVALO_MINIMO entre chamadas (controle de taxa)
    - Retenta erros de rede (timeout/DNS/conexão), status 429/5xx e páginas de
      erro transitórias (resposta 200 sem JSON)
    - Honra o header Retry-After quando a API o envia
    """
    ultimo_problema = "desconhecido"
    for tentativa in range(tentativas):
        _aguardar_taxa()
        try:
            r = _sessao.get(url, timeout=60)
        except req.exceptions.RequestException as e:  # timeout, DNS, conexão recusada...
            ultimo_problema = f"erro de rede: {type(e).__name__}: {e}"
            pausa = _calcular_espera(tentativa, espera)
            print(f"tentativa {tentativa + 1}/{tentativas}: {ultimo_problema} — repetindo em {pausa:.0f}s")
            time.sleep(pausa)
            continue

        if r.status_code == 200 and "json" in r.headers.get("Content-Type", "").lower():
            return r.json()

        # Página de erro transitória (200 sem JSON) ou status retentável (429/5xx)
        if r.status_code == 200 or r.status_code in _STATUS_RETENTAVEL:
            ultimo_problema = f"resposta inesperada (status {r.status_code})"
            pausa = _calcular_espera(tentativa, espera, r)
            print(f"tentativa {tentativa + 1}/{tentativas}: {ultimo_problema} — repetindo em {pausa:.0f}s")
            time.sleep(pausa)
            continue

        # Erros definitivos (404, 403...): não adianta repetir, falha rápido
        raise RuntimeError(f"Status {r.status_code} ao acessar {url} (não retentável): {r.text[:200]}")

    raise RuntimeError(f"Falha ao obter JSON de {url} após {tentativas} tentativas (último problema: {ultimo_problema})")


def baixar_arquivo(url: str, destino: str, tentativas: int = 5, espera: int = 5) -> str:
    """Baixa um arquivo (csv/xls/...) para `destino` com as mesmas regras de taxa e retentativa."""
    ultimo_problema = "desconhecido"
    for tentativa in range(tentativas):
        _aguardar_taxa()
        try:
            r = _sessao.get(url, timeout=120)
        except req.exceptions.RequestException as e:
            ultimo_problema = f"erro de rede: {type(e).__name__}: {e}"
            pausa = _calcular_espera(tentativa, espera)
            print(f"tentativa {tentativa + 1}/{tentativas}: {ultimo_problema} — repetindo em {pausa:.0f}s")
            time.sleep(pausa)
            continue
        if r.status_code == 200:
            with open(destino, "wb") as f:
                f.write(r.content)
            return destino
        if r.status_code in _STATUS_RETENTAVEL:
            ultimo_problema = f"status {r.status_code}"
            pausa = _calcular_espera(tentativa, espera, r)
            print(f"tentativa {tentativa + 1}/{tentativas}: {ultimo_problema} — repetindo em {pausa:.0f}s")
            time.sleep(pausa)
            continue
        raise RuntimeError(f"Status {r.status_code} ao baixar {url} (não retentável): {r.text[:200]}")
    raise RuntimeError(f"Falha ao baixar {url} após {tentativas} tentativas (último problema: {ultimo_problema})")


def normalizar_colunas(cols) -> list[str]:
    """Padroniza nomes de colunas: 'Razão Social' -> 'RAZAO_SOCIAL'."""
    norm = []
    for c in cols:
        c = str(c).strip().upper()
        c = unicodedata.normalize("NFKD", c).encode("ascii", "ignore").decode("ascii")
        c = re.sub(r"[^0-9A-Z]+", "_", c).strip("_")
        norm.append(c)
    return norm


# COMMAND ----------

# ============================================================================
# VERSÃO MODULAR — mesma ingestão das células acima, reescrita em funções
# Reutiliza baixar_json() e normalizar_colunas() da célula 2.
# Os DataFrames acampamento, mapa_turismo e df_populacao_brasil são produzidos
# com os mesmos nomes e conteúdos das células originais, para comparação.
# Os arquivos bronze são VERSIONADOS: se o portal atualizar um arquivo, a versão
# antiga é preservada em 'versoes/' e a nova passa a ser a corrente.
# ============================================================================

def listar_recursos_ckan(package_id: str, ano_minimo: int | None = None) -> list[dict[str, str]]:
    """Lista os recursos (arquivos) de um dataset do portal dados.turismo.gov.br.
    Se ano_minimo for informado, mantém apenas recursos com ano >= ano_minimo no nome."""
    url = f"https://dados.turismo.gov.br/api/3/action/package_show?id={package_id}"
    recursos = baixar_json(url)["result"]["resources"]

    selecionados = []
    for r in recursos:
        m = re.search(r"\d{4}", r["name"])
        if ano_minimo is None or (m and int(m.group(0)) >= ano_minimo):
            selecionados.append({
                "id": r["id"],
                "periodo": r["name"],
                "url": r["url"],
                "extensao": r["url"].split(".")[-1].lower(),
                # Versão do arquivo segundo o portal: data da última alteração no
                # CKAN (last_modified; fallback: created). É ela que permite
                # comparar com o que já temos SEM precisar baixar o arquivo.
                "versao": r.get("last_modified") or r.get("created") or "",
            })
    return selecionados


def _rotulo_versao(versao: str) -> str:
    """Converte '2025-03-10T09:15:00' em '20250310_091500' para uso em nome de arquivo."""
    data = versao[:10].replace("-", "")
    hora = versao[11:19].replace(":", "")
    return f"{data}_{hora}" if data else "sem-versao"


def _salvar_meta_recurso(meta_path: str, destino: str, versao: str) -> None:
    """Grava o registro de controle de versão de um recurso bronze (função interna)."""
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump({"versao": versao, "arquivo": os.path.basename(destino),
                   "baixado_em": time.strftime("%Y-%m-%dT%H:%M:%S")}, f, ensure_ascii=False)


def baixar_recurso(rec: dict[str, str], volume: str) -> str:
    """Baixa o arquivo de um recurso para o volume bronze, com controle de versão.

    - 1ª execução: baixa o arquivo e registra a versão da fonte em <id>.meta.json
    - Execuções seguintes: compara a versão da fonte (last_modified do CKAN) com
      a registrada:
        * igual     -> nada é feito (arquivo já em dia)
        * diferente -> a versão antiga é arquivada em 'versoes/' e a nova baixada
    - Fonte sem informação de versão: mantém o arquivo existente (como antes)
    Reutiliza baixar_arquivo() da célula 2: mesmo rate-limiting, backoff
    exponencial e retentativas das chamadas de API.
    """
    os.makedirs(volume, exist_ok=True)
    destino = os.path.join(volume, f"{rec['id']}.{rec['extensao']}")
    meta_path = os.path.join(volume, f"{rec['id']}.meta.json")
    versao_fonte = rec.get("versao", "")

    # Carrega o registro de versão salvo na última execução, se houver
    meta = {}
    if os.path.exists(meta_path):
        with open(meta_path, encoding="utf-8") as f:
            meta = json.load(f)
    versao_local = meta.get("versao", "")

    # Caso 1: arquivo nunca baixado -> baixa e registra a versão
    if not os.path.exists(destino):
        baixar_arquivo(rec["url"], destino)
        _salvar_meta_recurso(meta_path, destino, versao_fonte)
        print(f"    baixado (novo): {os.path.basename(destino)}")
        return destino

    # Caso 2: a fonte não informa versão -> mantém o que já existe (comportamento antigo)
    if not versao_fonte:
        print(f"    já existia (fonte sem versão): {os.path.basename(destino)}")
        return destino

    # Caso 3: versão da fonte é a mesma registrada -> arquivo em dia, nada a fazer
    if versao_fonte == versao_local:
        print(f"    em dia (v{_rotulo_versao(versao_local)}): {os.path.basename(destino)}")
        return destino

    # Caso 4: a fonte mudou o arquivo -> arquiva a versão antiga em versoes/ e baixa a nova
    pasta_versoes = os.path.join(volume, "versoes")
    os.makedirs(pasta_versoes, exist_ok=True)
    arquivado = os.path.join(pasta_versoes,
                             f"{rec['id']}__{_rotulo_versao(versao_local or 'sem-versao')}.{rec['extensao']}")
    os.replace(destino, arquivado)  # move atômico: o arquivo nunca fica pela metade
    print(f"    versão antiga arquivada: versoes/{os.path.basename(arquivado)}")
    baixar_arquivo(rec["url"], destino)
    _salvar_meta_recurso(meta_path, destino, versao_fonte)
    print(f"    baixado (atualizado): {os.path.basename(destino)}")
    return destino


def ler_recurso(destino: str, extensao: str) -> SparkDataFrame:
    """Lê um arquivo bronze (CSV ou Excel) como Spark DataFrame com colunas normalizadas."""
    if extensao == "csv":
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
    return df


def ingerir_dataset_turismo(
    package_id: str,
    volume: str,
    ano_minimo: int | None = None,
    adicionar_periodo: bool = False,
) -> tuple[SparkDataFrame | None, list[tuple[str, str, str]]]:
    """Pipeline completo de um dataset do portal: lista, baixa, lê e une tudo em um único DataFrame.
    Colunas ausentes em um arquivo ficam nulas na união. Devolve (dataframe, falhas)."""
    recursos = listar_recursos_ckan(package_id, ano_minimo)
    print(f"[{package_id}] Recursos selecionados: {len(recursos)}")
    for rec in recursos:
        print(f"    {rec['periodo']:<28} {rec['extensao']}")

    dfs, falhas = [], []
    for rec in recursos:
        try:
            destino = baixar_recurso(rec, volume)
            df = ler_recurso(destino, rec["extensao"])
            if adicionar_periodo:
                ano = int(re.search(r"\d{4}", rec["periodo"]).group(0))
                df = (df.withColumn("PERIODO_REFERENCIA", F.lit(rec["periodo"]))
                        .withColumn("ANO", F.lit(ano)))
            dfs.append(df)
        except Exception as e:
            falhas.append((rec["periodo"], type(e).__name__, str(e)[:150]))

    print(f"\n[{package_id}] Arquivos lidos: {len(dfs)} | Falhas: {len(falhas)}")
    for f in falhas:
        print("FALHA:", f)

    if not dfs:
        return None, falhas
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs), falhas


def ingerir_populacao_ibge(volume: str, arquivo: str = "populacao_municipios_censo2022.json") -> SparkDataFrame:
    """População residente por município e estado — Censo 2022 (IBGE / Sidra, tabela 4714).
    OBS: servicodados.ibge.gov.br não é acessível nesta rede; a API do Sidra funciona.
    A 1ª linha do JSON é apenas cabeçalho (metadados) e é descartada."""
    url = "https://api.sidra.ibge.gov.br/values/t/4714/n6/all/v/93/p/2022?format=json"

    os.makedirs(volume, exist_ok=True)
    arquivo_bruto = os.path.join(volume, arquivo)
    with open(arquivo_bruto, "w", encoding="utf-8") as f:
        json.dump(baixar_json(url), f, ensure_ascii=False)

    brutos = spark.read.option("multiLine", True).json(arquivo_bruto)
    dados = brutos.where(F.col("D1C").rlike("^\\d+$"))  # mantém somente linhas com código de município

    return (dados
        .withColumn("COD_MUNICIPIO", F.col("D1C").cast("int"))
        .withColumn("MUNICIPIO", F.expr("substring_index(D1N, ' - ', 1)"))
        .withColumn("UF", F.expr("substring_index(D1N, ' - ', -1)"))
        .withColumn("POPULACAO_RESIDENTE", F.col("V").cast("long"))
        .withColumn("ANO", F.col("D3C").cast("int"))
        .select("COD_MUNICIPIO", "MUNICIPIO", "UF", "POPULACAO_RESIDENTE", "ANO"))

# COMMAND ----------

# ----------------------------------------------------------------------------
# Ingestões: uma chamada por fonte — a única parte que muda entre datasets
# ----------------------------------------------------------------------------
volume_bronze = "/Volumes/workspace/turismo_brasil/bronze"

acampamento, _ = ingerir_dataset_turismo(
    package_id="acampamento-turistico",
    volume=f"{volume_bronze}/acampamento_turistico/arquivos",
    ano_minimo=2024,          # acampamentos dos últimos anos
    adicionar_periodo=True,   # acrescenta PERIODO_REFERENCIA e ANO (como na célula original)
)

mapa_turismo, _ = ingerir_dataset_turismo(
    package_id="emendas-parlamentares",
    volume=f"{volume_bronze}/mapa_turismo/arquivos",
    adicionar_periodo=False,  # igual à célula original: sem colunas de período
)

df_populacao_brasil = ingerir_populacao_ibge(f"{volume_bronze}/populacao_ibge/arquivos")

# Conferência — mesmos números esperados das células anteriores
print("Total de registros (2024-2026):", acampamento.count())
print("Total de registros:", mapa_turismo.count())
print("Municípios carregados:", df_populacao_brasil.count())
print("População total (Censo 2022):", df_populacao_brasil.agg(F.sum("POPULACAO_RESIDENTE")).collect()[0][0])

display(acampamento.orderBy("ANO", "PERIODO_REFERENCIA"))
display(mapa_turismo)
display(df_populacao_brasil)