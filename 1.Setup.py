# Databricks notebook source
# MAGIC %md
# MAGIC ## 1 - Criação de banco de dados e volumes para armazenar arquivos e tabelas

# COMMAND ----------

#Criar banco de dados
spark.sql("create schema if not exists workspace.turismo_brasil")

#Criar volume de dados pra armazenar arquivos brutos
spark.sql("create volume if not exists workspace.turismo_brasil.bronze")

# Criar volume de dados pra fazer curadoria dos dados brutos
spark.sql("create volume if not exists workspace.turismo_brasil.silver")

# Criar volume de dados pra enriquecer os dados e usar essa camada pra criar relatóios
spark.sql("create volume if not exists workspace.turismo_brasil.gold")