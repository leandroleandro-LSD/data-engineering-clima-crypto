# Notebook 01 - Bronze
# Pipeline de ingestão incremental GCS → Delta Lake
# Executa no Databricks

import json
import os
from google.cloud import storage
from pyspark.sql.types import *

KEY_PATH = "/Workspace/Users/leandro.leandro@aluno.impacta.edu.br/sp-trans-492822-e74e1ca7be7f.json"
BUCKET = "clima-crypto-datalake"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH
cliente = storage.Client.from_service_account_json(KEY_PATH)
bucket = cliente.bucket(BUCKET)

try:
    processados = spark.table("controle_arquivos").select("arquivo").rdd.flatMap(lambda x: x).collect()
except:
    processados = []
    spark.sql("""
        CREATE TABLE IF NOT EXISTS controle_arquivos (
            arquivo STRING, tipo STRING, dt_processamento TIMESTAMP
        )
    """)

schema_crypto = StructType([
    StructField("moeda", StringType()),
    StructField("preco_brl", DoubleType()),
    StructField("preco_usd", DoubleType()),
    StructField("variacao_24h_brl", DoubleType()),
    StructField("variacao_24h_usd", DoubleType()),
    StructField("market_cap_brl", DoubleType()),
    StructField("volume_24h_brl", DoubleType()),
    StructField("dt_ingestao", StringType()),
])

for blob in bucket.list_blobs(prefix="raw/crypto/"):
    if blob.name in processados:
        continue
    conteudo = json.loads(blob.download_as_text())
    dados = [{"moeda": str(m), "preco_brl": float(v.get("brl",0)),
              "preco_usd": float(v.get("usd",0)),
              "variacao_24h_brl": float(v.get("brl_24h_change",0)),
              "variacao_24h_usd": float(v.get("usd_24h_change",0)),
              "market_cap_brl": float(v.get("brl_market_cap",0)),
              "volume_24h_brl": float(v.get("brl_24h_vol",0)),
              "dt_ingestao": str(blob.updated)} for m, v in conteudo.items()]
    spark.createDataFrame(dados, schema=schema_crypto).write.format("delta").mode("append").saveAsTable("bronze_crypto")
    spark.sql(f"INSERT INTO controle_arquivos VALUES ('{blob.name}', 'crypto', current_timestamp())")

schema_clima = StructType([
    StructField("cidade", StringType()),
    StructField("temperatura", DoubleType()),
    StructField("velocidade_vento", DoubleType()),
    StructField("direcao_vento", DoubleType()),
    StructField("codigo_tempo", IntegerType()),
    StructField("dt_ingestao", StringType()),
])

for blob in bucket.list_blobs(prefix="raw/clima/"):
    if blob.name in processados:
        continue
    conteudo = json.loads(blob.download_as_text())
    dados = [{"cidade": str(c), "temperatura": float(v.get("current_weather",{}).get("temperature",0)),
              "velocidade_vento": float(v.get("current_weather",{}).get("windspeed",0)),
              "direcao_vento": float(v.get("current_weather",{}).get("winddirection",0)),
              "codigo_tempo": int(v.get("current_weather",{}).get("weathercode",0)),
              "dt_ingestao": str(blob.updated)} for c, v in conteudo.items()]
    spark.createDataFrame(dados, schema=schema_clima).write.format("delta").mode("append").saveAsTable("bronze_clima")
    spark.sql(f"INSERT INTO controle_arquivos VALUES ('{blob.name}', 'clima', current_timestamp())")

print("Bronze incremental concluído!")
