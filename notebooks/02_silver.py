# Notebook 02 - Silver
# Limpeza e enriquecimento dos dados Bronze
# Executa no Databricks

from pyspark.sql import functions as F

# ---- SILVER CRYPTO ----
df_crypto = spark.table("bronze_crypto")

df_silver_crypto = (
    df_crypto
    .withColumn("preco_brl", F.round("preco_brl", 2))
    .withColumn("preco_usd", F.round("preco_usd", 2))
    .withColumn("variacao_24h_brl", F.round("variacao_24h_brl", 4))
    .withColumn("market_cap_brl_bi", F.round(F.col("market_cap_brl") / 1e9, 2))
    .withColumn("volume_24h_brl_mi", F.round(F.col("volume_24h_brl") / 1e6, 2))
    .withColumn("tendencia",
        F.when(F.col("variacao_24h_brl") > 0, "ALTA")
         .when(F.col("variacao_24h_brl") < 0, "BAIXA")
         .otherwise("ESTÁVEL"))
    .withColumn("dt_processamento", F.current_timestamp())
    .drop("market_cap_brl", "volume_24h_brl")
)

df_silver_crypto.write.format("delta").mode("overwrite").saveAsTable("silver_crypto")
print("Silver crypto salvo!")

# ---- SILVER CLIMA ----
df_clima = spark.table("bronze_clima")

df_silver_clima = (
    df_clima
    .withColumn("sensacao_termica",
        F.when(F.col("temperatura") >= 30, "MUITO QUENTE")
         .when(F.col("temperatura") >= 25, "QUENTE")
         .when(F.col("temperatura") >= 20, "AGRADÁVEL")
         .when(F.col("temperatura") >= 15, "FRESCO")
         .otherwise("FRIO"))
    .withColumn("vento_desc",
        F.when(F.col("velocidade_vento") >= 30, "FORTE")
         .when(F.col("velocidade_vento") >= 15, "MODERADO")
         .otherwise("FRACO"))
    .withColumn("cidade_formatada",
        F.regexp_replace(F.initcap(F.col("cidade")), "_", " "))
    .withColumn("dt_processamento", F.current_timestamp())
)

df_silver_clima.write.format("delta").mode("overwrite").saveAsTable("silver_clima")
print("Silver clima salvo!")
