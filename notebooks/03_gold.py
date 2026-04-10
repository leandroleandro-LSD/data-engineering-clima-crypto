# Notebook 03 - Gold
# KPIs e análises cruzadas clima + crypto
# Executa no Databricks

from pyspark.sql import functions as F

df_crypto = spark.table("silver_crypto")
df_clima = spark.table("silver_clima")

# KPI 1 - Ranking de criptos por market cap
df_ranking = (
    df_crypto
    .groupBy("moeda")
    .agg(
        F.max("preco_brl").alias("preco_brl"),
        F.max("preco_usd").alias("preco_usd"),
        F.max("tendencia").alias("tendencia"),
        F.max("market_cap_brl_bi").alias("market_cap_brl_bi"),
        F.max("variacao_24h_brl").alias("variacao_24h_brl")
    )
    .orderBy(F.desc("market_cap_brl_bi"))
    .withColumn("ranking", F.monotonically_increasing_id() + 1)
)

df_ranking.write.format("delta").mode("overwrite").saveAsTable("gold_ranking_crypto")
print("Gold ranking crypto salvo!")

# KPI 2 - Clima cruzado com tendência do Bitcoin
tendencia_btc = df_crypto.filter(F.col("moeda") == "bitcoin").select("tendencia").first()[0]
variacao_btc = df_crypto.filter(F.col("moeda") == "bitcoin").select("variacao_24h_brl").first()[0]

df_gold_resumo = (
    df_clima
    .groupBy("cidade_formatada", "sensacao_termica", "vento_desc")
    .agg(F.max("temperatura").alias("temperatura"))
    .withColumn("btc_tendencia", F.lit(tendencia_btc))
    .withColumn("btc_variacao_24h", F.lit(round(variacao_btc, 4)))
    .withColumn("momento_mercado",
        F.when(F.col("btc_tendencia") == "ALTA", "Mercado em alta")
         .otherwise("Mercado em baixa"))
)

df_gold_resumo.write.format("delta").mode("overwrite").saveAsTable("gold_clima_crypto")
print("Gold clima+crypto salvo!")
