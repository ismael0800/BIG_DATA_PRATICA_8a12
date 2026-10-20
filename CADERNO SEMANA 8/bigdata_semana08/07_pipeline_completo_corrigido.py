# 07_pipeline_completo_corrigido.py
# Semana 8 - Big Data - Apache Spark
# CSV -> exploração -> Silver -> Gold -> Spark SQL -> Parquet

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, to_date, year, month,
    sum, avg, round
)

spark = (
    SparkSession.builder
    .appName("AnaliseVendas")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ";")
    .option("encoding", "UTF-8")
    .csv("vendas_2024.csv")
)

print("=== 1. EXPLORAÇÃO ===")
print("Linhas:", df.count())
print("Colunas:", len(df.columns))
df.printSchema()
df.show(5, truncate=False)
df.describe(["total", "quantidade", "shipping", "discount"]).show()

print("=== 2. NULOS POR COLUNA ===")
df.select([
    count(when(col(c).isNull(), 1)).alias(c)
    for c in df.columns
]).show()

df_silver = (
    df
    .dropDuplicates(["order_id"])
    .dropna(subset=["order_id", "total", "order_date"])
    .withColumn("total", col("total").cast("double"))
    .withColumn("shipping", col("shipping").cast("double"))
    .withColumn("discount", col("discount").cast("double"))
    .withColumn("order_date", to_date(col("order_date"), "dd/MM/yyyy"))
    .withColumn("ano", year("order_date"))
    .withColumn("mes", month("order_date"))
    .filter((col("total") > 0) & (col("status") == "paid"))
    .fillna({"shipping": 0.0, "discount": 0.0})
    .withColumn("total_com_frete", col("total") + col("shipping"))
    .withColumn(
        "faixa_valor",
        when(col("total") < 100, "baixo")
        .when(col("total") <= 500, "medio")
        .otherwise("alto")
    )
)

print("=== 3. SILVER ===")
print("Linhas após limpeza:", df_silver.count())
df_silver.show(10, truncate=False)

df_gold = (
    df_silver
    .groupBy("ano", "mes", "category")
    .agg(
        round(sum("total"), 2).alias("receita"),
        count("order_id").alias("pedidos"),
        round(avg("total"), 2).alias("ticket_medio")
    )
    .orderBy("ano", "mes", col("receita").desc())
)

print("=== 4. GOLD - DATAFRAME API ===")
df_gold.show(20, truncate=False)

df_silver.createOrReplaceTempView("vendas_silver")

sql_gold = spark.sql("""
    SELECT
        ano,
        mes,
        category,
        ROUND(SUM(total), 2) AS receita,
        COUNT(order_id) AS pedidos,
        ROUND(AVG(total), 2) AS ticket_medio
    FROM vendas_silver
    GROUP BY ano, mes, category
    ORDER BY ano, mes, receita DESC
""")

print("=== 5. GOLD - SPARK SQL ===")
sql_gold.show(20, truncate=False)

(
    df_silver.write
    .mode("overwrite")
    .partitionBy("ano", "mes")
    .parquet("output/silver/vendas")
)

(
    df_gold.write
    .mode("overwrite")
    .parquet("output/gold/receita_mensal")
)

print("=== 6. VERIFICAÇÃO DOS PARQUETS ===")
silver_lido = spark.read.parquet("output/silver/vendas")
gold_lido = spark.read.parquet("output/gold/receita_mensal")

print("Silver lido:", silver_lido.count(), "linhas")
gold_lido.show(5, truncate=False)

print("Pastas criadas:")
print("- output/silver/vendas")
print("- output/gold/receita_mensal")

spark.stop()
