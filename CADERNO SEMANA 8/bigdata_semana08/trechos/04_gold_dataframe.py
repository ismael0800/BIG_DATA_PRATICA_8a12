
from pyspark.sql.functions import sum, count, avg, round, col

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

print("=== GOLD - DATAFRAME API ===")
df_gold.show(20, truncate=False)
