
# Registra o DataFrame Silver como uma tabela temporária.
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

print("=== GOLD - SPARK SQL ===")
sql_gold.show(20, truncate=False)
