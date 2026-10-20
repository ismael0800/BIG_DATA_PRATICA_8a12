
# Salva Silver particionada por ano e mês.
(
    df_silver.write
    .mode("overwrite")
    .partitionBy("ano", "mes")
    .parquet("output/silver/vendas")
)

# Salva Gold.
(
    df_gold.write
    .mode("overwrite")
    .parquet("output/gold/receita_mensal")
)

print("Parquet salvo.")

# VERIFICAÇÃO: ler de volta é a melhor prova de que funcionou.
silver_lido = spark.read.parquet("output/silver/vendas")
gold_lido = spark.read.parquet("output/gold/receita_mensal")

print("=== VERIFICAÇÃO SILVER ===")
print("Linhas lidas do Parquet:", silver_lido.count())
silver_lido.printSchema()

print("=== VERIFICAÇÃO GOLD ===")
gold_lido.show(5, truncate=False)
