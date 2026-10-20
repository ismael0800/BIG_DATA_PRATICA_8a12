
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = (
    SparkSession.builder
    .appName("Semana08Spark")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

# O CSV usa ; como separador.
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ";")
    .option("encoding", "UTF-8")
    .csv("vendas_2024.csv")
)

# DataFrame carregado.

print("=== TAMANHO ===")
print("Linhas:", df.count())
print("Colunas:", len(df.columns))

print("=== SCHEMA ===")
df.printSchema()

print("=== AMOSTRA ===")
df.show(5, truncate=False)

print("=== ESTATÍSTICAS ===")
df.describe(["total", "quantidade", "shipping", "discount"]).show()

print("=== NULOS POR COLUNA ===")
# CORREÇÃO IMPORTANTE:
# count(when(col(c).isNull(), 1)) conta 1 quando o valor é nulo.
df.select([
    count(when(col(c).isNull(), 1)).alias(c)
    for c in df.columns
]).show()



from pyspark.sql.functions import col, to_date, year, month, when

# Partimos do DataFrame df já carregado.
df_silver = (
    df

    # Remove order_id repetido.
    .dropDuplicates(["order_id"])

    # Remove registros sem campos essenciais.
    .dropna(subset=["order_id", "total", "order_date"])

    # Garante tipos numéricos.
    .withColumn("total", col("total").cast("double"))
    .withColumn("shipping", col("shipping").cast("double"))
    .withColumn("discount", col("discount").cast("double"))

    # Converte a data de texto para data real.
    .withColumn("order_date", to_date(col("order_date"), "dd/MM/yyyy"))

    # Cria colunas úteis para análise.
    .withColumn("ano", year("order_date"))
    .withColumn("mes", month("order_date"))

    # Mantém apenas pedidos pagos e com valor positivo.
    .filter((col("total") > 0) & (col("status") == "paid"))

    # Substitui nulos de frete/desconto por zero.
    .fillna({"shipping": 0.0, "discount": 0.0})

    # Nova coluna: valor + frete.
    .withColumn("total_com_frete", col("total") + col("shipping"))

    # Classificação didática por faixa.
    .withColumn(
        "faixa_valor",
        when(col("total") < 100, "baixo")
        .when(col("total") <= 500, "medio")
        .otherwise("alto")
    )
)

print("=== SILVER ===")
print("Linhas após limpeza:", df_silver.count())
df_silver.show(10, truncate=False)
df_silver.printSchema()


spark.stop()
