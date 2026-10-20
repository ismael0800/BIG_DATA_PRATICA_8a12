
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


spark.stop()
