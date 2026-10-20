
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Semana08Spark")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

print("Versão do Spark:", spark.version)
print("Modo de execução:", spark.sparkContext.master)

spark.stop()
