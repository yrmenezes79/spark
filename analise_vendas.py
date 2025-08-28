from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, sum # Importando a função 'sum'

spark = SparkSession.builder.appName("AnaliseDeVendas").getOrCreate()

df = (spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("vendas.csv"))

receita_df = df.withColumn("Receita", col("Preco") * col("Quantidade"))

receita_por_categoria = (receita_df.groupBy("Categoria")
                                   .agg(sum("Receita").alias("Receita_Total"))
                                   .orderBy(desc("Receita_Total")))

print("--- Receita Total por Categoria ---")
receita_por_categoria.show()

spark.stop()
