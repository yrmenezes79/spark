from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, desc

# 1. Criar a Spark Session
spark = SparkSession.builder \
    .appName("AnaliseSupermercado") \
    .getOrCreate()

# 2. Ler o arquivo CSV do HDFS
# Troque o caminho pelo local onde você colocou o arquivo no HDFS
caminho = "hdfs:///user/ec2-user/supermercado/supermercado.csv"

df = (spark.read.format("csv")
      .option("header", "true")   # primeira linha é cabeçalho
      .option("inferSchema", "true")  # detectar tipos automaticamente
      .load(caminho))

print("Schema detectado:")
df.printSchema()

# 3. Mostrar algumas linhas
print("Exemplo de registros:")
df.show(10, truncate=False)

# 4. Análises

# 4.1 Quantidade total de registros
total_registros = df.count()
print(f"Total de registros: {total_registros}")

# 4.2 Receita total (Preco * Quantidade)
df_com_receita = df.withColumn("Receita", col("Preco") * col("Quantidade"))

receita_total = df_com_receita.agg(_sum("Receita")).collect()[0][0]
print(f"Receita total: R$ {receita_total:,.2f}")

# 4.3 Top 10 produtos mais vendidos em quantidade
print("Top 10 produtos mais vendidos (quantidade):")
(df.groupBy("Produto")
   .agg(_sum("Quantidade").alias("TotalVendido"))
   .orderBy(desc("TotalVendido"))
   .show(10, truncate=False))

# 4.4 Receita por categoria
print("Receita total por categoria:")
(df_com_receita.groupBy("Categoria")
   .agg(_sum("Receita").alias("ReceitaTotal"))
   .orderBy(desc("ReceitaTotal"))
   .show())

# 4.5 Receita média por cidade
print("Receita média por cidade:")
(df_com_receita.groupBy("Cidade")
   .agg(avg("Receita").alias("ReceitaMedia"))
   .orderBy(desc("ReceitaMedia"))
   .show())

# 4.6 Número de registros por cidade
print("Quantidade de compras por cidade:")
(df.groupBy("Cidade")
   .agg(count("*").alias("NumCompras"))
   .orderBy(desc("NumCompras"))
   .show())

# Finalizar Spark
spark.stop()
