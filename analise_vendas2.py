from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, sum

# Criando sessão Spark
spark = SparkSession.builder.appName("AnaliseDeVendas").getOrCreate()

# Lendo arquivo CSV
df = (spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("vendas.csv"))

# Criando coluna de Receita
receita_df = df.withColumn("Receita", col("Preco") * col("Quantidade"))

# --- 1) Receita total por categoria ---
receita_por_categoria = (receita_df.groupBy("Categoria")
                                   .agg(sum("Receita").alias("Receita_Total"))
                                   .orderBy(desc("Receita_Total")))

print("\n--- Receita Total por Categoria ---")
receita_por_categoria.show()

# --- 2) Top 5 produtos mais vendidos (quantidade total) ---
produtos_mais_vendidos = (receita_df.groupBy("ID_Produto")
                                      .agg(sum("Quantidade").alias("Quantidade_Total"))
                                      .orderBy(desc("Quantidade_Total"))
                                      .limit(5))

print("\n--- Top 5 Produtos Mais Vendidos ---")
produtos_mais_vendidos.show()

# --- 3) Receita por cidade apenas para 'Eletrônicos' ---
receita_eletronicos_por_cidade = (receita_df.filter(col("Categoria") == "Eletrônicos")
                                              .groupBy("Cidade")
                                              .agg(sum("Receita").alias("Receita_Total"))
                                              .orderBy(desc("Receita_Total")))

print("\n--- Receita de Eletrônicos por Cidade ---")
receita_eletronicos_por_cidade.show()

# Encerrando sessão Spark
spark.stop()
