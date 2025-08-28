# receita_eletronicos_cidade.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, sum

# 1. Inicialização da SparkSession
spark = SparkSession.builder.appName("ReceitaEletronicosPorCidade").getOrCreate()

print("Iniciando a análise de receita de eletrônicos por cidade...")

# 2. Leitura dos dados do CSV
df = (spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("vendas_200k.csv")) # Usando o arquivo com mais dados

# 3. Lógica da Análise
#    - PASSO A: Primeiro, criamos a coluna 'Receita' (Preco * Quantidade)
#    - PASSO B: Filtramos o DataFrame para manter apenas as linhas onde 'Categoria' é 'Eletrônicos'
#    - PASSO C: Agrupamos o DataFrame filtrado pela coluna 'Cidade'
#    - PASSO D: Somamos a 'Receita' para cada cidade e renomeamos para 'Receita_Total_Eletronicos'
#    - PASSO E: Ordenamos o resultado para ver as cidades com maior receita primeiro
receita_eletronicos_df = (df.withColumn("Receita", col("Preco") * col("Quantidade"))
                            .filter(col("Categoria") == "Eletrônicos")
                            .groupBy("Cidade")
                            .agg(sum("Receita").alias("Receita_Total_Eletronicos"))
                            .orderBy(desc("Receita_Total_Eletronicos")))

print("\n--- Receita da Categoria 'Eletrônicos' por Cidade ---")
receita_eletronicos_df.show()

spark.stop()
