from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, sum

spark = SparkSession.builder.appName("Top5ProdutosMaisVendidos").getOrCreate()

print("Iniciando a análise dos produtos mais vendidos...")

# 2. Leitura dos dados do CSV
df = (spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("vendas_200k.csv")) # Usando o arquivo com mais dados

# 3. Lógica da Análise
#    - Agrupamos os dados por ID_Produto
#    - Usamos .agg() para aplicar a função de agregação sum() na coluna 'Quantidade'
#    - Renomeamos a coluna resultante para 'Total_Quantidade' com .alias()
#    - Ordenamos o resultado em ordem decrescente
#    - Limitamos o resultado final aos 5 primeiros
top_produtos_df = (df.groupBy("ID_Produto")
                     .agg(sum("Quantidade").alias("Total_Quantidade"))
                     .orderBy(desc("Total_Quantidade"))
                     .limit(5))

# 4. Exibição do Resultado
print("\n--- Top 5 Produtos Mais Vendidos (por Quantidade) ---")
top_produtos_df.show()

# 5. Finalização da Sessão
spark.stop()
