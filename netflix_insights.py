# analise_netflix.py
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, split, explode, trim

def main(csv_path):
    spark = (SparkSession.builder
             .appName("AnaliseNetflixLocal")
             .getOrCreate())

    print(f"Iniciando a análise do arquivo: {csv_path}")

    # Carregar os dados do CSV a partir do caminho fornecido
    df = (spark.read.format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(csv_path))

    # Insight 1: Contagem por Tipo (Filme vs. TV Show) ---
    print("\n--- Insight 1: Contagem por Tipo de Conteúdo ---")
    count_by_type = df.groupBy("type").count().orderBy(desc("count"))
    count_by_type.show()

    #Insight 2: Top 10 Países Produtores ---
    print("\n--- Insight 2: Top 10 Países com Mais Conteúdo ---")
    top_countries = (df.filter(col("country").isNotNull())
                     .withColumn("country_single", explode(split(col("country"), ",")))
                     .withColumn("country_single", trim(col("country_single")))
                     .groupBy("country_single")
                     .count()
                     .orderBy(desc("count"))
                     .limit(10))
    top_countries.show()

    #Insight 3: Número de Lançamentos por Ano ---
    print("\n--- Insight 3: Evolução de Lançamentos por Ano ---")
    releases_per_year = (df.filter(col("release_year").isNotNull())
                         .groupBy("release_year")
                         .count()
                         .orderBy(desc("release_year")))
    releases_per_year.show(20)

    #Insight 4: Top 10 Diretores ---
    print("\n--- Insight 4: Top 10 Diretores com Mais Títulos ---")
    top_directors = (df.filter(col("director").isNotNull())
                     .groupBy("director")
                     .count()
                     .orderBy(desc("count"))
                     .limit(10))
    top_directors.show(truncate=False)

    print("\nAnálise concluída com sucesso!")
        
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: spark-submit analise_netflix.py <caminho_para_o_arquivo_csv>", file=sys.stderr)
        sys.exit(-1)
    
    csv_file_path = sys.argv[1]
    main(csv_file_path)
