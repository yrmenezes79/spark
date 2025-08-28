from pyspark.sql import SparkSession

def main():
    # Criar sessão Spark
    spark = SparkSession.builder.appName("NetflixInsights").getOrCreate()

    # Caminho do arquivo CSV (ajuste conforme a pasta onde você salvou o dataset)
    file_path = "/root/netflix_titles_nov_2019.csv"

    # Ler dataset Netflix
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Mostrar schema
    print("### Estrutura do DataFrame ###")
    df.printSchema()

    # Total de registros
    print("\n### Total de registros ###")
    print(df.count())

    # Quantidade de filmes e séries
    print("\n### Quantidade de Filmes e Séries ###")
    df.groupBy("type").count().show()

    # Top 5 países com mais títulos
    print("\n### Top 5 países com mais títulos ###")
    df.groupBy("country").count().orderBy("count", ascending=False).show(5)

    # Ano com mais lançamentos
    print("\n### Ano com mais lançamentos ###")
    df.groupBy("release_year").count().orderBy("count", ascending=False).show(1)

    # Top 5 diretores mais frequentes
    print("\n### Top 5 diretores mais frequentes ###")
    df.filter(df["director"].isNotNull()) \
      .groupBy("director") \
      .count() \
      .orderBy("count", ascending=False) \
      .show(5)

    # Encerrar sessão
    spark.stop()

if __name__ == "__main__":
    main()
