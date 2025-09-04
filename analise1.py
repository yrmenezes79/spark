from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Lendo o arquivo de texto (ex: "livro.txt")
rdd = spark.sparkContext.textFile("livro.txt")

palavras = rdd.flatMap(lambda linha: linha.split(" "))
pares = palavras.map(lambda palavra: (palavra.lower(), 1))
contagem = pares.reduceByKey(lambda a, b: a + b)

top10 = contagem.takeOrdered(10, key=lambda x: -x[1])
for palavra, freq in top10:
    print(f"{palavra}: {freq}")

spark.stop()
