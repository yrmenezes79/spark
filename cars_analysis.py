from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

# Criar Spark session
spark = SparkSession.builder.appName("CarsAnalysis").getOrCreate()

# Ler CSV do HDFS
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("hdfs:///user/ec2-user/cars/car_sales_data.csv")

# Mostrar primeiras linhas
print("=== Primeiras linhas do dataset ===")
df.show()

# Média de preço por fabricante
print("=== Média de preço por fabricante ===")
df.groupBy("Manufacturer").avg("Price").show()

# Carro com maior Milhas
print("=== Carro com maior Milhas ===")
df.orderBy(desc("Mileage")).show(1)

# Contagem por tipo de combustível
print("=== Quantidade de carros por Fuel type ===")
df.groupBy("Fuel type").count().show()

# Carros com Engine size > 2.0
print("=== Carros com Engine size > 2.0 ===")
df.filter(df["Engine size"] > 2.0).show()

# Salvar resultado no S3 (exemplo: carros com preco > 20000)
output_path = "s3://capricornio16011979yuri/cars_high_price/"
df.filter(df["Price"] > 20000).write.mode("overwrite").csv(output_path)

print(f"Resultados salvos em {output_path}")

# Parar Spark session
spark.stop()
