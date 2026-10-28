from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("StreamingTaxisKafka") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

esquema = StructType([
    StructField("fecha_inicio", StringType(), True),
    StructField("fecha_fin", StringType(), True),
    StructField("distancia_millas", DoubleType(), True),
    StructField("tarifa_dolares", DoubleType(), True)
])

print("Conectando Spark Streaming con Kafka...", flush=True)

mensajes = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "viajes-taxi") \
    .option("startingOffsets", "earliest") \
    .load()

viajes = mensajes.select(
    from_json(
        col("value").cast("string"),
        esquema
    ).alias("viaje")
).select("viaje.*")

viajes_validos = viajes.filter(
    col("distancia_millas").isNotNull()
    & col("tarifa_dolares").isNotNull()
    & (col("distancia_millas") > 0)
    & (col("tarifa_dolares") > 0)
)

consulta = viajes_validos.agg(
    count("*").alias("cantidad_viajes"),
    avg("distancia_millas").alias("distancia_promedio_millas"),
    avg("tarifa_dolares").alias("tarifa_promedio_dolares")
).writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .start()

print("Streaming activo. Esperando los mensajes de Kafka...", flush=True)

consulta.awaitTermination()

