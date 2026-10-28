import json
import os
from pyspark.sql import SparkSession
from kafka import KafkaProducer

RUTA_DATOS = os.path.expanduser(
    "~/Tarea3_Spark_Kafka/datos/viajes_taxi_enero_2025.parquet"
)
TOPICO = "viajes-taxi"

spark = SparkSession.builder \
    .appName("ProductorTaxisKafka") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Cargando los viajes de taxi...", flush=True)
df = spark.read.parquet(RUTA_DATOS)

columnas = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount"
]

faltantes = [c for c in columnas if c not in df.columns]
if faltantes:
    raise ValueError(f"Faltan columnas en el Parquet: {faltantes}")

viajes = df.select(*columnas).dropna().limit(1000).collect()

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda dato: json.dumps(dato).encode("utf-8")
)

try:
    for i, fila in enumerate(viajes, start=1):
        dato = {
            "fecha_inicio": str(fila["tpep_pickup_datetime"]),
            "fecha_fin": str(fila["tpep_dropoff_datetime"]),
            "distancia_millas": float(fila["trip_distance"]),
            "tarifa_dolares": float(fila["fare_amount"])
        }
        producer.send(TOPICO, value=dato)

        if i % 100 == 0:
            producer.flush()
            print(f"Enviados {i} viajes", flush=True)

    producer.flush()
    print(f"Proceso terminado. Total enviado: {len(viajes)} viajes.", flush=True)

finally:
    producer.close()
    spark.stop()
