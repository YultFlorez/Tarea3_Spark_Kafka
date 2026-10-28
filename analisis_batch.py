from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, avg, count, to_timestamp
import os

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("AnalisisBatchTaxis") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Ruta del archivo de datos
archivo = os.path.expanduser(
    "~/Tarea3_Spark_Kafka/datos/viajes_taxi_enero_2025.parquet"
)

if not os.path.exists(archivo):
    print("ERROR: No se encuentra el archivo Parquet:", archivo)
    spark.stop()
    raise SystemExit(1)

print("Cargando datos históricos...")
df = spark.read.parquet(archivo)

print("Columnas disponibles:")
df.printSchema()

# Verificar que existan las columnas necesarias
necesarias = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount"
]

faltantes = [c for c in necesarias if c not in df.columns]

if faltantes:
    print("Faltan columnas necesarias:", faltantes)
    spark.stop()
    raise SystemExit(1)

total_original = df.count()

# Limpieza y transformación
df = df.withColumn(
    "inicio",
    to_timestamp(col("tpep_pickup_datetime"))
)

df = df.withColumn(
    "fin",
    to_timestamp(col("tpep_dropoff_datetime"))
)

df = df.withColumn(
    "duracion_min",
    (col("fin").cast("long") - col("inicio").cast("long")) / 60
)

limpio = df.filter(
    col("inicio").isNotNull()
    & col("fin").isNotNull()
    & (col("fin") > col("inicio"))
    & (col("duracion_min") <= 1440)
    & (col("trip_distance") > 0)
    & (col("fare_amount") > 0)
)

total_limpio = limpio.count()

por_hora = limpio.groupBy(
    hour("inicio").alias("hora")
).agg(
    count("*").alias("cantidad_viajes"),
    avg("trip_distance").alias("distancia_promedio_millas"),
    avg("fare_amount").alias("tarifa_promedio_dolares")
).orderBy("hora")

salida = os.path.expanduser("~/Tarea3_Spark_Kafka/resultados")
os.makedirs(salida, exist_ok=True)

datos = por_hora.toPandas()
datos.to_csv(os.path.join(salida, "resumen_por_hora.csv"), index=False)

print("Total original:", total_original, flush=True)
print("Total después de limpieza:", total_limpio, flush=True)
print("Registros descartados:", total_original - total_limpio, flush=True)
por_hora.show(24, truncate=False)

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.figure(figsize=(10, 5))
plt.bar(datos["hora"], datos["cantidad_viajes"])
plt.xlabel("Hora del día")
plt.ylabel("Cantidad de viajes")
plt.title("Viajes de taxis por hora - Enero de 2025")
plt.xticks(range(24))
plt.tight_layout()
plt.savefig(os.path.join(salida, "viajes_por_hora.png"))
plt.close()

print("Resultados guardados en:", salida, flush=True)
spark.stop()
