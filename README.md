# Tarea 3 - Big Data con Apache Spark y Kafka

## Descripcion
Proyecto academico para analizar viajes en taxi de Nueva York con Apache Spark Batch y procesar mensajes en tiempo real mediante Kafka y Spark Structured Streaming.

## Tecnologias
- Python y PySpark
- Apache Spark
- Apache Kafka
- Matplotlib

## Archivos principales
- analisis_batch.py: analisis por lotes.
- codigo/productor_taxis.py: productor de mensajes para Kafka.
- codigo/streaming_taxis.py: procesamiento con Spark Streaming.
- resultados/resumen_por_hora.csv: resumen por hora.
- resultados/viajes_por_hora.png: grafica de resultados.

## Datos
Los datos de enero de 2025 se guardan localmente en la carpeta datos y no se incluyen en este repositorio.
Fuente: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
