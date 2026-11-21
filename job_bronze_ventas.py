
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    lit
)

# 1. Inicializar Spark
spark = SparkSession.builder.getOrCreate()

# 2. Definir rutas S3
ruta_origen = (
    "s3://lablake/landing/Ventas/2026/11/01/"
)

ruta_destino = (
    "s3://lablake/bronze/ventas/fecha_carga=2026-11-01/"
)



# 3. Leer archivo CSV
df_ventas = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(ruta_origen)
)

# 4. Agregar columnas de auditoria
df_bronze = (
    df_ventas
    .withColumn("fecha_procesamiento", current_timestamp())
    .withColumn("archivo_origen", input_file_name())
    )

# 5. Mostrar estructura y cantidad
df_bronze.printSchema()

print("Total registros:", df_bronze.count())

# 6. Guardar datos en Bronze
(
    df_bronze.write
    .mode("overwrite")
    .parquet(ruta_destino)
)

print("Carga Bronze finalizada")
