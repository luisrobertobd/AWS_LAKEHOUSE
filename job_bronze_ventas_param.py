import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name
)


spark = SparkSession.builder.getOrCreate()

# 1. Definir argumento
args = getResolvedOptions(
    sys.argv,
    ["FECHA_PROCESO"]
)

fecha_proceso = datetime.strptime(
    args["FECHA_PROCESO"],
    "%Y-%m-%d"
)

# 2. Definir rutas S3
fecha_carga = fecha_proceso.strftime("%Y-%m-%d")
fecha_archivo = fecha_proceso.strftime("%Y%m%d")

ruta_origen = (
    f"s3://lablake/landing/Ventas/"
    f"{fecha_proceso:%Y/%m/%d}/"
    f"ventas_{fecha_archivo}.csv"
)

ruta_destino = (
    f"s3://lablake/bronze/ventas/"
    f"fecha_carga={fecha_carga}/"
)

print(f"Fecha de proceso: {fecha_carga}")
print(f"Origen: {ruta_origen}")
print(f"Destino: {ruta_destino}")



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
