
import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# 1. RECIBIR PARAMETROS

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "FECHA_PROCESO"]
)

fecha_proceso = args["FECHA_PROCESO"]

datetime.strptime(fecha_proceso, "%Y-%m-%d")

print(f"Fecha de proceso: {fecha_proceso}")

# 2. LEER BRONZE

ruta_bronze = (
    f"s3://lablake/bronze/ventas/"
    f"fecha_carga={fecha_proceso}/"
)

print(f"Leyendo: {ruta_bronze}")

df_bronze = spark.read.parquet(ruta_bronze)

print("Registros Bronze:", df_bronze.count())

# 3. TRANSFORMAR DATOS

df_silver = df_bronze.select(
    F.expr("try_cast(id_venta as BIGINT)").alias("id_venta"),
    F.expr("try_cast(fecha_venta as DATE)").alias("fecha_venta"),
    F.expr("try_cast(id_cliente as BIGINT)").alias("id_cliente"),
    F.expr("try_cast(id_producto as BIGINT)").alias("id_producto"),
    F.expr("try_cast(cantidad as INT)").alias("cantidad"),
    F.expr("try_cast(precio_unitario as DECIMAL(18,2))").alias("precio_unitario"),
    F.expr("try_cast(importe_venta as DECIMAL(18,2))").alias("importe_venta"),
    F.current_timestamp().alias("fecha_procesamiento"),
    F.col("archivo_origen"),
    F.lit(fecha_proceso).cast("date").alias("fecha_carga")
)

# 4. VALIDAR CALIDAD DE DATOS

regla_validez = (
    F.col("id_venta").isNotNull() &
    F.col("fecha_venta").isNotNull() &
    F.col("id_cliente").isNotNull() &
    F.col("id_producto").isNotNull() &
    F.col("cantidad").isNotNull() &
    (F.col("cantidad") > 0) &
    F.col("precio_unitario").isNotNull() &
    (F.col("precio_unitario") >= 0) &
    F.col("importe_venta").isNotNull()
)

df_validos = df_silver.filter(regla_validez)

df_rechazados = df_silver.filter(~regla_validez)

print("Registros validos:", df_validos.count())
print("Registros rechazados:", df_rechazados.count())

# 5. VALIDAR DUPLICADOS

df_duplicados = (
    df_validos.groupBy("id_venta")
    .count()
    .filter(F.col("count") > 1)
)

if df_duplicados.count() > 0:
    df_duplicados.show(truncate=False)
    raise Exception("Se encontraron IDs de venta duplicados")



# 7. CREAR VISTA TEMPORAL

df_validos.createOrReplaceTempView("tmp_ventas")

# 8. CARGA INCREMENTAL ICEBERG

spark.sql("""
    MERGE INTO
        glue_catalog.db_lake_ventas.silver_ventas AS tgt
    USING tmp_ventas AS src
    ON tgt.id_venta = src.id_venta

    WHEN MATCHED THEN
        UPDATE SET *

    WHEN NOT MATCHED THEN
        INSERT *
""")

# 9. VERIFICAR RESULTADOS

spark.sql("""
    SELECT fecha_carga, COUNT(*) AS total_registros
    FROM glue_catalog.db_lake_ventas.silver_ventas
    GROUP BY fecha_carga
    ORDER BY fecha_carga
""").show(truncate=False)

print("Carga Silver finalizada correctamente")
