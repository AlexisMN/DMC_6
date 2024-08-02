# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType,IntegerType

# COMMAND ----------

spark

# COMMAND ----------

df_ventas=spark.read.table("ventas_christian_guerrero")

# COMMAND ----------

df_ventas.display()

# COMMAND ----------

df_ventas.select("Tienda","Genero").show()

# COMMAND ----------

df_ventas = df_ventas.withColumn("precio_venta_dolares",df_ventas["precio_venta_soles"]/3.71,)

# COMMAND ----------

df_ventas = df_ventas.withColumn("precio_venta_dolares",col("precio_venta_dolares").cast("Double"))

# COMMAND ----------

df_ventas.display()
