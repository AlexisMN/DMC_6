# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round
from pyspark.sql.types import StructType, StructField, StringType,IntegerType

# COMMAND ----------

df_ventas=spark.read.table("ventas_ejercicio_christian_guerrero")

# COMMAND ----------

df_ventas.display()

# COMMAND ----------

df_ventas = df_ventas.withColumn("Precio_Total",df_ventas["CANTIDAD"]*df_ventas["PRECIO"],)
df_ventas = df_ventas.withColumn("Impuesto",df_ventas["Precio_Total"]*0.18,)

# COMMAND ----------

df_ventas = df_ventas.withColumn("Precio_Total",col("Precio_Total").cast("Double"))
df_ventas = df_ventas.withColumn("Impuesto",col("Impuesto").cast("Double"))
df_ventas = df_ventas.withColumn("Precio_Total",round("Precio_Total",2))
df_ventas = df_ventas.withColumn("Impuesto",round("Impuesto",2))

# COMMAND ----------

df_ventas.display()
