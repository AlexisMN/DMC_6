# Databricks notebook source

from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import pandas as pd 
import numpy as np
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

ruta_ejercicio = 'dbfs:/FileStore/data/Colegios_grupo1.csv'

# COMMAND ----------


df = spark.read.option("header",True).option("useHeader", "true").option("treatEmptyValuesAsNulls", "false").option("treat", "false").csv(ruta_ejercicio, sep = ";", encoding="ISO-8859-1")

display(df)



# COMMAND ----------

df = df.drop("ANEXO").drop("CODLOCAL").drop("IGNORAR").drop("NIV_MOD").drop("COD_CAR").drop("TIPSSEXO").drop("GESTION").drop("GES_DEP").drop("CODCP_INEI").drop("CODCCPP").drop("AREA_CENSO").drop("CODGEO").drop("CODOOII").drop("NLAT_IE").drop("NLONG_IE").drop("TIPOPROG").drop("COD_TUR").drop("ESTADO").drop("COD_TUR").drop("FECHA_ACT")

display(df)

# COMMAND ----------

df = df.withColumn("TALUM_HOM",col("TALUM_HOM").cast("Integer"))
df = df.withColumn("TALUM_MUJ",col("TALUM_MUJ").cast("Integer"))
df = df.withColumn("TALUMNO",col("TALUMNO").cast("Integer"))
df = df.withColumn("VAR_DIF_SEXOS",df["TALUM_HOM"]/df["TALUMNO"],)
df = df.withColumn("VAR_DIF_SEXOS",col("VAR_DIF_SEXOS").cast("Double"))
df = df.withColumn("VAR_DIF_SEXOS",round("VAR_DIF_SEXOS",2))
df = df.withColumn("TDOCENTE",col("TDOCENTE").cast("Integer"))
df = df.withColumn("TSECCION",col("TSECCION").cast("Integer"))
df = df.withColumn("FECHAREG",col("FECHAREG").cast("Date"))

display(df)

# COMMAND ----------

db= " G_01_Taller5"
spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
spark.sql(f"USE {db}")


delta_table_path = "dbfs:/FileStore/delta_02/"

df.write.format("delta").mode("overwrite").save(delta_table_path)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS data_taller05
    USING delta
    LOCATION '{delta_table_path}'
""")


# COMMAND ----------

spark.sql("SELECT * FROM data_taller05").show()
