# Databricks notebook source
# MAGIC %md
# MAGIC Ejercio
# MAGIC
# MAGIC 1.Cargar archivo proporcionado y procesarlo de manera directa, respetando los tipos de datos.
# MAGIC
# MAGIC 2.Eliminar el campo repetido, si existiera.
# MAGIC
# MAGIC 3.Dado el supuesto que la edad de los estudiantes puede afectar su linea de credito realice un shock la variable edad, añadiendo un campo edad maxima de endeudamiento, la edad maxima será 5% adicional a la variable edad, así como repita el mismo procedimiento para la probabilidad de dafault(CAMPO DEFAULT), DONDE DEBE DE CREAR UN NUEVO CAMPO CON EL CRECIMIENTO DE 8% DEL DEFAULT ORIGINAL

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round
from pyspark.sql.types import StructType, StructField, StringType,IntegerType
import pandas as pd 
import numpy as np
from pandas import DataFrame

# COMMAND ----------


from pyspark.sql.types import *

ruta = "dbfs:/FileStore/data/DATA_EJERCICIO_S4_Christian_Guerrero.csv"

#Definir esquema


# COMMAND ----------

df_with_schema = spark.read.format("csv").option("header","true").option("delimiter",";").load(ruta)

# COMMAND ----------

df_with_schema.printSchema()

# COMMAND ----------

df_with_schema.display()

# COMMAND ----------

df_with_schema = df_with_schema.withColumn("EDAD",col("EDAD").cast("Float"))
df_with_schema = df_with_schema.withColumn("PD",col("PD").cast("Float"))

# COMMAND ----------

df_with_schema = df_with_schema.withColumn("Mayor de 40",df_with_schema['EDAD'] > 40)
df_with_schema = df_with_schema.withColumn("Impuesto",df_with_schema["Default"]*1.08,)


# COMMAND ----------

df_with_schema = df_with_schema.select([col for col in df_with_schema.columns if not col.startswith('E')])
df_with_schema.display()

