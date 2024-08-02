# Databricks notebook source
from pyspark.sql.types import *

ruta_ejercicio = 'dbfs:/FileStore/data/DATA_EJERCICIO_S4_Christian_Guerrero.csv'

schema = StructType([
    StructField("ID", IntegerType(), True), \
    StructField("ID_1", IntegerType(), True), \
    StructField("NIVEL_EDUCATIVO", StringType(), True), \
    StructField("SEXO", StringType(), True), \
    StructField("CATEGORIA_EMPLEO", StringType(), True), \
    StructField("EXPERIENCIA_LABORAL", IntegerType(), True), \
    StructField("ESTADO_CIVIL", StringType(), True), \
    StructField("EDAD", IntegerType(), True), \
    StructField("UTILIZACION_TARJETAS", IntegerType(), True), \
    StructField("NUMERO_ENTIDADES", StringType(), True), \
    StructField("DEFAULT", StringType(), True), \
    StructField("NUM_ENT_W", FloatType(), True), \
    StructField("EDUC_W", FloatType(), True), \
    StructField("EXP_LAB_W", FloatType(), True), \
    StructField("EDAD_W", FloatType(), True), \
    StructField("UTIL_TC_W", FloatType(), True), \
    StructField("PD", FloatType(), True), \
    StructField("RPD", FloatType(), True), \
])

df_schema = spark.read.option("header",True).schema(schema).csv(ruta_ejercicio, sep = ";")
df_schema = df_schema.drop("ID_1")

# COMMAND ----------

from pyspark.sql import SQLContext

sc = spark.sparkContext

sqlContext.registerDataFrameAsTable(df_schema, "df_schema")

# COMMAND ----------

df_schema.display()

# COMMAND ----------

# MAGIC %md
# MAGIC EJERCICIO
# MAGIC
# MAGIC Elaborar una consulta donde se muestren los clientes mayores a 35 años con menores probabilidades de default (los 5 primeros).
# MAGIC
# MAGIC Elaborar una consulta donde se muestren los solteros menores a 35 años que utilizan más de 3 veces su tc y tienen menor probabilidad de default (10 primeros)

# COMMAND ----------

sqlContext.sql("""
               SELECT * from df_schema WHERE EDAD > 35 order by pd ASC limit 5
               """).display()

# COMMAND ----------

sqlContext.sql("""
               SELECT * from df_schema where ESTADO_CIVIL = 'SOLTERO' AND UTILIZACION_TARJETAS > 3 ORDER BY PD ASC LIMIT 10
               """).display()
