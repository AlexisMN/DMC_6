# Databricks notebook source
lista = ['AWS', 'Azure', 'GCP', 'Huawei', 'On Premise']

# COMMAND ----------

print(type(lista))

# COMMAND ----------

rdd = spark.sparkContext.parallelize(lista)

# COMMAND ----------

type(rdd)

# COMMAND ----------

df_park = spark.read.table("ventas_ejercicio_christian_guerrero")

# COMMAND ----------

type(df_park)

# COMMAND ----------

rdd2 = df_park.rdd

# COMMAND ----------

rdd2.collect()

# COMMAND ----------

data = [("AWS", 20000),("Azure",50000),("GCP", 150000),("On Premise", 800000)]

# COMMAND ----------

columnas = ["Nombre_Nube","Cant_Usuarios"]


# COMMAND ----------

rdd3 = spark.sparkContext.parallelize(data)

# COMMAND ----------

df_to_rdd=rdd3.toDF()

# COMMAND ----------

df_from_data = rdd3.toDF(columnas)
df_from_data.printSchema()

# COMMAND ----------

df_from_data2 = spark.createDataFrame(data).toDF(*columnas)
df_from_data2.printSchema()
