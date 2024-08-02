# Databricks notebook source
###Conexion con firma de acceso compartido
######En Azure se configuran los permisos, los objetos y el tiempo en que va a ser util esta firma, lueg se vence
storage_account= "storageaccountchristiang"
contenedor="outputs"
TokenSAS = "sp=racwdl&st=2024-07-12T01:09:26Z&se=2024-07-12T09:09:26Z&spr=https&sv=2022-11-02&sr=c&sig=b3PjOersMdwlZV5f8BXvjbuC%2FhGsL%2B69lbcGM%2BHOef0%3D"
####Seteo de configuracion
spark.conf.set("fs.azure.sas."+contenedor+"."+storage_account+".blob.core.windows.net",TokenSAS)
###Para quitar
#spark.conf.unset("fs.azure.sas."+contenedor+"."+storage_account+".blob.core.windows.net")


# COMMAND ----------

dbutils.fs.ls("wasbs://"+contenedor+"@"+storage_account+".blob.core.windows.net")

# COMMAND ----------

from pyspark.sql.types import *
df= spark.read.csv(file_location + "/SERVICIOS_USADOS.csv", sep = ";")
df.display()

# COMMAND ----------

sc = spark.sparkContext

sqlContext.registerDataFrameAsTable(df, "df")

# COMMAND ----------

sqlContext.sql("""
               select month(start_time),avg(duracion_hrs) from (
               SELECT *,(finish_time – start_time) as duracion_hrs from df 
               where year(start_time) = '2018')
               group by month(start_time)
               order by month(start_time) desc
               """).display()

df.write.mode('overwrite').format('parquet').save("wasbs://grupo001taller3@"+storage_account+".blob.core.windows.net/Parte1Parquet1")

# COMMAND ----------

sqlContext.sql("""
               select * ,(finish_time – start_time) as duracion_hrs from df
               order by price limit 500
               """).display()

df.write.mode('overwrite').format('parquet').save("wasbs://grupo001taller3@"+storage_account+".blob.core.windows.net/Parte1Parquet2")

# COMMAND ----------

sqlContext.sql("""
               select scooter_id,avg(duracion_hrs),avg(price) from 
               (select * ,(finish_time – start_time) as duracion_hrs from df)
               """).display()

df.write.mode('overwrite').format('parquet').save("wasbs://grupo001taller3@"+storage_account+".blob.core.windows.net/Parte1Parquet3")

# COMMAND ----------

sqlContext.sql("""
               select user_id,count(*) from (
               SELECT *,(finish_time – start_time) as duracion_hrs from df 
               where year(start_time) = '2018')
               group by user_id
               order by 2 desc
               """).display()

df.write.mode('overwrite').format('parquet').save("wasbs://grupo001taller3@"+storage_account+".blob.core.windows.net/Parte1Parquet4")

# COMMAND ----------

sqlContext.sql("""
               select user_id,count(*) from (
               SELECT *,(finish_time – start_time) as duracion_hrs from df 
               where year(start_time) = '2018')
               group by user_id
               order by 2 desc
               """).display()

df.write.mode('overwrite').format('parquet').save("wasbs://grupo001taller3@"+storage_account+".blob.core.windows.net/Parte1Parquet4")

# COMMAND ----------

spark.conf.unset("fs.azure.sas."+contenedor+"."+storage_account+".blob.core.windows.net")
