# Databricks notebook source
import random
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import pandas as pd 
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC Primero hay que crear el azure key vault
# MAGIC
# MAGIC Luego, dar roles con la cuenta de azure (correo) con rol "Agente de secretos de Key Vault" y a "azuredatabricks" con el rol "Usuario de secretos de Key Vault"
# MAGIC
# MAGIC Luego de eso, generar un secreto (se genera dentro del key value) con algun identificador y un valor a enmascarar (contraseña de BD, contraseña de usuario, etc)
# MAGIC
# MAGIC Utilizar el link sgte para la creacion de scopes. En el menu agregas un nombre de Scope, el nombre de la DNS y el ID de Recurso (se encuentran en las propiedades del Key Vault)
# MAGIC
# MAGIC Finalmente seguir instrucciones

# COMMAND ----------

###Link para crear scopes
#https://<databricks-instance>#secrets/createScope

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="scope_cguerrero")

# COMMAND ----------

password = dbutils.secrets.get(scope="scope_cguerrero", key="secret-cguerrero")

# COMMAND ----------

print(password)

# COMMAND ----------

### Crear conexion a postgreSQL 
driver = "org.postgresql.Driver"
database_host = "databricksdmc.postgres.database.azure.com"
database_port = "5432"
database_name = "northwind"
database_user = "grupo6_antony"
database_user_psw = password
table = "orders"

url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"

# COMMAND ----------

### Leer tabla en PostgreSQL
sql_order = (spark.read.format("jdbc")
             .option("driver",driver)
             .option("url",url)
             .option("dbtable",table)
             .option("user",database_user)
             .option("password",database_user_psw)
             .load())


# COMMAND ----------

sql_order.display()
