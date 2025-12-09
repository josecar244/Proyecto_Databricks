# Databricks notebook source
# MAGIC %md
# MAGIC ###Creación de widgets

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("storage_name", ".")
dbutils.widgets.text("container", ".")
dbutils.widgets.text("catalogo", ".")
dbutils.widgets.text("esquema", ".")

# COMMAND ----------

storage_name = dbutils.widgets.get("storage_name")
container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

ruta_airlines = f"abfss://{container}@{storage_name}.dfs.core.windows.net/raw/airlines.csv"
ruta_airports = f"abfss://{container}@{storage_name}.dfs.core.windows.net/raw/airports.csv"
ruta_flights = f"abfss://{container}@{storage_name}.dfs.core.windows.net/raw/flights.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inserción de datos desde raw a bronze

# COMMAND ----------

df_airlines = spark.read.option("header", True).option("inferSchema", True).csv(ruta_airlines)
df_airlines.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.airlines")

# COMMAND ----------

df_airports = spark.read.option("header", True).option("inferSchema", True).csv(ruta_airports)\
                        .withColumn("LATITUDE", col("LATITUDE").cast("string")) \
                        .withColumn("LONGITUDE", col("LONGITUDE").cast("string"))
df_airports.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.airports")

# COMMAND ----------

df_flights = spark.read.option("header", True).option("inferSchema", True).csv(ruta_flights)
df_flights.write.mode("append").saveAsTable(f"{catalogo}.{esquema}.flights")
