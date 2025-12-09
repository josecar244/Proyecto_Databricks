# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("storageName","adlsmartdata2446")
dbutils.widgets.text("containerName","lhclproject")
dbutils.widgets.text("catalogo","final_project_flights")

# COMMAND ----------

storageName = dbutils.widgets.get("storageName")
nameContainer = dbutils.widgets.get("containerName")
catalogo = dbutils.widgets.get("catalogo")

ruta = f"abfss://{nameContainer}@{storageName}.dfs.core.windows.net"

# COMMAND ----------

catalogo

# COMMAND ----------

# MAGIC %md
# MAGIC ### Eliminacion tablas Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLES
# MAGIC DROP TABLE IF EXISTS ${catalogo}.bronze.airlines;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.bronze.airports;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.bronze.flights;

# COMMAND ----------

## REMOVE DATA (Bronze)
dbutils.fs.rm(f"{ruta}/bronze/airlines", True)
dbutils.fs.rm(f"{ruta}/bronze/airports", True)
dbutils.fs.rm(f"{ruta}/bronze/flights", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Eliminacion tablas Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLES
# MAGIC DROP TABLE IF EXISTS ${catalogo}.silver.airports_silver;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.silver.flights_silver;

# COMMAND ----------

## REMOVE DATA (Silver)
dbutils.fs.rm(f"{ruta}/silver/airports", True)
dbutils.fs.rm(f"{ruta}/silver/flights", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Eliminacion tablas Golden

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLES
# MAGIC DROP TABLE IF EXISTS ${catalogo}.golden.TM_TIEMPO;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.golden.TM_AIRPORTS;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.golden.RPT_RESUMEN_VUELOS_DIARIO;
# MAGIC DROP TABLE IF EXISTS ${catalogo}.golden.TR_PERFORMANCE_RUTA;

# COMMAND ----------

## REMOVE DATA (Golden)
dbutils.fs.rm(f"{ruta}/golden/tm_tiempo", True)
dbutils.fs.rm(f"{ruta}/golden/tm_airports", True)
dbutils.fs.rm(f"{ruta}/golden/rpt_resumen_vuelos_diario", True)
dbutils.fs.rm(f"{ruta}/golden/tr_performance_ruta", True)
