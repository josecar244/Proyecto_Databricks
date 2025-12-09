# Databricks notebook source
# MAGIC %sql
# MAGIC GRANT SELECT ON TABLE final_project_flights.golden.rpt_resumen_vuelos_diario TO `josecar244@gmail.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG final_project_flights TO `Analistas`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE SCHEMA ON SCHEMA final_project_flights.bronze TO `Desarrolladores`;
# MAGIC GRANT CREATE ON SCHEMA final_project_flights.bronze TO `Desarrolladores`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON TABLE final_project_flights.silver.airports_silver TO `Analistas`;

# COMMAND ----------

# MAGIC %sql
# MAGIC REVOKE USE SCHEMA ON SCHEMA final_project_flights.golden FROM `josecar244@gmail.com`;
