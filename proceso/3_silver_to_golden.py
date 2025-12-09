# Databricks notebook source
from pyspark.sql.functions import sequence, explode, date_trunc, dayofweek, year, month, dayofmonth, weekofyear, quarter, date_format, min, max, col, col, count, sum, avg, round, when, to_date, date_format, lit, expr, greatest,coalesce
from pyspark.sql.types import DateType
from datetime import timedelta, date

# COMMAND ----------

# MAGIC %md
# MAGIC ##Tabla Maestra Tiempo

# COMMAND ----------

df_dates = spark.read.table("final_project_flights.silver.flights_silver").select(
    min(col("SCHEDULED_DEPARTURE_TS")).alias("start_date"), 
    max(col("SCHEDULED_DEPARTURE_TS")).alias("end_date")
).collect()

start_date = df_dates[0]['start_date'].date()
end_date = df_dates[0]['end_date'].date()

# COMMAND ----------

# 1. Crear un DataFrame con la secuencia de fechas
df_time = spark.range(1).select(
    explode(
        sequence(
            date_trunc('DAY', lit(start_date).cast(DateType())),
            date_trunc('DAY', lit(end_date).cast(DateType())),
            expr('INTERVAL 1 DAY')
        )
    ).alias("date")
)

# 2. Aplicar transformaciones para extraer atributos
df_time = df_time.withColumn("DATE_KEY", date_format(col("date"), "yyyyMMdd").cast("integer")) \
                 .withColumn("DAY_OF_WEEK", dayofweek(col("date"))) \
                 .withColumn("DAY_NAME", date_format(col("date"), "EEEE")) \
                 .withColumn("DAY_OF_MONTH", dayofmonth(col("date"))) \
                 .withColumn("WEEK_OF_YEAR", weekofyear(col("date"))) \
                 .withColumn("MONTH", month(col("date"))) \
                 .withColumn("QUARTER", quarter(col("date"))) \
                 .withColumn("YEAR", year(col("date"))) \
                 .withColumn("FULL_DATE", col("date").cast("date")) \
                 .drop("date")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Tabla Hechos Resumen Diario

# COMMAND ----------

df_flights_silver = spark.read.table("final_project_flights.silver.flights_silver")

# 1. CÁLCULOS DE KPIS

# MOTIVO DEL RETRASO
df_flights_base = df_flights_silver.withColumn(
    "MAYOR_CAUSA_RETRASO",
    coalesce(
        when(greatest(col("AIR_SYSTEM_DELAY"), col("SECURITY_DELAY"), col("AIRLINE_DELAY"), col("LATE_AIRCRAFT_DELAY"), col("WEATHER_DELAY")) == col("AIRLINE_DELAY"), lit("Aerolínea (Propio)")),
        when(greatest(col("AIR_SYSTEM_DELAY"), col("SECURITY_DELAY"), col("AIRLINE_DELAY"), col("LATE_AIRCRAFT_DELAY"), col("WEATHER_DELAY")) == col("LATE_AIRCRAFT_DELAY"), lit("Aerolínea (Aeronave Tarde)")),
        when(greatest(col("AIR_SYSTEM_DELAY"), col("SECURITY_DELAY"), col("AIRLINE_DELAY"), col("LATE_AIRCRAFT_DELAY"), col("WEATHER_DELAY")) == col("AIR_SYSTEM_DELAY"), lit("Sistema Aéreo")),
        when(greatest(col("AIR_SYSTEM_DELAY"), col("SECURITY_DELAY"), col("AIRLINE_DELAY"), col("LATE_AIRCRAFT_DELAY"), col("WEATHER_DELAY")) == col("WEATHER_DELAY"), lit("Clima")),
        when(greatest(col("AIR_SYSTEM_DELAY"), col("SECURITY_DELAY"), col("AIRLINE_DELAY"), col("LATE_AIRCRAFT_DELAY"), col("WEATHER_DELAY")) == col("SECURITY_DELAY"), lit("Seguridad")),
        lit("No Aplica / Sin Retraso Mayor")
    )
)

# 2. AGREGACIÓN PARA RPT_RESUMEN_VUELOS_DIARIO

df_flights_daily = df_flights_base.withColumn(
    "SCHEDULED_DATE",
    to_date(col("SCHEDULED_DEPARTURE_TS"))
).withColumn(
    "DATE_KEY",
    date_format(col("SCHEDULED_DATE"), "yyyyMMdd").cast("integer")
)

df_daily_summary = df_flights_daily.groupBy(
    col("DATE_KEY"),
    col("SCHEDULED_DATE"),
    col("AIRLINE"),
    col("MAYOR_CAUSA_RETRASO")
).agg(
    count("*").alias("TOTAL_FLIGHTS"),
    sum(col("DISTANCE")).alias("TOTAL_DISTANCE_MILES"),
    sum(col("CANCELLED")).alias("TOTAL_CANCELLED"),
    sum(col("DIVERTED")).alias("TOTAL_DIVERTED"),
    round(avg(col("DEPARTURE_DELAY")), 2).alias("AVG_DEPARTURE_DELAY_MIN"),
    round(avg(col("ARRIVAL_DELAY")), 2).alias("AVG_ARRIVAL_DELAY_MIN"),
    sum(when(col("ARRIVAL_DELAY") <= 15, 1).otherwise(0)).alias("FLIGHTS_ON_TIME_COUNT")
)

# 3. CÁLCULO DE TASAS Y RATIO

df_daily_summary = df_daily_summary.withColumn(
    "CANCELLATION_RATE",
    round(col("TOTAL_CANCELLED") / col("TOTAL_FLIGHTS"), 4)
).withColumn(
    "ON_TIME_PERFORMANCE_RATE",
    round(col("FLIGHTS_ON_TIME_COUNT") / col("TOTAL_FLIGHTS"), 4)
)

# COMMAND ----------

df_flights_silver = spark.read.table("final_project_flights.silver.flights_silver")

# 1. CÁLCULOS DE KPIS PRE-AGREGACIÓN

# TIPO_RUTA (Nueva Dimensión de Agrupación)
df_flights_route = df_flights_silver.withColumn("TIPO_RUTA",
    when(col("DISTANCE") <= 800, lit("Corta (<= 800 mi)"))
    .when(col("DISTANCE") <= 2500, lit("Media (801 - 2500 mi)"))
    .otherwise(lit("Larga (> 2500 mi)"))
)

# 2. PREPARACIÓN DE FECHA Y AGREGACIÓN POR RUTA

df_flights_route = df_flights_route.withColumn(
    "SCHEDULED_DATE", 
    to_date(col("SCHEDULED_DEPARTURE_TS"))
).withColumn(
    "YEAR_MONTH", 
    date_format(col("SCHEDULED_DATE"), "yyyyMM").cast("integer")
)

df_route_performance = df_flights_route.groupBy(
    col("YEAR_MONTH"),
    col("AIRLINE"),
    col("ORIGIN_AIRPORT"),
    col("DESTINATION_AIRPORT"),
    col("TIPO_RUTA") # Agrupamiento por la nueva dimensión
).agg(
    # Métricas ya existentes
    count("*").alias("TOTAL_FLIGHTS_ROUTE"),
    round(avg(col("AIR_TIME")), 2).alias("AVG_AIR_TIME_MIN"),
    round(avg(col("DEPARTURE_DELAY")), 2).alias("AVG_DEPARTURE_DELAY_MIN"),
    round(avg(col("ARRIVAL_DELAY")), 2).alias("AVG_ARRIVAL_DELAY_MIN"),
    round(avg(col("DISTANCE")), 0).alias("AVG_DISTANCE_MILES")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inserción de datos a golden

# COMMAND ----------

# Tabla Tiempo
df_time.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("final_project_flights.golden.TM_TIEMPO")

# COMMAND ----------

# Tabla Aeropuesto
df_airports_golden = spark.read.table("final_project_flights.silver.airports_silver")

df_airports_golden.select(
    col("IATA_CODE").alias("AIRPORT_KEY"),
    col("AIRPORT"),
    col("CITY"),
    col("STATE"),
    col("LATITUDE").alias("GEO_LAT"),
    col("LONGITUDE").alias("GEO_LONG")
).write.format("delta") \
 .mode("overwrite") \
 .saveAsTable("final_project_flights.golden.TM_AIRPORTS")

# COMMAND ----------

# Tabla Resumen diario
df_daily_summary.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("final_project_flights.golden.RPT_RESUMEN_VUELOS_DIARIO")

# COMMAND ----------

# Tabla Performance por Ruta
df_route_performance.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("final_project_flights.golden.TR_PERFORMANCE_RUTA")
