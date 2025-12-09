# Databricks notebook source
# MAGIC %md
# MAGIC ##Transformaciones de data

# COMMAND ----------

from pyspark.sql.functions import col, count, when, concat, concat_ws, to_timestamp, date_format, lpad, to_date, coalesce, lit, expr, format_string, length

# COMMAND ----------

# MAGIC %md
# MAGIC ###Tabla Airports

# COMMAND ----------

df_airports = spark.read.table("final_project_flights.bronze.airports")

# Inspección inicial del esquema
df_airports.printSchema()

# COMMAND ----------

df_airports = df_airports.withColumn("LATITUDE", col("LATITUDE").cast("double")) \
                         .withColumn("LONGITUDE", col("LONGITUDE").cast("double"))

# COMMAND ----------

print("--- Nulos en campos clave de AIRPORTS ---")
df_airports.select(
    count(when(col("IATA_CODE").isNull(), True)).alias("Nulos_IATA_CODE"),
    count(when(col("AIRPORT").isNull(), True)).alias("Nulos_AIRPORT_NAME"),
    count(when(col("LATITUDE").isNull(), True)).alias("Nulos_LATITUDE")
).show()

# COMMAND ----------

# Contar registros duplicados en IATA_CODE
df_airports.groupBy("IATA_CODE").agg(count("*").alias("count")) \
           .filter("count > 1").show()

# Contar nulos en IATA_CODE
df_airports.select(count(when(col("IATA_CODE").isNull(), True)).alias("Nulos_IATA")).show()

# COMMAND ----------

# Rellenamos los datos null con los datos reales
df_airports = df_airports.withColumn(
    "LATITUDE",
    when(col("IATA_CODE") == "UST", 29.9555)
    .when(col("IATA_CODE") == "PBG", 44.65094)
    .when(col("IATA_CODE") == "ECP", 30.35824)
    .otherwise(col("LATITUDE"))
).withColumn(
    "LONGITUDE",
    when(col("IATA_CODE") == "UST", -81.3372)
    .when(col("IATA_CODE") == "PBG", -73.46814)
    .when(col("IATA_CODE") == "ECP", -85.79560)
    .otherwise(col("LONGITUDE"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Tabla Airlines

# COMMAND ----------

df_airlines = spark.read.table("final_project_flights.bronze.airlines")

# Inspección inicial del esquema
df_airlines.printSchema()

# COMMAND ----------

df_airlines = df_airlines.withColumnRenamed("AIRLINE", "AIRLINE_NAME")

# COMMAND ----------

# Contar registros duplicados en IATA_CODE (código de aerolínea)
df_airlines.groupBy("IATA_CODE").agg(count("*").alias("count")) \
           .filter("count > 1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Tabla Flights

# COMMAND ----------

df_flights = spark.read.table("final_project_flights.bronze.flights")


# COMMAND ----------

# 1. GENERAR EL TIMESTAMP
df_flights = df_flights.withColumn("SCHEDULED_DEPARTURE_TS", 
    to_timestamp(
        concat(
            col("YEAR"), 
            lpad(col("MONTH").cast("string"), 2, "0"),
            lpad(col("DAY").cast("string"), 2, "0"),
            lpad(col("SCHEDULED_DEPARTURE").cast("string"), 4, "0")
        ), 
        "yyyyMMddHHmm"
    )
)

# 2. LIMPIEZA DE COLUMNAS ORIGINALES
df_flights = df_flights.drop("YEAR", "MONTH", "DAY", "SCHEDULED_DEPARTURE")

# 3. CORRECCIÓN DE TIPOS NUMÉRICOS
df_flights = df_flights.withColumn("DISTANCE", col("DISTANCE").cast("integer")) \
                       .withColumn("AIR_TIME", col("AIR_TIME").cast("integer")) \
                       .withColumn("DEPARTURE_DELAY", col("DEPARTURE_DELAY").cast("integer")) \
                       .withColumn("ARRIVAL_DELAY", col("ARRIVAL_DELAY").cast("integer"))

# COMMAND ----------

df_flights = df_flights.withColumn("ELAPSED_TIME", 
    when((col("CANCELLED") == 1) | (col("DIVERTED") == 1), 0)
    .otherwise(col("ELAPSED_TIME")).cast("integer")
)

df_flights = df_flights.withColumn("AIR_TIME", 
    when((col("CANCELLED") == 1) | (col("DIVERTED") == 1), 0)
    .otherwise(col("AIR_TIME")).cast("integer")
)

df_flights = df_flights.withColumn("TAXI_OUT", 
    when(col("CANCELLED") == 1, 0)
    .otherwise(col("TAXI_OUT")).cast("integer")
)

df_flights = df_flights.withColumn("TAXI_IN", 
    when(col("CANCELLED") == 1, 0)
    .otherwise(col("TAXI_IN")).cast("integer")
)

df_flights = df_flights.filter(
    (col("ORIGIN_AIRPORT").isNotNull()) & (col("DESTINATION_AIRPORT").isNotNull())
)

# COMMAND ----------

def create_scheduled_arrival_timestamp(df, time_col_name, new_col_name):
    date_str_col = date_format(col("SCHEDULED_DEPARTURE_TS"), "yyyy-MM-dd")
    time_str_col = lpad(col(time_col_name).cast("string"), 4, "0")
    
    df = df.withColumn(new_col_name, 
        to_timestamp(
            concat(date_str_col, time_str_col), 
            "yyyy-MM-ddHHmm"
        )
    )
    return df

# A. CONVERSIÓN DE TIEMPOS PROGRAMADOS Y REALES
df_flights = create_scheduled_arrival_timestamp(df_flights, "SCHEDULED_ARRIVAL", "SCHEDULED_ARRIVAL_TS")

# [DEPARTURE_TIME_TS] Cálculo de Salida Real
df_flights = df_flights.withColumn("DEPARTURE_TIME_TS", 
    col("SCHEDULED_DEPARTURE_TS") + col("DEPARTURE_DELAY").cast("bigint") * expr("INTERVAL 1 MINUTE")
)

# [ARRIVAL_TIME_TS] Cálculo de Llegada Real
df_flights = df_flights.withColumn("ARRIVAL_TIME_TS", 
    col("SCHEDULED_ARRIVAL_TS") + col("ARRIVAL_DELAY").cast("bigint") * expr("INTERVAL 1 MINUTE")
)

# B. LIMPIEZA DE DATOS DE TEXTO
df_flights = df_flights.withColumn("CANCELLATION_REASON", 
    coalesce(col("CANCELLATION_REASON"), lit("NONE"))
)

# C. ELIMINACIÓN DE COLUMNAS OBSOLETAS
columns_to_drop_final = [
    "DEPARTURE_TIME", 
    "WHEELS_OFF", 
    "WHEELS_ON", 
    "SCHEDULED_ARRIVAL", 
    "ARRIVAL_TIME"
]

df_flights = df_flights.drop(*columns_to_drop_final)

# COMMAND ----------

# A. Unir Vuelos y Aerolíneas
df_final = df_flights.join(
    df_airlines, 
    df_flights.AIRLINE == df_airlines.IATA_CODE, 
    "left"
).drop(df_airlines.IATA_CODE)

# B. Unir con Aeropuertos (Origen)
airports_origin = df_airports.select(
    col("IATA_CODE").alias("ORIGIN_CODE"),
    col("CITY").alias("ORIGIN_CITY"),
    col("STATE").alias("ORIGIN_STATE"),
    col("LATITUDE").alias("ORIGIN_LAT") ,
    col("LONGITUDE").alias("ORIGIN_LON")
)

df_final = df_final.join(
    airports_origin, 
    df_final.ORIGIN_AIRPORT == airports_origin.ORIGIN_CODE, 
    "left"
).drop("ORIGIN_CODE")

# C. Unir con Aeropuertos (Destino)
airports_destination = df_airports.select(
    col("IATA_CODE").alias("DESTINATION_CODE"),
    col("CITY").alias("DESTINATION_CITY"),
    col("STATE").alias("DESTINATION_STATE"),
    col("LATITUDE").alias("DESTINATION_LAT"),
    col("LONGITUDE").alias("DESTINATION_LON")  
)

df_final = df_final.join(
    airports_destination, 
    df_final.DESTINATION_AIRPORT == airports_destination.DESTINATION_CODE, 
    "left"
).drop("DESTINATION_CODE")

df_final = df_final.filter(
    (length(col("ORIGIN_AIRPORT")) == 3) & 
    (length(col("DESTINATION_AIRPORT")) == 3)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inserción de datos transformados a silver

# COMMAND ----------

df_final.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("final_project_flights.silver.flights_silver")

df_airports.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("final_project_flights.silver.airports_silver")
