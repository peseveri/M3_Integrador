import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, explode, avg, max
import os

# ============================
# 1️⃣ Inicializar Spark
# ============================
spark = SparkSession.builder \
    .appName("ETL_Clima") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set(
    "fs.s3a.aws.credentials.provider",
    "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
)

# Configuración adicional para evitar problemas
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
spark.conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")

# ============================
# 2️⃣ Definir rutas S3
# ============================
s3_path_bronze = "s3a://henry-m3-pedro/bronze/"
s3_path_silver = "s3a://henry-m3-pedro/silver/"
s3_path_gold = "s3a://henry-m3-pedro/gold/"

# ============================
# 3️⃣ Leer datos Bronze
# ============================
df_bronze = spark.read.parquet(f"{s3_path_bronze}*/*.parquet")

# Separar por ciudad
df_patagonia = df_bronze.filter(df_bronze["city_name"] == "Patagonia")
df_riohacha = df_bronze.filter(df_bronze["city_name"] == "Riohacha")

# ============================
# 4️⃣ Transformación: Bronze → Silver
# ============================
def bronze_to_silver(df):
    df_silver = df \
        .withColumn("timestamp", to_timestamp(col("dt_iso"))) \
        .withColumn("date", to_date(col("dt_iso"))) \
        .withColumn("weather", explode(col("weather"))) \
        .select(
            "city_name",
            "timestamp",
            "date",
            col("main.temp").alias("temp"),
            col("main.humidity").alias("humidity"),
            col("wind.speed").alias("wind_speed"),
            col("weather.main").alias("weather_main"),
            col("weather.description").alias("weather_description")
        )
    return df_silver

df_patagonia_silver = bronze_to_silver(df_patagonia)
df_riohacha_silver = bronze_to_silver(df_riohacha)

# Guardar Silver
df_patagonia_silver.write.mode("overwrite").parquet(f"{s3_path_silver}Patagonia/")
df_riohacha_silver.write.mode("overwrite").parquet(f"{s3_path_silver}Riohacha/")

# ============================
# 5️⃣ Agregación: Silver → Gold
# ============================
def silver_to_gold(df_silver):
    df_gold = df_silver.groupBy("city_name", "date") \
        .agg(
            avg("temp").alias("avg_temp"),
            avg("humidity").alias("avg_humidity"),
            max("wind_speed").alias("max_wind_speed"),
            # Conteo de eventos de clima
            # count("weather_main").alias("weather_events_count")
        )
    return df_gold

df_patagonia_gold = silver_to_gold(df_patagonia_silver)
df_riohacha_gold = silver_to_gold(df_riohacha_silver)

# Guardar Gold
df_patagonia_gold.write.mode("overwrite").parquet(f"{s3_path_gold}Patagonia/")
df_riohacha_gold.write.mode("overwrite").parquet(f"{s3_path_gold}Riohacha/")

# ============================
# 6️⃣ Finalizar Spark
# ============================
spark.stop()
print("Proceso ETL finalizado.")
