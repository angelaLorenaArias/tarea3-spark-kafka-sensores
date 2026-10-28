from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

# Crear la sesion de Spark
spark = (
    SparkSession.builder
    .appName("SensorDataAnalysis")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Definir la estructura de los datos del sensor
schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", IntegerType(), True)
])

# Leer los datos desde Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sensor_data")
    .option("startingOffsets", "latest")
    .load()
)

# Convertir los mensajes JSON a columnas
sensor_df = (
    df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    )
    .select("data.*")
)

# Calcular promedios por sensor
resultado = (
    sensor_df.groupBy("sensor_id")
    .agg(
        avg("temperature").alias("promedio_temperatura"),
        avg("humidity").alias("promedio_humedad")
    )
)

# Mostrar resultados en la terminal
consulta = (
    resultado.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .start()
)

consulta.awaitTermination()
