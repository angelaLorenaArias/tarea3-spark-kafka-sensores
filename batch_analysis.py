import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round

spark = (
    SparkSession.builder
    .appName("AnalisisBatchSensores")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Cargar el archivo historico
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ruta = os.path.join(BASE_DIR, "sensores_historicos.csv")

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(ruta)
)

print("\n===== ANALISIS BATCH DE SENSORES =====")
print("Registros originales:", df.count())

# Limpiar datos incompletos y valores fuera de rango
limpio = (
    df.dropna(subset=[
        "sensor_id", "temperature", "humidity", "timestamp"
    ])
    .filter(
        (col("temperature") >= 0) &
        (col("temperature") <= 60) &
        (col("humidity") >= 0) &
        (col("humidity") <= 100)
    )
)

print("Registros despues de limpiar:", limpio.count())

# Estadisticas por sensor
resumen = (
    limpio.groupBy("sensor_id")
    .agg(
        round(avg("temperature"), 2).alias("promedio_temperatura"),
        round(avg("humidity"), 2).alias("promedio_humedad"),
        count("*").alias("cantidad_mediciones")
    )
    .orderBy("sensor_id")
)

print("\n===== RESULTADOS POR SENSOR =====")
resumen.show(20, truncate=False)

# Guardar los resultados en CSV
resumen.coalesce(1).write.mode("overwrite").option(
    "header", "true"
).csv(os.path.join(BASE_DIR, "resultados_batch"))

# Generar graficas
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    filas = resumen.collect()
    sensores = [str(f["sensor_id"]) for f in filas]
    temperaturas = [float(f["promedio_temperatura"]) for f in filas]
    humedades = [float(f["promedio_humedad"]) for f in filas]

    plt.figure(figsize=(8, 5))
    plt.bar(sensores, temperaturas)
    plt.title("Temperatura promedio por sensor")
    plt.xlabel("Sensor")
    plt.ylabel("Temperatura (grados C)")
    plt.tight_layout()
    plt.savefig(os.path.join(BASE_DIR, "grafica_temperatura.png"))
    plt.close()

    plt.figure(figsize=(8, 5))
    plt.bar(sensores, humedades)
    plt.title("Humedad promedio por sensor")
    plt.xlabel("Sensor")
    plt.ylabel("Humedad (%)")
    plt.tight_layout()
    plt.savefig(os.path.join(BASE_DIR, "grafica_humedad.png"))
    plt.close()

    print("Graficas guardadas correctamente.")

except ImportError:
    print("Falta matplotlib; debemos instalarlo para crear las graficas.")

spark.stop()
