# Análisis de sensores ambientales con Apache Spark y Kafka

Proyecto práctico de Big Data que demuestra dos formas de procesar mediciones simuladas de temperatura y humedad:

- **Batch:** lectura de un CSV histórico, limpieza básica, análisis exploratorio y promedios por sensor con PySpark.
- **Tiempo real:** un productor Python envía eventos al topic `sensor_data` de Kafka y Spark Structured Streaming calcula promedios de temperatura y humedad por sensor.

> **Nota sobre los datos:** `sensores_historicos.csv` contiene 1.000 registros simulados con fines académicos. No corresponde a mediciones reales de una instalación física. Los datos de Kafka también se generan aleatoriamente.

## Archivos del repositorio

- `sensores_historicos.csv`: 1.000 registros históricos simulados.
- `batch_analysis.py`: limpieza, estadísticas, resultados CSV y gráficas del procesamiento por lotes.
- `kafka_producer.py`: genera una medición simulada cada segundo y la envía a Kafka.
- `spark_streaming_consumer.py`: consume eventos del topic `sensor_data` y muestra estadísticas en tiempo real.
- `grafica_temperatura.png` y `grafica_humedad.png`: ejemplos de visualizaciones batch generadas durante las pruebas.
- `arquitectura_kafka.png`: diagrama ilustrativo de la arquitectura de Kafka.

## Requisitos del entorno

Las instrucciones siguientes reflejan el entorno de desarrollo usado en la práctica:

- Ubuntu en una máquina virtual.
- Python 3.
- Apache Spark 3.5.8 instalado en `/opt/spark`.
- Apache Kafka 3.9.2 instalado en `/opt/Kafka`, con la configuración ZooKeeper de esta instalación.
- Java compatible con Spark/Kafka (en las pruebas se usó Java 11).
- Paquete Python `kafka-python` para el productor y `matplotlib` para las gráficas.
- Conexión a Internet la primera vez que `spark-submit --packages` descarga el conector Spark-Kafka.

## 1. Instalar paquetes de Python

```bash
python3 -m pip install --user kafka-python matplotlib
```

## 2. Ejecutar el procesamiento batch

Desde la carpeta del repositorio:

```bash
/opt/spark/bin/spark-submit batch_analysis.py
```

El programa muestra el número de registros originales y limpios, estadísticas descriptivas y una tabla con promedio de temperatura, promedio de humedad y cantidad de mediciones por sensor. También crea:

- `resultados_batch/`: resultados tabulares en CSV (directorio de salida de Spark).
- `grafica_temperatura.png`: gráfica de temperatura promedio por sensor.
- `grafica_humedad.png`: gráfica de humedad promedio por sensor.

El conjunto de datos inicial contiene 1.000 registros y, con los filtros implementados, se espera que los 1.000 pasen la limpieza porque fueron generados dentro de los rangos configurados.

## 3. Iniciar ZooKeeper y Kafka

En la instalación utilizada en la práctica, abrir una terminal y ejecutar:

```bash
sudo /opt/Kafka/bin/zookeeper-server-start.sh -daemon /opt/Kafka/config/zookeeper.properties
sudo /opt/Kafka/bin/kafka-server-start.sh -daemon /opt/Kafka/config/server.properties
```

Comprobar los puertos:

```bash
ss -lntp | grep -E '2181|9092'
```

Listar topics existentes:

```bash
/opt/Kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

Crear el topic solo si todavía no existe:

```bash
/opt/Kafka/bin/kafka-topics.sh --create --topic sensor_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## 4. Ejecutar el procesamiento en tiempo real

Usar tres terminales, manteniendo los procesos activos:

**Terminal 1 — productor:**

```bash
python3 kafka_producer.py
```

**Terminal 2 — Spark Streaming:**

```bash
/opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 spark_streaming_consumer.py
```

El conector se descarga desde Maven la primera vez, por lo que se necesita conexión a Internet. El consumidor está configurado con `startingOffsets=latest`; empieza a leer los mensajes nuevos que lleguen al topic después de iniciar la consulta.

**Terminal 3 — (opcional) comprobar mensajes de Kafka:**

```bash
/opt/Kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sensor_data --from-beginning
```

Para detener cada proceso interactivo, usa `Ctrl + C` en la terminal correspondiente. Para volver a ejecutar la prueba después de apagar la máquina virtual, inicia de nuevo ZooKeeper y Kafka.

## Resultados de referencia de la prueba batch

En la ejecución documentada se procesaron 1.000 registros, se conservaron 1.000 tras la limpieza y se calcularon promedios para los diez sensores. Los promedios observados estaban aproximadamente entre 24,63 °C y 25,05 °C para temperatura. Los resultados pueden cambiar si se modifica o vuelve a generar el CSV.

## Alcance y limitaciones

Esta es una demostración académica en un entorno local y con datos simulados de 1.000 filas. Sirve para comprobar el flujo de procesamiento batch y streaming, pero no representa por sí sola un benchmark de rendimiento ni un despliegue de producción para datos masivos.
