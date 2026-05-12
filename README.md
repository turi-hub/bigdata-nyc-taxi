# NYC Taxi — Predicción de Demanda en Tiempo Real
**Proyecto Final — Big Data | Grado en Ciencia de Datos**

Pipeline de análisis de tráfico urbano en tiempo real usando Apache Kafka y Spark Streaming sobre el dataset público de NYC Taxi Trips.

---

## Stack tecnológico

| Herramienta | Versión | Uso |
|---|---|---|
| Python | 3.11 | Lenguaje principal |
| Apache Spark (PySpark) | 3.5.4 | Procesado batch y streaming |
| Apache Kafka | 3.7.0 | Bus de mensajes en tiempo real |
| Zookeeper | 3.8.3 | Coordinación de Kafka |
| kafka-python | latest | Cliente Kafka para Python |

---

## Requisitos previos

- Acceso al contenedor de la universidad (entorno `big26`)
- Scala 2.12.18 y OpenJDK 21 (ya incluidos en el contenedor)
- Conexión a internet para la descarga inicial de Kafka

---

## Instalación del entorno

### 1. Descargar e instalar Kafka

```bash
# Descargar Kafka
wget https://archive.apache.org/dist/kafka/3.7.0/kafka_2.12-3.7.0.tgz

# Descomprimir
tar -xzf kafka_2.12-3.7.0.tgz

# Entrar en la carpeta
cd kafka_2.12-3.7.0
```

### 2. Instalar cliente Python de Kafka

```bash
pip install kafka-python --break-system-packages
```

---

## Arrancar los servicios

Cada vez que abras una nueva sesión en el contenedor tienes que arrancar Kafka manualmente. Sigue este orden:

### Paso 1 — Arrancar Zookeeper

```bash
cd ~/kafka_2.12-3.7.0
bin/zookeeper-server-start.sh config/zookeeper.properties &
```

Espera a ver esta línea en los logs antes de continuar:
```
binding to port 0.0.0.0/0.0.0.0:2181
```

### Paso 2 — Arrancar Kafka

```bash
bin/kafka-server-start.sh config/server.properties &
```

Espera a ver esta línea en los logs antes de continuar:
```
[KafkaServer id=0] started
```

### Paso 3 — Verificar que funciona

```bash
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

Si no devuelve error, Kafka está listo.

---

## Verificar el stack completo

Ejecuta el script de prueba para confirmar que productor y consumidor funcionan correctamente:

```bash
python test_kafka.py
```

Deberías ver:
```
Enviando mensajes...
  Enviado: {'id': 0, 'texto': 'viaje_0'}
  ...
Leyendo mensajes...
  Recibido: {'id': 0, 'texto': 'viaje_0'}
  ...
```

---

## Estructura del repositorio

```
proyecto-bigdata/
├── data/
│   └── README_datos.md       # Instrucciones para descargar el dataset
├── notebooks/
│   ├── 01_exploracion.ipynb  # EDA del dataset NYC Taxi
│   ├── 022_modelo_batch.ipynb # Primeras pruebas sobre el entrenamiento y ML
|   |── 02_modeloFinal.ipynb  #Modelado final y conclusiones ML del proyecto
│   ├── 03_streaming.ipynb    # Pipeline Spark Streaming + Kafka
│   └── 04_visualizacion.ipynb #Generacion de HTML para mostrar resultados del modelado vs datos reales
├── producer/
│   └── taxi_producer.py      # Script que simula el stream de viajes
│   └── prepare_stream_test_data.py      # Script que carga los datos que se usan como test para simular
├── streaming/
│   └── taxi_consumer.py      # Consumer del flujo de streaming
├── docs/
│   # Generacion de html y pngs extras durante el flujo 
├── test_kafka.py             # Script de verificación del stack
├── requirements.txt          # Dependencias Python
├── nyc_taxi_visualizacion.html          # html final de visualizacion
└── README.md                 # Este fichero

```

---

## Dataset

El dataset utilizado es el **NYC Taxi Trip Record Data** de la NYC Taxi & Limousine Commission.

- **URL oficial:** https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Formato:** Parquet, organizado por año y mes
- **Tamaño:** +1.000 millones de viajes desde 2009

Para el desarrollo se recomienda empezar con 1-2 meses de datos y escalar progresivamente.

```bash
# Ejemplo: descargar enero 2009
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2009-01.parquet
```

---

## Dependencias Python

```
kafka-python
pyspark==3.5.4
pandas
pyarrow
matplotlib
folium
```

Instalar con:
```bash
pip install -r requirements.txt --break-system-packages
```

---

## Reparto del equipo

| Fase | Responsable | Descripción |
|---|---|---|
| Infraestructura + Git | Persona 1 | Setup Kafka, README, estructura repo |
| Exploración + Modelo | Personas 1 y 2 | EDA, features, entrenamiento SparkML |
| Pipeline Streaming | Personas 3 y 4 | Productor Kafka + Spark Streaming |
| Experimentos + Docs | Todo el equipo | Escalabilidad, memoria, diapositivas |

---

## Notas importantes

- Kafka y Zookeeper deben arrancarse **cada vez** que se inicia una nueva sesión en el contenedor. No persisten entre sesiones.
- Los datos **no se suben al repositorio** por su tamaño. Seguir las instrucciones de descarga en `data/README_datos.md`.
- El notebook `03_streaming.ipynb` requiere que Kafka esté corriendo antes de ejecutarse.
