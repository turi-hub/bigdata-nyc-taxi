"""
Exporta streaming_output/ a un JSON compacto para la visualización web.
Uso:
    python export_streaming_json.py
Genera streaming_viz.json en el directorio actual.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json

STREAMING_OUTPUT = "/home/alumno/Desktop/bigdata-nyc-taxi/streaming_output/"
OUTPUT_JSON = "/home/alumno/Desktop/bigdata-nyc-taxi/streaming_viz.json"

spark = SparkSession.builder.master("local[*]").appName("export-viz").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(STREAMING_OUTPUT)

# Redondear coordenadas para consistencia
df = df.withColumn("zone_lon", F.round("zone_lon", 2)) \
       .withColumn("zone_lat", F.round("zone_lat", 2)) \
       .withColumn("window_start", F.date_format("window_start", "yyyy-MM-dd HH:mm:ss"))

# Recoger datos ordenados por ventana
rows = df.orderBy("window_start", "zone_lon", "zone_lat").collect()

# Agrupar por ventana temporal
windows = {}
for row in rows:
    ts = row["window_start"]
    if ts not in windows:
        windows[ts] = []
    windows[ts].append({
        "lon": row["zone_lon"],
        "lat": row["zone_lat"],
        "real": round(float(row["trip_count"]), 1),
        "pred": round(float(row["prediction"]), 1),
    })

# Calcular MAE global por ventana
mae_by_window = {}
for ts, zones in windows.items():
    errors = [abs(z["real"] - z["pred"]) for z in zones]
    mae_by_window[ts] = round(sum(errors) / len(errors), 2)

result = {
    "timestamps": sorted(windows.keys()),
    "windows": windows,
    "mae": mae_by_window,
}

# Al final del export_streaming_json.py, en lugar de guardar JSON por separado:

json_str = json.dumps(result, separators=(",", ":"))

# Leer la plantilla HTML
with open("/home/alumno/Desktop/bigdata-nyc-taxi/nyc_taxi_viz.html", "r") as f:
    html = f.read()

# Inyectar los datos dentro del HTML
html = html.replace(
    "fetch('streaming_viz.json')",
    f"Promise.resolve({json_str})"
)

# Guardar HTML autocontenido
output_html = "/home/alumno/Desktop/bigdata-nyc-taxi/nyc_taxi_viz_standalone.html"
with open(output_html, "w") as f:
    f.write(html)

print(f"HTML autocontenido generado: {output_html}")
