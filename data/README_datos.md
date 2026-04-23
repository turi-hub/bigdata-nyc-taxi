# Datos — NYC Taxi Trip Record Data

Los datos NO se suben al repositorio por su tamaño. Cada miembro los descarga en local.

## Fuente oficial

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Cómo descargar

```bash
# Enero 2009 (punto de partida del proyecto)
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2009-01.parquet

# Para más meses cambia año y mes: yellow_tripdata_YYYY-MM.parquet
```

## Dónde guardarlos

Guarda los archivos en esta carpeta `data/` en local. El .gitignore ya está configurado para que no se suban accidentalmente.

## Columnas relevantes para el proyecto

- `Trip_Pickup_DateTime` — hora de recogida
- `Start_Lon` / `Start_Lat` — coordenadas de origen
- `Trip_Distance` — distancia del viaje
- `Passenger_Count` — número de pasajeros
