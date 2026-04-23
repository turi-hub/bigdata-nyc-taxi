from kafka import KafkaProducer, KafkaConsumer
import json
import time

# Productor — manda 5 mensajes
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Enviando mensajes...")
for i in range(5):
    mensaje = {"id": i, "texto": f"viaje_{i}"}
    producer.send('test', value=mensaje)
    print(f"  Enviado: {mensaje}")

producer.flush()
print("Mensajes enviados.\n")

# Consumidor — lee los 5 mensajes
consumer = KafkaConsumer(
    'test',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    consumer_timeout_ms=5000
)

print("Leyendo mensajes...")
for mensaje in consumer:
    print(f"  Recibido: {mensaje.value}")