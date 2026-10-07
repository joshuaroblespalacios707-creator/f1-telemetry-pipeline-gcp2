**🇲🇽 Español** | [🇬🇧 English](#-f1-real-time-telemetry-pipeline-on-google-cloud-platform)

# 🏎️ Pipeline de Telemetría en Tiempo Real: EA Sports F1 ➡️ Google Cloud Platform

*Construido para F1 25 — adaptable a cualquier simulador de carreras con salida de telemetría UDP.*

Documentación técnica de la arquitectura, despliegue y operación de un pipeline de ingesta y análisis de telemetría en tiempo real, desde el simulador de Fórmula 1 hacia Google Cloud Platform (GCP). Captura velocidad, RPM, freno, temperatura de neumáticos y frenos en las cuatro esquinas, además de eventos de sesión (circuito y vuelta).

> 🚧 **Estado: Beta abierta.** Este proyecto nació para F1 25, pero el protocolo UDP de telemetría es similar en otros juegos de carreras (iRacing, Assetto Corsa, Forza, Gran Turismo, otras entregas de F1). Si corres otro título con salida de telemetría UDP, adapta el parser (`f1_publisher.py`) a tu juego y manda un PR. La idea es que esto sea una base que cualquier sim racer pueda usar y mejorar.

---

## 🤝 Cómo Contribuir / Adaptar a Otro Juego

1. **Identifica el formato UDP de tu juego** — la mayoría de los simuladores documentan su estructura de paquetes (busca "UDP telemetry format" + el nombre de tu juego).
2. **Ajusta el `struct.unpack`** en `f1_publisher.py` al layout de bytes de tu juego.
3. **Adapta la vista de BigQuery** (`v_f1_telemetry_live`) si tu juego expone métricas distintas.
4. **Documenta tu adaptación** en un fork o PR, idealmente nombrando el archivo con el juego (ej. `iracing_publisher.py`).

---

## 📐 1. Arquitectura del Sistema

```
+-----------------------------------------------------------------------------------+
|                                 ORIGEN DE DATOS                                   |
|   EA Sports F1 (Simulador PC)                                                     |
|   - Formato UDP: F1 2025 (hasta 60 Hz)                                            |
|   - Puerto: UDP 20777                                                             |
+-----------------------------------------------------------------------------------+
                                          │
                                 (Red Local / Internet)
                                          │
                                          ▼
+-----------------------------------------------------------------------------------+
|                            INGESTA EN LA NUBE (GCP)                               |
|   Compute Engine (VM: f1-telemetry-server)                                        |
|   - Demonio systemd: f1-telemetry.service                                         |
|   - Publisher Python: ~/f1_publisher.py                                           |
|   - Sockets UDP y desempaquetado binario (struct)                                 |
+-----------------------------------------------------------------------------------+
                                          │
                               (Mensajería asíncrona)
                                          │
                                          ▼
+-----------------------------------------------------------------------------------+
|                            STREAMING Y PUENTE DE DATOS                            |
|   Cloud Pub/Sub                                                                   |
|   - Topic: f1-telemetry-topic                                                     |
|   - Suscripción BigQuery: f1-telemetry-bq-sub                                     |
+-----------------------------------------------------------------------------------+
                                          │
                                 (Carga directa a BigQuery)
                                          │
                                          ▼
+-----------------------------------------------------------------------------------+
|                       ALMACENAMIENTO Y CAPA ANALÍTICA                             |
|   BigQuery (<PROJECT_ID>)                                                         |
|   - Dataset: f1_dataset                                                           |
|   - Tabla JSON cruda: telemetry_table                                             |
|   - Vista resiliente: v_f1_telemetry_live                                         |
+-----------------------------------------------------------------------------------+
```

---

## 🛠️ 2. Componentes e Infraestructura

| Componente | Recurso / Nombre | Descripción |
|---|---|---|
| Origen UDP | EA Sports F1 | Emite paquetes binarios estructurados de telemetría a hasta 60 Hz en el puerto 20777 |
| Servidor receptor | Compute Engine (`f1-telemetry-server`) | VM Linux que escucha sockets UDP y empaqueta los datos como JSON |
| Demonio de sistema | `f1-telemetry.service` | Servicio systemd que ejecuta en segundo plano el script `f1_publisher.py` |
| Ingesta streaming | Cloud Pub/Sub (`f1-telemetry-topic`) | Topic de mensajería asíncrona que desacopla la ingesta del almacenamiento |
| Suscripción BigQuery | `f1-telemetry-bq-sub` | Suscripción nativa de Pub/Sub que escribe cada mensaje JSON directamente en BigQuery |
| Almacenamiento | BigQuery (`telemetry_table`) | Tabla en bruto (JSON) que registra cada evento transmitido |
| Capa de abstracción | Vista SQL (`v_f1_telemetry_live`) | Vista con lógica `COALESCE` que parsea estructuras JSON dinámicas y expone tipos nativos (`INT64`, `FLOAT64`, `TIMESTAMP`) |

---

## 🚀 3. Guía de Despliegue y Configuración

### Paso 1 — Configurar el Simulador de F1

En los ajustes de telemetría del juego:

| Opción | Valor |
|---|---|
| UDP Telemetry | On |
| UDP Format | 2025 (o acorde a la versión del parser) |
| UDP Target IP | IP pública de la VM (`f1-telemetry-server`) |
| UDP Port | 20777 |
| UDP Send Rate | 60Hz (o 20Hz según el ancho de banda deseado) |

---

### Paso 2 — Permisos IAM en GCP

Asignar el rol de edición en BigQuery a la cuenta de servicio gestionada de Pub/Sub:

```bash
PROJECT_ID="<PROJECT_ID>"
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
PUBSUB_SA="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${PUBSUB_SA}" \
  --role="roles/bigquery.dataEditor"
```

---

### Paso 3 — Infraestructura en Pub/Sub

```bash
# 1. Crear el topic de Pub/Sub
gcloud pubsub topics create f1-telemetry-topic

# 2. Crear la suscripción directa hacia BigQuery (sin metadatos, para JSON puro)
gcloud pubsub subscriptions create f1-telemetry-bq-sub \
  --topic=f1-telemetry-topic \
  --bigquery-table=<PROJECT_ID>:f1_dataset.telemetry_table
```

---

### Paso 4 — Script Receptor en Python (`~/f1_publisher.py`)

Guardar el siguiente código en la VM (`f1-telemetry-server`). El script procesa tres tipos de paquete del juego:

| Packet ID | Contenido | Uso |
|---|---|---|
| 1 | Datos de sesión | Detecta el circuito actual |
| 2 | Datos de vuelta (*Lap Data*) | Detecta cambio de vuelta y último tiempo |
| 6 | Telemetría del monoplaza (*Car Telemetry*) | Velocidad, RPM, freno, temperaturas de neumáticos y frenos |

```python
import socket
import struct
import json
from google.cloud import pubsub_v1

PROJECT_ID = '<PROJECT_ID>'
TOPIC_ID = 'f1-telemetry-topic'

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

UDP_IP = '0.0.0.0'
UDP_PORT = 20777

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((UDP_IP, UDP_PORT))

TRACKS = {
    0: 'Melbourne (Australia)', 1: 'Paul Ricard', 2: 'Shanghai', 3: 'Sakhir (Bahrain)',
    4: 'Catalunya', 5: 'Monaco', 6: 'Montreal', 7: 'Silverstone', 8: 'Hockenheim',
    9: 'Hungaroring', 10: 'Spa', 11: 'Monza', 12: 'Singapore', 13: 'Suzuka',
    14: 'Abu Dhabi', 15: 'Texas', 16: 'Brazil', 17: 'Austria', 18: 'Sochi',
    19: 'Mexico', 20: 'Baku', 21: 'Sakhir Short', 22: 'Silverstone Short',
    23: 'Texas Short', 24: 'Suzuka Short', 25: 'Hanoi', 26: 'Zandvoort',
    27: 'Imola', 28: 'Portimao', 29: 'Jeddah', 30: 'Miami', 31: 'Las Vegas', 32: 'Qatar'
}

current_track = 'Detectando...'
last_published_track = ''
last_lap = 0

print("🟢 Servidor escuchando UDP en el puerto 20777...")

while True:
    try:
        data, addr = sock.recvfrom(2048)
        if len(data) >= 29:
            header = struct.unpack('<HBBBBBQfIIBB', data[:29])
            packet_id = header[5]
            player_car_index = header[10]
            car_idx = player_car_index if player_car_index < 22 else 0

            # Paquete 1: Datos de sesión (nombre del circuito)
            if packet_id == 1 and len(data) >= 37:
                track_id = struct.unpack('<b', data[36:37])[0]
                if track_id in TRACKS:
                    current_track = TRACKS[track_id]

            # Paquete 2: Datos de vuelta (vueltas y tiempos)
            elif packet_id == 2:
                lap_offset = 29 + (car_idx * 57)
                if len(data) >= lap_offset + 57:
                    lap_bytes = data[lap_offset:lap_offset+57]
                    last_lap_time = struct.unpack('<I', lap_bytes[0:4])[0] / 1000.0
                    current_lap = lap_bytes[31]

                    if current_track != last_published_track or current_lap != last_lap:
                        session_payload = {
                            'data': {
                                'type': 'session_event',
                                'track_name': current_track,
                                'lap_number': int(current_lap),
                                'last_lap_time': float(last_lap_time)
                            }
                        }
                        publisher.publish(topic_path, json.dumps(session_payload).encode('utf-8'))
                        last_published_track = current_track
                        last_lap = current_lap

            # Paquete 6: Telemetría del monoplaza (velocidad, RPM, frenos, neumáticos)
            elif packet_id == 6:
                car_offset = 29 + (car_idx * 60)
                if len(data) >= car_offset + 60:
                    car_data = data[car_offset:car_offset+60]
                    unpacked = struct.unpack('<HfffBbHBBH4H4B4BH4f4B', car_data)

                    telemetry_payload = {
                        'data': {
                            'type': 'car_telemetry',
                            'speed': int(unpacked[0]),
                            'brake': round(float(unpacked[3]), 2),
                            'engine_rpm': int(unpacked[6]),
                            'brakes_temp': [unpacked[10], unpacked[11], unpacked[12], unpacked[13]],
                            'tyre_temps': [unpacked[14], unpacked[15], unpacked[16], unpacked[17]]
                        }
                    }
                    publisher.publish(topic_path, json.dumps(telemetry_payload).encode('utf-8'))
    except Exception:
        continue
```

---

### Paso 5 — Vista Resiliente en BigQuery (`v_f1_telemetry_live`)

Esta vista maneja dinámicamente distintos niveles de anidación JSON (`data.*` o `data.data.*`) mediante `COALESCE`. Existe porque durante el desarrollo se acumularon registros históricos con estructuras distintas (*schema drift*) en la misma tabla.

```sql
CREATE OR REPLACE VIEW `<PROJECT_ID>.f1_dataset.v_f1_telemetry_live` AS
SELECT
  publish_time,
  -- Telemetría dinámica
  COALESCE(
    CAST(JSON_VALUE(data.speed) AS INT64),
    CAST(JSON_VALUE(data.data.speed) AS INT64)
  ) AS velocidad_kmh,
  COALESCE(
    CAST(JSON_VALUE(data.engine_rpm) AS INT64),
    CAST(JSON_VALUE(data.data.engine_rpm) AS INT64)
  ) AS rpm,
  COALESCE(
    CAST(JSON_VALUE(data.brake) AS FLOAT64),
    CAST(JSON_VALUE(data.data.brake) AS FLOAT64)
  ) AS freno,
  -- Temperatura de neumáticos (°C) [RL, RR, FL, FR]
  COALESCE(
    CAST(JSON_VALUE(data, '$.tyre_temps[0]') AS INT64),
    CAST(JSON_VALUE(data, '$.data.tyre_temps[0]') AS INT64)
  ) AS temp_llanta_RL,
  COALESCE(
    CAST(JSON_VALUE(data, '$.tyre_temps[1]') AS INT64),
    CAST(JSON_VALUE(data, '$.data.tyre_temps[1]') AS INT64)
  ) AS temp_llanta_RR,
  COALESCE(
    CAST(JSON_VALUE(data, '$.tyre_temps[2]') AS INT64),
    CAST(JSON_VALUE(data, '$.data.tyre_temps[2]') AS INT64)
  ) AS temp_llanta_FL,
  COALESCE(
    CAST(JSON_VALUE(data, '$.tyre_temps[3]') AS INT64),
    CAST(JSON_VALUE(data, '$.data.tyre_temps[3]') AS INT64)
  ) AS temp_llanta_FR,
  -- Temperatura de discos de freno (°C) [RL, RR, FL, FR]
  COALESCE(
    CAST(JSON_VALUE(data, '$.brakes_temp[0]') AS INT64),
    CAST(JSON_VALUE(data, '$.data.brakes_temp[0]') AS INT64)
  ) AS temp_freno_RL,
  COALESCE(
    CAST(JSON_VALUE(data, '$.brakes_temp[1]') AS INT64),
    CAST(JSON_VALUE(data, '$.data.brakes_temp[1]') AS INT64)
  ) AS temp_freno_RR,
  COALESCE(
    CAST(JSON_VALUE(data, '$.brakes_temp[2]') AS INT64),
    CAST(JSON_VALUE(data, '$.data.brakes_temp[2]') AS INT64)
  ) AS temp_freno_FL,
  COALESCE(
    CAST(JSON_VALUE(data, '$.brakes_temp[3]') AS INT64),
    CAST(JSON_VALUE(data, '$.data.brakes_temp[3]') AS INT64)
  ) AS temp_freno_FR
FROM `<PROJECT_ID>.f1_dataset.telemetry_table`
WHERE JSON_VALUE(data.type) = 'car_telemetry'
   OR JSON_VALUE(data.data.type) = 'car_telemetry'
   OR JSON_VALUE(data.speed) IS NOT NULL
   OR JSON_VALUE(data.data.speed) IS NOT NULL;
```

---

## 📊 4. Diccionario de Datos

| Campo | Tipo | Unidades / Rango | Descripción |
|---|---|---|---|
| `publish_time` | `TIMESTAMP` | UTC | Marca de tiempo exacta de llegada del paquete a Google Cloud |
| `velocidad_kmh` | `INT64` | 0 a 370 km/h | Velocidad instantánea del monoplaza |
| `rpm` | `INT64` | 0 a 15,000 RPM | Revoluciones por minuto del motor |
| `freno` | `FLOAT64` | 0.0 a 1.0 | Presión aplicada sobre el pedal de freno (0% a 100%) |
| `temp_llanta_*` | `INT64` | 20 °C a 130 °C | Temperatura de **superficie** del neumático en las 4 esquinas (RL, RR, FL, FR) |
| `temp_freno_*` | `INT64` | 20 °C a 1,200 °C | Temperatura de los discos de freno en las 4 esquinas (RL, RR, FL, FR) |

**Eventos de sesión** (publicados con `type = 'session_event'`; aún no expuestos en la vista, ver *Próximos pasos*):

| Campo | Tipo | Descripción |
|---|---|---|
| `track_name` | `STRING` | Circuito actual, detectado por el ID de la sesión |
| `lap_number` | `INT64` | Vuelta actual acumulada durante la sesión |
| `last_lap_time` | `FLOAT64` | Tiempo de la última vuelta completada, en segundos |

---

## 🔍 5. Comandos de Operación y Diagnóstico

**Diagnóstico del servicio en la VM:**

```bash
# Reiniciar el servicio
sudo systemctl restart f1-telemetry

# Ver logs en vivo
sudo journalctl -u f1-telemetry -f -n 20
```

**Prueba de ingesta manual en Pub/Sub (Cloud Shell):**

```bash
# Escuchar 5 mensajes directamente desde Pub/Sub
gcloud pubsub subscriptions create f1-debug-sub --topic=f1-telemetry-topic
gcloud pubsub subscriptions pull f1-debug-sub --auto-ack --limit=5
gcloud pubsub subscriptions delete f1-debug-sub
```

**Consulta de verificación en BigQuery:**

```sql
SELECT
  publish_time,
  velocidad_kmh,
  rpm,
  freno,
  temp_llanta_FL AS llanta_del_izq_c,
  temp_freno_FL AS freno_del_izq_c
FROM `<PROJECT_ID>.f1_dataset.v_f1_telemetry_live`
ORDER BY publish_time DESC
LIMIT 10;
```

**Operación diaria (control de costos):**

```bash
# Antes de la sesión de juego
gcloud compute instances start f1-telemetry-server --zone=us-central1-a

# Al terminar la sesión
gcloud compute instances stop f1-telemetry-server --zone=us-central1-a
```

El servicio `systemd` inicia el script automáticamente al encender la VM, sin necesidad de conectarse por SSH.

---

## 🔧 6. Bitácora de Resolución de Problemas

### Discriminación de tipo de paquete UDP
- **Causa raíz:** la primera versión procesaba el Packet ID 0 (*Motion*), ignorando el Packet ID 6 (*Car Telemetry*), que es el que contiene los datos de neumáticos y frenos.
- **Solución:** mapeo explícito del tipo de paquete en el encabezado antes de desempaquetar.

### Integridad del código en la VM
- **Causa raíz:** inserciones incompletas vía SSH truncaban la sintaxis del bucle principal.
- **Solución:** edición y verificación directa con `nano` en la VM.

### Schema drift en BigQuery
- **Causa raíz:** más de 60,000 registros históricos con estructuras JSON distintas (sin claves de neumáticos, o con anidación `data.data.*`) descartaban filas con cláusulas `WHERE` estrictas.
- **Solución:** vista resiliente con `COALESCE` que soporta ambas estructuras, más filtrado por ventana temporal para aislar sesiones activas.

---

## 📈 7. Próximos Pasos

- **Vista de eventos de sesión:** exponer `track_name`, `lap_number` y `last_lap_time` en una segunda vista, para analizar rendimiento por circuito y por vuelta.
- **Dashboard en Looker Studio:** conectar BigQuery para indicadores térmicos de las 4 esquinas con cambio de color automático (azul / verde / rojo) y gráficos de velocímetro en vivo.
- **Speed Trace:** vistas SQL comparativas entre la vuelta rápida personal y la vuelta actual, para análisis curva por curva.
- **Desglose de sectores:** incorporar tiempos de Sector 1, 2 y 3 desde *Lap Data*.
- **Daño y desgaste:** capturar el paquete *Car Damage* (ID 10) para desgaste de neumáticos y alerones.
- **Endurecimiento de seguridad:** restringir el rango de origen de la regla de firewall UDP a la IP pública del jugador, en vez de `0.0.0.0/0`, y registrar los errores del parser en vez de ignorarlos silenciosamente.

---

## 🎓 Conceptos Aplicados

- Ingesta en tiempo real desde un protocolo UDP binario hacia una arquitectura cloud-native
- Desempaquetado de estructuras binarias (`struct.unpack`) con offsets exactos, incluyendo arreglos de 4 posiciones
- Discriminación de tipos de paquete mediante el encabezado binario (Session, Lap Data, Car Telemetry)
- Streaming desacoplado con Pub/Sub y suscripción directa a BigQuery
- Manejo de *schema drift* en JSON semi-estructurado con vistas SQL resilientes (`COALESCE`)
- Persistencia de procesos con `systemd` y control de costos apagando la VM entre sesiones

---

## 📌 Notas

Este proyecto nació de una necesidad personal real: entender mi propio desempeño corriendo F1 25 (ritmo, gestión térmica de neumáticos y frenos) con datos cuantificables, en vez de solo sensaciones. Es un ejemplo end-to-end de arquitectura de streaming en la nube, aplicado a un caso de uso fuera del contexto típico de laboratorio.

---
---

[⬆ Español version above](#️-pipeline-de-telemetría-en-tiempo-real-ea-sports-f1--google-cloud-platform)

# 🏎️ F1 Real-Time Telemetry Pipeline on Google Cloud Platform

*Built for F1 25 — adaptable to any racing sim with UDP telemetry output.*

Technical documentation of the architecture, deployment, and operation of a real-time telemetry ingestion and analytics pipeline, from the Formula 1 simulator to Google Cloud Platform (GCP). It captures speed, RPM, brake, four-corner tire and brake temperatures, plus session events (track and lap).

> 🚧 **Status: Open beta.** This project was built for F1 25, but the UDP telemetry protocol is similar across other racing titles (iRacing, Assetto Corsa, Forza, Gran Turismo, other F1 entries). If you play another sim with UDP telemetry output, adapt the parser (`f1_publisher.py`) to your game and send a PR. The goal is a base any sim racer can use and improve.

---

## 🤝 How to Contribute / Adapt to Another Game

1. **Identify your game's UDP format** — most sims document their packet structure (search "UDP telemetry format" + your game's name).
2. **Adjust the `struct.unpack`** in `f1_publisher.py` to your game's byte layout.
3. **Adapt the BigQuery view** (`v_f1_telemetry_live`) if your game exposes different metrics.
4. **Document your adaptation** in a fork or PR, ideally naming the file after the game (e.g., `iracing_publisher.py`).

---

## 📐 1. System Architecture

```
+-----------------------------------------------------------------------------------+
|                                   DATA SOURCE                                     |
|   EA Sports F1 (PC Simulator)                                                     |
|   - UDP format: F1 2025 (up to 60 Hz)                                             |
|   - Port: UDP 20777                                                               |
+-----------------------------------------------------------------------------------+
                                          │
                                 (Local Network / Internet)
                                          │
                                          ▼
+-----------------------------------------------------------------------------------+
|                             CLOUD INGESTION (GCP)                                 |
|   Compute Engine (VM: f1-telemetry-server)                                        |
|   - systemd daemon: f1-telemetry.service                                          |
|   - Python publisher: ~/f1_publisher.py                                           |
|   - UDP sockets and binary unpacking (struct)                                     |
+-----------------------------------------------------------------------------------+
                                          │
                                (Asynchronous messaging)
                                          │
                                          ▼
+-----------------------------------------------------------------------------------+
|                             STREAMING & DATA BRIDGE                               |
|   Cloud Pub/Sub                                                                   |
|   - Topic: f1-telemetry-topic                                                     |
|   - BigQuery subscription: f1-telemetry-bq-sub                                    |
+-----------------------------------------------------------------------------------+
                                          │
                                  (Direct load to BigQuery)
                                          │
                                          ▼
+-----------------------------------------------------------------------------------+
|                          STORAGE & ANALYTICS LAYER                                |
|   BigQuery (<PROJECT_ID>)                                                         |
|   - Dataset: f1_dataset                                                           |
|   - Raw JSON table: telemetry_table                                               |
|   - Resilient view: v_f1_telemetry_live                                           |
+-----------------------------------------------------------------------------------+
```

---

## 🛠️ 2. Components & Infrastructure

| Component | Resource / Name | Description |
|---|---|---|
| UDP source | EA Sports F1 | Emits structured binary telemetry packets at up to 60 Hz on port 20777 |
| Receiver server | Compute Engine (`f1-telemetry-server`) | Linux VM that listens on UDP sockets and packages the data as JSON |
| System daemon | `f1-telemetry.service` | systemd service running the `f1_publisher.py` script in the background |
| Streaming ingestion | Cloud Pub/Sub (`f1-telemetry-topic`) | Asynchronous messaging topic decoupling ingestion from storage |
| BigQuery subscription | `f1-telemetry-bq-sub` | Native Pub/Sub subscription writing each JSON message directly to BigQuery |
| Storage | BigQuery (`telemetry_table`) | Raw (JSON) table recording every transmitted event |
| Abstraction layer | SQL view (`v_f1_telemetry_live`) | View with `COALESCE` logic that parses dynamic JSON structures and exposes native types (`INT64`, `FLOAT64`, `TIMESTAMP`) |

---

## 🚀 3. Deployment & Configuration Guide

### Step 1 — Configure the F1 Simulator

In the game's telemetry settings:

| Setting | Value |
|---|---|
| UDP Telemetry | On |
| UDP Format | 2025 (or matching the parser version) |
| UDP Target IP | Public IP of the VM (`f1-telemetry-server`) |
| UDP Port | 20777 |
| UDP Send Rate | 60Hz (or 20Hz depending on desired bandwidth) |

---

### Step 2 — GCP IAM Permissions

Assign the BigQuery editor role to Pub/Sub's managed service account:

```bash
PROJECT_ID="<PROJECT_ID>"
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
PUBSUB_SA="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${PUBSUB_SA}" \
  --role="roles/bigquery.dataEditor"
```

---

### Step 3 — Pub/Sub Infrastructure

```bash
# 1. Create the Pub/Sub topic
gcloud pubsub topics create f1-telemetry-topic

# 2. Create the direct BigQuery subscription (no metadata, for pure JSON)
gcloud pubsub subscriptions create f1-telemetry-bq-sub \
  --topic=f1-telemetry-topic \
  --bigquery-table=<PROJECT_ID>:f1_dataset.telemetry_table
```

---

### Step 4 — Python Receiver Script (`~/f1_publisher.py`)

Save the following code on the VM (`f1-telemetry-server`). The script handles three packet types from the game:

| Packet ID | Content | Use |
|---|---|---|
| 1 | Session data | Detects the current track |
| 2 | Lap Data | Detects lap changes and last lap time |
| 6 | Car Telemetry | Speed, RPM, brake, tire and brake temperatures |

```python
import socket
import struct
import json
from google.cloud import pubsub_v1

PROJECT_ID = '<PROJECT_ID>'
TOPIC_ID = 'f1-telemetry-topic'

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

UDP_IP = '0.0.0.0'
UDP_PORT = 20777

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((UDP_IP, UDP_PORT))

TRACKS = {
    0: 'Melbourne (Australia)', 1: 'Paul Ricard', 2: 'Shanghai', 3: 'Sakhir (Bahrain)',
    4: 'Catalunya', 5: 'Monaco', 6: 'Montreal', 7: 'Silverstone', 8: 'Hockenheim',
    9: 'Hungaroring', 10: 'Spa', 11: 'Monza', 12: 'Singapore', 13: 'Suzuka',
    14: 'Abu Dhabi', 15: 'Texas', 16: 'Brazil', 17: 'Austria', 18: 'Sochi',
    19: 'Mexico', 20: 'Baku', 21: 'Sakhir Short', 22: 'Silverstone Short',
    23: 'Texas Short', 24: 'Suzuka Short', 25: 'Hanoi', 26: 'Zandvoort',
    27: 'Imola', 28: 'Portimao', 29: 'Jeddah', 30: 'Miami', 31: 'Las Vegas', 32: 'Qatar'
}

current_track = 'Detectando...'
last_published_track = ''
last_lap = 0

print("🟢 Server listening on UDP port 20777...")

while True:
    try:
        data, addr = sock.recvfrom(2048)
        if len(data) >= 29:
            header = struct.unpack('<HBBBBBQfIIBB', data[:29])
            packet_id = header[5]
            player_car_index = header[10]
            car_idx = player_car_index if player_car_index < 22 else 0

            # Packet 1: Session data (track name)
            if packet_id == 1 and len(data) >= 37:
                track_id = struct.unpack('<b', data[36:37])[0]
                if track_id in TRACKS:
                    current_track = TRACKS[track_id]

            # Packet 2: Lap data (laps and times)
            elif packet_id == 2:
                lap_offset = 29 + (car_idx * 57)
                if len(data) >= lap_offset + 57:
                    lap_bytes = data[lap_offset:lap_offset+57]
                    last_lap_time = struct.unpack('<I', lap_bytes[0:4])[0] / 1000.0
                    current_lap = lap_bytes[31]

                    if current_track != last_published_track or current_lap != last_lap:
                        session_payload = {
                            'data': {
                                'type': 'session_event',
                                'track_name': current_track,
                                'lap_number': int(current_lap),
                                'last_lap_time': float(last_lap_time)
                            }
                        }
                        publisher.publish(topic_path, json.dumps(session_payload).encode('utf-8'))
                        last_published_track = current_track
                        last_lap = current_lap

            # Packet 6: Car telemetry (speed, RPM, brakes, tires)
            elif packet_id == 6:
                car_offset = 29 + (car_idx * 60)
                if len(data) >= car_offset + 60:
                    car_data = data[car_offset:car_offset+60]
                    unpacked = struct.unpack('<HfffBbHBBH4H4B4BH4f4B', car_data)

                    telemetry_payload = {
                        'data': {
                            'type': 'car_telemetry',
                            'speed': int(unpacked[0]),
                            'brake': round(float(unpacked[3]), 2),
                            'engine_rpm': int(unpacked[6]),
                            'brakes_temp': [unpacked[10], unpacked[11], unpacked[12], unpacked[13]],
                            'tyre_temps': [unpacked[14], unpacked[15], unpacked[16], unpacked[17]]
                        }
                    }
                    publisher.publish(topic_path, json.dumps(telemetry_payload).encode('utf-8'))
    except Exception:
        continue
```

---

### Step 5 — Resilient View in BigQuery (`v_f1_telemetry_live`)

This view dynamically handles different JSON nesting levels (`data.*` or `data.data.*`) using `COALESCE`. It exists because historical records with different structures (*schema drift*) accumulated in the same table during development.

```sql
CREATE OR REPLACE VIEW `<PROJECT_ID>.f1_dataset.v_f1_telemetry_live` AS
SELECT
  publish_time,
  -- Dynamic telemetry
  COALESCE(
    CAST(JSON_VALUE(data.speed) AS INT64),
    CAST(JSON_VALUE(data.data.speed) AS INT64)
  ) AS velocidad_kmh,
  COALESCE(
    CAST(JSON_VALUE(data.engine_rpm) AS INT64),
    CAST(JSON_VALUE(data.data.engine_rpm) AS INT64)
  ) AS rpm,
  COALESCE(
    CAST(JSON_VALUE(data.brake) AS FLOAT64),
    CAST(JSON_VALUE(data.data.brake) AS FLOAT64)
  ) AS freno,
  -- Tire temperature (°C) [RL, RR, FL, FR]
  COALESCE(
    CAST(JSON_VALUE(data, '$.tyre_temps[0]') AS INT64),
    CAST(JSON_VALUE(data, '$.data.tyre_temps[0]') AS INT64)
  ) AS temp_llanta_RL,
  COALESCE(
    CAST(JSON_VALUE(data, '$.tyre_temps[1]') AS INT64),
    CAST(JSON_VALUE(data, '$.data.tyre_temps[1]') AS INT64)
  ) AS temp_llanta_RR,
  COALESCE(
    CAST(JSON_VALUE(data, '$.tyre_temps[2]') AS INT64),
    CAST(JSON_VALUE(data, '$.data.tyre_temps[2]') AS INT64)
  ) AS temp_llanta_FL,
  COALESCE(
    CAST(JSON_VALUE(data, '$.tyre_temps[3]') AS INT64),
    CAST(JSON_VALUE(data, '$.data.tyre_temps[3]') AS INT64)
  ) AS temp_llanta_FR,
  -- Brake disc temperature (°C) [RL, RR, FL, FR]
  COALESCE(
    CAST(JSON_VALUE(data, '$.brakes_temp[0]') AS INT64),
    CAST(JSON_VALUE(data, '$.data.brakes_temp[0]') AS INT64)
  ) AS temp_freno_RL,
  COALESCE(
    CAST(JSON_VALUE(data, '$.brakes_temp[1]') AS INT64),
    CAST(JSON_VALUE(data, '$.data.brakes_temp[1]') AS INT64)
  ) AS temp_freno_RR,
  COALESCE(
    CAST(JSON_VALUE(data, '$.brakes_temp[2]') AS INT64),
    CAST(JSON_VALUE(data, '$.data.brakes_temp[2]') AS INT64)
  ) AS temp_freno_FL,
  COALESCE(
    CAST(JSON_VALUE(data, '$.brakes_temp[3]') AS INT64),
    CAST(JSON_VALUE(data, '$.data.brakes_temp[3]') AS INT64)
  ) AS temp_freno_FR
FROM `<PROJECT_ID>.f1_dataset.telemetry_table`
WHERE JSON_VALUE(data.type) = 'car_telemetry'
   OR JSON_VALUE(data.data.type) = 'car_telemetry'
   OR JSON_VALUE(data.speed) IS NOT NULL
   OR JSON_VALUE(data.data.speed) IS NOT NULL;
```

---

## 📊 4. Data Dictionary

| Field | Type | Units / Range | Description |
|---|---|---|---|
| `publish_time` | `TIMESTAMP` | UTC | Exact timestamp of the packet's arrival in Google Cloud |
| `velocidad_kmh` | `INT64` | 0 to 370 km/h | Instantaneous car speed |
| `rpm` | `INT64` | 0 to 15,000 RPM | Engine revolutions per minute |
| `freno` | `FLOAT64` | 0.0 to 1.0 | Pressure applied to the brake pedal (0% to 100%) |
| `temp_llanta_*` | `INT64` | 20 °C to 130 °C | **Surface** tire temperature at the 4 corners (RL, RR, FL, FR) |
| `temp_freno_*` | `INT64` | 20 °C to 1,200 °C | Brake disc temperature at the 4 corners (RL, RR, FL, FR) |

**Session events** (published with `type = 'session_event'`; not yet exposed in the view, see *Next Steps*):

| Field | Type | Description |
|---|---|---|
| `track_name` | `STRING` | Current track, detected from the session ID |
| `lap_number` | `INT64` | Current lap accumulated during the session |
| `last_lap_time` | `FLOAT64` | Time of the last completed lap, in seconds |

---

## 🔍 5. Operation & Diagnostics Commands

**Service diagnostics on the VM:**

```bash
# Restart the service
sudo systemctl restart f1-telemetry

# Live logs
sudo journalctl -u f1-telemetry -f -n 20
```

**Manual ingestion test on Pub/Sub (Cloud Shell):**

```bash
# Pull 5 messages directly from Pub/Sub
gcloud pubsub subscriptions create f1-debug-sub --topic=f1-telemetry-topic
gcloud pubsub subscriptions pull f1-debug-sub --auto-ack --limit=5
gcloud pubsub subscriptions delete f1-debug-sub
```

**Verification query in BigQuery:**

```sql
SELECT
  publish_time,
  velocidad_kmh,
  rpm,
  freno,
  temp_llanta_FL AS llanta_del_izq_c,
  temp_freno_FL AS freno_del_izq_c
FROM `<PROJECT_ID>.f1_dataset.v_f1_telemetry_live`
ORDER BY publish_time DESC
LIMIT 10;
```

**Daily operation (cost control):**

```bash
# Before the gaming session
gcloud compute instances start f1-telemetry-server --zone=us-central1-a

# After the session
gcloud compute instances stop f1-telemetry-server --zone=us-central1-a
```

The `systemd` service starts the script automatically when the VM boots, with no SSH connection needed.

---

## 🔧 6. Troubleshooting Log

### UDP packet type discrimination
- **Root cause:** the first version processed Packet ID 0 (*Motion*), ignoring Packet ID 6 (*Car Telemetry*), which holds the tire and brake data.
- **Resolution:** explicit packet-type mapping from the header before unpacking.

### Code integrity on the VM
- **Root cause:** incomplete pastes via SSH truncated the main loop's syntax.
- **Resolution:** direct editing and verification with `nano` on the VM.

### Schema drift in BigQuery
- **Root cause:** over 60,000 historical records with different JSON structures (missing tire keys, or `data.data.*` nesting) caused rows to be dropped by strict `WHERE` clauses.
- **Resolution:** a resilient view with `COALESCE` supporting both structures, plus time-window filtering to isolate active sessions.

---

## 📈 7. Next Steps

- **Session events view:** expose `track_name`, `lap_number`, and `last_lap_time` in a second view, to analyze performance per track and per lap.
- **Looker Studio dashboard:** connect BigQuery for four-corner thermal indicators with automatic color changes (blue / green / red) and live speedometer charts.
- **Speed trace:** comparative SQL views between the personal best lap and the current lap, for corner-by-corner analysis.
- **Sector breakdown:** add Sector 1, 2, and 3 times from *Lap Data*.
- **Damage & wear:** capture the *Car Damage* packet (ID 10) for tire and wing wear.
- **Security hardening:** restrict the UDP firewall rule's source range to the player's public IP instead of `0.0.0.0/0`, and log parser errors instead of silently ignoring them.

---

## 🎓 Concepts Applied

- Real-time ingestion from a binary UDP protocol into a cloud-native architecture
- Binary structure unpacking (`struct.unpack`) with exact offsets, including 4-position arrays
- Packet type discrimination via the binary header (Session, Lap Data, Car Telemetry)
- Decoupled streaming with Pub/Sub and a direct BigQuery subscription
- Handling schema drift in semi-structured JSON with resilient SQL views (`COALESCE`)
- Process persistence with `systemd`, and cost control by stopping the VM between sessions

---

## 📌 Notes

This project came from a real personal need: understanding my own performance racing F1 25 (pace, tire and brake thermal management) with quantifiable data, rather than just gut feeling. It's an end-to-end example of a cloud streaming architecture, applied to a use case outside the typical lab context.
