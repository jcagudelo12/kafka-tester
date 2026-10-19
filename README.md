# kafka-tester

Vista web en Rust (axum + rskafka, 100% Rust, sin librdkafka) para probar la comunicación con Kafka.

## Estructura

```
src/
├── main.rs          # vista web (cargo run)
├── lib.rs           # declara los módulos compartidos
├── config.rs        # broker/topic por defecto y conexión
├── producer.rs      # enviar mensajes
├── consumer.rs      # escuchar mensajes
├── topics.rs        # listar y crear topics
├── web/
│   ├── mod.rs       # estado y rutas HTTP
│   ├── handlers.rs  # endpoints /api/*
│   └── error.rs     # errores JSON para el navegador
└── bin/
    ├── producer.rs  # cargo run --bin producer  (como producer.py)
    └── consumer.rs  # cargo run --bin consumer  (como consumer.py)
static/index.html    # la página web
```

### Ejecutables de terminal

```bash
cargo run --bin consumer                          # escucha "pruebas", solo mensajes nuevos
cargo run --bin consumer -- pruebas earliest      # escucha desde el inicio
cargo run --bin producer -- "hola"                # envía "hola" a "pruebas"
cargo run --bin producer -- "hola" otro-topic     # envía a otro topic
```

Ambos usan `KAFKA_BROKERS` igual que la vista web.

## Requisitos

- Rust (`cargo`) instalado.
- Acceso de red al broker `192.168.78.165:9092`. Para comprobarlo:

  ```bash
  timeout 5 bash -c '</dev/tcp/192.168.78.165/9092' && echo "puerto abierto" || echo "sin conexión"
  ```

## Paso a paso para probar la app

### 1. Arrancar el servidor

```bash
cd /home/milo/Documentos/Projects/kafka-tester
cargo run
```

Deberías ver en la terminal:

```
Conectado a Kafka en 192.168.78.165:9092
Vista web en http://127.0.0.1:3000
```

Si aparece `No se pudo conectar a ...`, revisa la red o el broker (ver *Problemas comunes*).

### 2. Abrir la vista web

Abre http://127.0.0.1:3000 en el navegador. Arriba a la izquierda debe verse el punto verde
y el texto **Conectado a 192.168.78.165:9092**. Si está en rojo, escribe el broker en el campo
de arriba y pulsa **Conectar**.

### 3. Elegir (o crear) un topic

- En **Topics**, elige el topic en la lista (por ejemplo `pruebas`). Si no aparece, pulsa **↻**.
- Para crear uno nuevo, escribe el nombre en **Nuevo topic**, indica las particiones y pulsa **Crear**.

### 4. Empezar a escuchar

En el panel **Consumidor**:

1. Elige **Solo nuevos** (solo lo que llegue desde ahora) o **Desde el inicio** (todo el historial del topic).
2. Pulsa **Escuchar**. Aparecerá la línea `Escuchando <topic> (partición 0, ...)`.

### 5. Enviar un mensaje

En **Producir mensaje**:

1. **Partición**: deja `0` (salvo que el topic tenga varias).
2. **Key** (opcional): por ejemplo `cliente-1`.
3. **Value**: el contenido, por ejemplo `{"hola": "kafka"}`.
4. **Headers** (opcional, en JSON): por ejemplo `{"origen": "web"}`.
5. Pulsa **Enviar**.

### 6. Verificar la comunicación

- En el log aparece `Enviado a <topic> → offset N`: Kafka aceptó el mensaje.
- Justo después, el consumidor muestra el mensaje con su `offset`, `key`, hora y headers.
  Si el value es JSON, se ve formateado.

Si ves las dos cosas, la comunicación funciona en los dos sentidos (producir y consumir).

Pulsa **Detener** para dejar de escuchar y **Limpiar** para vaciar el log.

### 7. (Opcional) Probar sin la web, con `curl`

Con el servidor corriendo, en una terminal deja escuchando el topic:

```bash
curl -N "http://127.0.0.1:3000/api/consume?topic=pruebas&from=latest"
```

Y en otra terminal envía un mensaje:

```bash
curl -X POST http://127.0.0.1:3000/api/produce -H 'content-type: application/json' -d '{"topic":"pruebas","key":"k1","value":"hola desde curl"}'
```

La primera terminal debe mostrar un `event: message` con el mensaje enviado.

## Probar con un Kafka local (sin el servidor remoto)

```bash
docker compose up -d
KAFKA_BROKERS=localhost:9092 cargo run
```

Para apagarlo: `docker compose down`.

## Problemas comunes

- **`No se pudo conectar` / tiempo de espera agotado**: el broker no responde en esa IP y puerto. Revisa la VPN o la red y el firewall.
- **Conecta, pero falla al enviar o escuchar**: el broker puede estar anunciando una dirección
  (`advertised.listeners`) a la que no llegas desde esta máquina, como `localhost`. Hay que corregirlo en la configuración del broker.
- **`No conectado a Kafka`**: pulsa **Conectar** en la web.
- **El puerto 3000 está ocupado**: arranca con `BIND=127.0.0.1:3001 cargo run`.

## Configuración

- `KAFKA_BROKERS`: brokers separados por coma (por defecto `192.168.78.165:9092`; también se puede cambiar desde la web). Para el Kafka local de Docker usa `KAFKA_BROKERS=localhost:9092`.
- `BIND`: dirección del servidor web (por defecto `127.0.0.1:3000`).

## API

| Método | Ruta | Descripción |
|---|---|---|
| GET  | `/api/status` | Estado de la conexión |
| POST | `/api/connect` | `{ "brokers": "host:9092" }` |
| GET  | `/api/topics` | Lista de topics |
| POST | `/api/topics` | `{ "name": "t", "partitions": 1 }` |
| POST | `/api/produce` | `{ "topic", "partition", "key", "value", "headers" }` |
| GET  | `/api/consume?topic=t&partition=0&from=earliest\|latest` | Stream SSE de mensajes |

Nota: sin TLS/SASL de momento; para un servidor remoto con autenticación hay que añadir
`tls_config` / `sasl_config` en `ClientBuilder` (src/main.rs).
