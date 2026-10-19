# kafka-tester

Vista web en Rust (axum + rskafka, 100% Rust, sin librdkafka) para probar la comunicación con Kafka.

## Uso

```bash
docker compose up -d                              # Kafka local en localhost:9092 (opcional)
KAFKA_BROKERS=localhost:9092 cargo run            # abre http://127.0.0.1:3000
```

- `KAFKA_BROKERS`: brokers separados por coma (también se puede conectar desde la web).
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
