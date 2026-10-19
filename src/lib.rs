//! Lógica de Kafka compartida por la vista web (`main.rs`) y los ejecutables
//! de línea de comandos (`src/bin/producer.rs`, `src/bin/consumer.rs`).

pub mod config;
pub mod consumer;
pub mod producer;
pub mod topics;
pub mod web;
