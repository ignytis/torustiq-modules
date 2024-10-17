use std::collections::HashMap;

use log::{error, info};
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};


fn queue_poll_error_cb(message: String) {
    if message.contains("Connection refused") || message.contains("Authentication failed") {
        error!("FATAL ERROR: {}", message);
        std::process::exit(-1);
    }
}

#[derive(Clone)]
pub struct KafkaProducer {
    rd_producer: FutureProducer,
}

pub struct KafkaMessage {
    pub topic: String,
    pub key: Option<String>,
    pub headers: HashMap<String, String>,
    pub payload: Vec<u8>,
}

impl KafkaProducer {
    pub fn new(cfg: &HashMap<String, String>) -> Self {
        let bootstrap_servers = cfg.get("bootstrap_servers")
            .unwrap_or(&String::from("(not provided)"))
            .clone();  // to resolve a '&String vs String' issue
        info!("Bootstrap servers: {}", bootstrap_servers);

        let mut kafka_config = &mut ClientConfig::new();
        for (k, v) in cfg.iter() {
            kafka_config = kafka_config.set(k.replace("_", "."), v);
        }
        kafka_config = kafka_config.set_queue_poll_error_cb(queue_poll_error_cb);

        KafkaProducer {
            rd_producer: kafka_config.create().expect("Can't create a Kafka producer")
        }
    }

    // On success returns a tuple (partition, offset)
    // On failure returns an error message
    pub async fn produce(&self, msg: &KafkaMessage) -> Result<(i32, i64), String> {
        let p = &msg.payload.to_vec();

        let mut headers = OwnedHeaders::new_with_capacity(msg.headers.len());
        for (key, value) in &msg.headers {
            headers = headers.insert(Header{key, value: Some(value)});
        }

        // Init a message with payload
        let mut future_record: FutureRecord<'_, String, Vec<u8>> = FutureRecord::to(&msg.topic)
            .payload(p)
            .headers(headers);
        future_record = match &msg.key {
            Some(k) => future_record.key(k),
            None => future_record,
        };


        let res = match self.rd_producer.send_result(future_record) {
            Ok(o) => o,
            Err((kafka_error, _)) => return Err(format!("{:?}", kafka_error)),
        };
        match res.await {
            Ok(res2) => match res2 {
                Ok(d) => Ok(d),
                Err((kafka_error, _)) => Err(format!("{:?}", kafka_error))
            },
            Err(_) => Err(format!("An error occurred")),
        }
    }
}