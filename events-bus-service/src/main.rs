use events_bus_service::{ensure_topic_exists, produce_message};
use fluvio::{Compression, Fluvio, TopicProducerConfigBuilder};
use logging_service::log;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

const ORDERS_TOPIC: &str = "orders";
const SERVICE_NAME: &str = "order-service";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = "0.0.0.0:9184".parse()?;
    prometheus_exporter::start(addr).expect("Failed to start prometheus exporter");
    log!(INFO, SERVICE_NAME, "main", "Prometheus exporter started on port 9184");

    if let Err(e) = ensure_topic_exists(SERVICE_NAME, ORDERS_TOPIC, 1, 1).await {
        log!(ERROR, SERVICE_NAME, "main", "Failed to ensure topic exists", "error" => e.to_string());
        return Err(e);
    }
    log!(INFO, SERVICE_NAME, "main", "Topic 'orders' is ready");

    let fluvio = Fluvio::connect().await.expect("Failed to connect to Fluvio");

    let producer_config = TopicProducerConfigBuilder::default().batch_size(500).linger(Duration::from_millis(500)).compression(Compression::Gzip).build()?;

    log!(INFO, SERVICE_NAME, "main", "Producer config created", "batch_size" => 500, "linger_ms" => 500, "compression" => "gzip");

    let producer = fluvio.topic_producer_with_config(ORDERS_TOPIC, producer_config).await.expect("Failed to create producer");

    let mut order_id = 1;
    log!(INFO, SERVICE_NAME, "main", "Starting producer loop...");
    loop {
        let key = format!("order-{}", order_id);
        let value = format!(
              r#"{{"order_id": {}, "customer_id": "cust-abc", "item": "super-widget"}}"#,
              order_id
        );

        if let Err(e) = produce_message(SERVICE_NAME, &producer, key.clone(), &value).await
        {
            log!(ERROR, SERVICE_NAME, "main", "Failed to produce message", "key" => key, "error" => e.to_string());
        }

        order_id += 1;
        sleep(Duration::from_secs(2)).await;
    }
}