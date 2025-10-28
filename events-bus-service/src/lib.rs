use fluvio::consumer::ConsumerConfigExtBuilder;
use fluvio::spu::SpuPool;
use fluvio::{Fluvio, Offset, RecordKey, TopicProducer};

use futures::StreamExt;
use logging_service::log;
#[allow(unused_imports)]
use std::time::Duration;
#[allow(unused_imports)]
use tokio::time::timeout;

pub async fn produce_message<S>(
    service_name: &str,
    producer: &TopicProducer<S>,
    key: impl Into<RecordKey>,
    value: &str,
) -> anyhow::Result<()>
where
    S: SpuPool + Send + Sync + 'static,
{
    let topic = producer.topic();
    log!(INFO, service_name, "produce_message", "Producing message", "topic" => topic);

    producer.send(key, value).await?;

    log!(INFO, service_name, "produce_message", "Message queued successfully", "topic" => topic);

    Ok(())
}

pub async fn consume_until_value(
    service_name: &str,
    topic: &str,
    target_value: &str,
) -> anyhow::Result<()> {
    log!(INFO, service_name, "consume_until_value", "Starting to consume messages", "topic" => topic);

    let fluvio = Fluvio::connect().await?;

    let mut stream = fluvio
        .consumer_with_config(
            ConsumerConfigExtBuilder::default()
                .topic(topic)
                .partition(0)
                .offset_start(Offset::beginning())
                .build()?,
        )
        .await?;

    while let Some(Ok(record)) = stream.next().await {
        let key = record.get_key().map(|k| k.as_utf8_lossy_string());
        let value = record.get_value().as_utf8_lossy_string();

        log!(INFO, service_name, "consume_until_value", "Consumed message", "topic" => topic, "key" => key, "value" => value.clone());

        if value == target_value {
            log!(INFO, service_name, "consume_until_value", "Target value found, stopping consumption", "topic" => topic);
            return Ok(());
        }
    }

    Ok(())
}

pub async fn ensure_topic_exists(
    service_name: &str,
    topic: &str,
    partitions: u32,
    replication: u32,
) -> anyhow::Result<()> {
    log!(INFO, service_name, "ensure_topic_exists", "Ensuring topic exists", "topic" => topic);
    let fluvio = Fluvio::connect().await?;
    let admin = fluvio.admin().await;

    let topic_spec =
        fluvio::metadata::topic::TopicSpec::new_computed(partitions, replication, None);
    match admin.create(topic.to_string(), false, topic_spec).await {
        Ok(_) => {
            log!(INFO, service_name, "ensure_topic_exists", "Topic created successfully", "topic" => topic);
        }
        Err(e) => {
            log!(WARN, service_name, "ensure_topic_exists", "Topic may already exist or failed to create", "topic" => topic, "error" => e.to_string());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_TOPIC: &str = "test-topic";
    const TIMEOUT_MS: u64 = 5000;
    const TEST_SERVICE: &str = "event-bus-tests";

    #[tokio::test]
    async fn test_produce_consume() -> anyhow::Result<()> {
        log!(INFO, TEST_SERVICE, "test_produce_consume", "Starting produce-consume test", "topic" => TEST_TOPIC);
        if let Err(e) = ensure_topic_exists(TEST_SERVICE, TEST_TOPIC, 1, 1).await {
            log!(ERROR, TEST_SERVICE, "test_produce_consume", "Failed to ensure test topic exists", "topic" => TEST_TOPIC, "error" => e.to_string());
            return Err(e);
        }

        let key = "test-key";
        let value = "test-value";

        let producer_task = async move {
            let fluvio = Fluvio::connect().await.expect("Failed to connect");
            let producer = fluvio
                .topic_producer(TEST_TOPIC)
                .await
                .expect("Failed to create producer");

            produce_message(TEST_SERVICE, &producer, key, value)
                .await
                .expect("Produce failed");

            producer.flush().await.expect("Flush failed");
        };

        let produce_handle = tokio::spawn(producer_task);
        let consume_handle = tokio::spawn(consume_until_value(TEST_SERVICE, TEST_TOPIC, value));

        let result = timeout(Duration::from_millis(TIMEOUT_MS), async {
            let produce_result = produce_handle.await;
            let consume_result = consume_handle.await;
            (produce_result, consume_result)
        })
        .await;

        match result {
            Ok((produce_result, consume_result)) => {
                if let Err(e) = produce_result {
                    log!(ERROR, TEST_SERVICE, "test_produce_consume", "Produce task panicked", "topic" => TEST_TOPIC, "error" => e.to_string());
                    return Err(anyhow::anyhow!("Produce task panicked: {e}"));
                }
                if let Err(e) = consume_result {
                    log!(ERROR, TEST_SERVICE, "test_produce_consume", "Consume task panicked", "topic" => TEST_TOPIC, "error" => e.to_string());
                    return Err(anyhow::anyhow!("Consume task panicked: {e}"));
                }
                log!(INFO, TEST_SERVICE, "test_produce_consume", "Produce-consume test completed successfully", "topic" => TEST_TOPIC);

                Ok(())
            }
            Err(_) => {
                log!(ERROR, TEST_SERVICE, "test_produce_consume", "Test timed out", "topic" => TEST_TOPIC, "timeout_ms" => TIMEOUT_MS.to_string());
                Err(anyhow::anyhow!("Test timed out after {TIMEOUT_MS}ms"))
            }
        }
    }
}
