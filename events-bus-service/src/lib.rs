use fluvio::consumer::ConsumerConfigExtBuilder;
use fluvio::{Fluvio, Offset, RecordKey};

use futures::StreamExt;
use logging_service::log;
#[allow(unused_imports)]
use std::time::Duration;
#[allow(unused_imports)]
use tokio::time::timeout;

const SERVICE_NAME: &str = "events-bus-service";

// Produce a single key-value record to the given topic.
pub async fn produce_message(
    topic: &str,
    key: impl Into<RecordKey>,
    value: &str,
) -> anyhow::Result<()> {
    log!(INFO, SERVICE_NAME, "produce_message", "Starting message production", "topic" => topic);

    let producer = fluvio::producer(topic).await?;
    producer.send(key, value).await?;
    producer.flush().await?;

    log!(INFO, SERVICE_NAME, "produce_message", "Message produced successfully", "topic" => topic);

    Ok(())
}

pub async fn consume_until_value(topic: &str, target_value: &str) -> anyhow::Result<()> {
    log!(INFO, SERVICE_NAME, "consume_until_value", "Starting to consume messages", "topic" => topic);

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

        log!(INFO, SERVICE_NAME, "consume_until_value", "Consumed message", "topic" => topic, "key" => key, "value" => value.clone());

        if value == target_value {
            log!(INFO, SERVICE_NAME, "consume_until_value", "Target value found, stopping consumption", "topic" => topic);
            return Ok(());
        }
    }

    Ok(())
}

pub async fn ensure_topic_exists(
    topic: &str,
    partitions: u32,
    replication: u32,
) -> anyhow::Result<()> {
    log!(INFO, SERVICE_NAME, "ensure_topic_exists", "Ensuring topic exists", "topic" => topic);
    let fluvio = Fluvio::connect().await?;
    let admin = fluvio.admin().await;

    let topic_spec =
        fluvio::metadata::topic::TopicSpec::new_computed(partitions, replication, None);
    match admin.create(topic.to_string(), false, topic_spec).await {
        Ok(_) => {
           log!(INFO, SERVICE_NAME, "ensure_topic_exists", "Topic created successfully", "topic" => topic);
        }
        Err(e) => {
           log!(WARN, SERVICE_NAME, "ensure_topic_exists", "Topic creation failed, it may already exist", "topic" => topic, "error" => e.to_string());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_TOPIC: &str = "test-topic";
    const TIMEOUT_MS: u64 = 5000;

    #[tokio::test]
    async fn test_produce_consume() -> anyhow::Result<()> {
       log!(INFO, SERVICE_NAME, "test_produce_consume", "Starting produce-consume test", "topic" => TEST_TOPIC);
        if let Err(e) = ensure_topic_exists(TEST_TOPIC, 1, 1).await {
           log!(ERROR, SERVICE_NAME, "test_produce_consume", "Failed to ensure test topic exists", "topic" => TEST_TOPIC, "error" => e.to_string());
            return Err(e);
        }

        let key = "test-key";
        let value = "test-value";

        let produce_handle = tokio::spawn(produce_message(TEST_TOPIC, key, value));
        let consume_handle = tokio::spawn(consume_until_value(TEST_TOPIC, value));

        let result = timeout(Duration::from_millis(TIMEOUT_MS), async {
            let produce_result = produce_handle.await;
            let consume_result = consume_handle.await;
            (produce_result, consume_result)
        })
        .await;

        match result {
            Ok((produce_result, consume_result)) => {
                let produce_outcome = match produce_result {
                    Ok(res) => res,
                    Err(e) => {
                     log!(ERROR, SERVICE_NAME, "test_produce_consume", "Produce task panicked", "topic" => TEST_TOPIC, "error" => e.to_string());
                        return Err(anyhow::anyhow!("Produce task panicked: {e}"));
                    }
                };
                let consume_outcome = match consume_result {
                    Ok(res) => res,
                    Err(e) => {
                        log!(ERROR, SERVICE_NAME, "test_produce_consume", "Consume task panicked", "topic" => TEST_TOPIC, "error" => e.to_string());
                        return Err(anyhow::anyhow!("Consume task panicked: {e}"));
                    }
                };

                if let Err(e) = produce_outcome {
                   log!(ERROR, SERVICE_NAME, "test_produce_consume", "Produce failed", "topic" => TEST_TOPIC, "error" => e.to_string());
                    return Err(e);
                }

                if let Err(e) = consume_outcome {
                   log!(ERROR, SERVICE_NAME, "test_produce_consume", "Consume failed", "topic" => TEST_TOPIC, "error" => e.to_string());
                    return Err(e);
                }

                log!(INFO, SERVICE_NAME, "test_produce_consume", "Produce-consume test completed successfully", "topic" => TEST_TOPIC);
                Ok(())
            }
            Err(_) => {
              log!(ERROR, SERVICE_NAME, "test_produce_consume", "Test timed out", "topic" => TEST_TOPIC, "timeout_ms" => TIMEOUT_MS.to_string());
                Err(anyhow::anyhow!("Test timed out after {TIMEOUT_MS}ms"))
            }
        }
    }
}
