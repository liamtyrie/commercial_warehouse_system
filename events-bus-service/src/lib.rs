use fluvio::consumer::ConsumerConfigExtBuilder;
use fluvio::{Fluvio, Offset, RecordKey};

use futures::StreamExt;
#[allow(unused_imports)]
use std::time::Duration;
#[allow(unused_imports)]
use tokio::time::timeout;

use logging_service::log_event::{LogEvent, LogLevel};
use std::collections::HashMap;

// Produce a single key-value record to the given topic.
pub async fn produce_message(
    topic: &str,
    key: impl Into<RecordKey>,
    value: &str,
) -> anyhow::Result<()> {
    log_info("produce_message", "Starting message production", topic);
    let producer = fluvio::producer(topic).await?;
    producer.send(key, value).await?;
    producer.flush().await?;

    log_info("produce_message", "Message produced successfully", topic);
    Ok(())
}

pub async fn consume_until_value(topic: &str, target_value: &str) -> anyhow::Result<()> {
    log_info("consume_until_value", "Starting consumption", topic);

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
        log_debug("consume_until_value", &format!("Got record: key={:?}, value={}", key, value), topic);

        if value == target_value {
            log_info("consume_until_value", "Found target value", topic);
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
    log_info("ensure_topic_exists", &format!("Ensuring topic exists: partitions={partitions}, replication={replication}"), topic);
    let fluvio = Fluvio::connect().await?;
    let admin = fluvio.admin().await;

    let topic_spec =
        fluvio::metadata::topic::TopicSpec::new_computed(partitions, replication, None);
    match admin.create(topic.to_string(), false, topic_spec).await {
        Ok(_) => log_info("ensure_topic_exists", "Topic created successfully", topic),
        Err(e) => {
            log_warn("ensure_topic_exists", &format!("Failed to create topic: {e}"), topic);
        }
    }

    Ok(())
}

fn log_info(target: &str, message: &str, topic: &str) {
    emit_log(LogLevel::INFO, target, message, topic);
}

fn log_debug(target: &str, message: &str, topic: &str) {
    emit_log(LogLevel::DEBUG, target, message, topic);
}

fn log_warn(target: &str, message: &str, topic: &str) {
    emit_log(LogLevel::WARN, target, message, topic);
}

#[allow(dead_code)]
fn log_error(target: &str, message: &str, topic: &str) {
    emit_log(LogLevel::ERROR, target, message, topic);
}

fn emit_log(level: LogLevel, target: &str, message: &str, topic: &str) {
    let mut extra_fields = HashMap::new();
    extra_fields.insert("topic".to_string(), serde_json::json!(topic));
    
    let log_event = LogEvent {
        level,
        msg: message.to_string(),
        target: target.to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        service_name: "events-bus-service".to_string(),
        extra_fields,
    };
    
    let log_json = serde_json::to_string(&log_event).unwrap_or_else(|_| "Failed to serialize log".to_string());
    
    // Use stderr for ERROR level, stdout for everything else
    if matches!(log_event.level, LogLevel::ERROR) {
        eprintln!("{}", log_json);
    } else {
        println!("{}", log_json);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_TOPIC: &str = "test-topic";
    const TIMEOUT_MS: u64 = 5000;

    #[tokio::test]
    async fn test_produce_consume() -> Result<(), Box<dyn std::error::Error>> {
        if let Err(e) = run_test().await {
            eprintln!("Test failed: {e}");
            return Err(e.into());
        }
        Ok(())
    }
    async fn run_test() -> anyhow::Result<()> {
        if let Err(e) = ensure_topic_exists(TEST_TOPIC, 1, 1).await {
            eprintln!("Failed to ensure topic exists: {e}");
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
                        eprintln!("Produce task panicked: {e}");
                        return Err(anyhow::anyhow!("Product task panicked: {e}"));
                    }
                };

                let consume_outcome = match consume_result {
                    Ok(res) => res,
                    Err(e) => {
                        eprintln!("Consume task panicked: {e}");
                        return Err(anyhow::anyhow!("Consume task panicked: {e}"));
                    }
                };

                if let Err(e) = produce_outcome {
                    eprintln!("Produce failed: {e}");
                    return Err(e);
                }

                if let Err(e) = consume_outcome {
                    eprintln!("Consume failed: {e}");
                    return Err(e);
                }

                Ok(())
            }
            Err(_) => {
                eprintln!("Test timed out after {TIMEOUT_MS}ms");
                Err(anyhow::anyhow!("Test timed out after {TIMEOUT_MS}ms"))
            }
        }
    }
}
