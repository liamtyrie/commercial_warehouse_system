use fluvio::consumer::ConsumerConfigExtBuilder;
use fluvio::{Fluvio, Offset, RecordKey};

use futures::StreamExt;
#[allow(unused_imports)]
use std::time::Duration;
#[allow(unused_imports)]
use tokio::time::timeout;

const SERVICE_NAME: &str = "events-bus-service";

// Small helper macro to build the `extra` metadata map used for logging.
// Usage: let extra = meta! { "topic" => topic, "key" => key };
macro_rules! meta {
    ( $( $k:expr => $v:expr ),* $(,)? ) => {{
        let mut m: std::collections::HashMap<String, serde_json::Value> = std::collections::HashMap::new();
        $(
            m.insert($k.to_string(), serde_json::json!($v));
        )*
        m
    }};
}

// Produce a single key-value record to the given topic.
pub async fn produce_message(
    topic: &str,
    key: impl Into<RecordKey>,
    value: &str,
) -> anyhow::Result<()> {
    let extra = meta! { "topic" => topic };

    logging_service::log_info(
        SERVICE_NAME,
        "produce_message",
        "Starting message production",
        Some(extra),
    );

    let producer = fluvio::producer(topic).await?;
    producer.send(key, value).await?;
    producer.flush().await?;

    let extra = meta! { "topic" => topic };
    logging_service::log_info(
        SERVICE_NAME,
        "produce_message",
        "Message produced successfully",
        Some(extra),
    );

    Ok(())
}

pub async fn consume_until_value(topic: &str, target_value: &str) -> anyhow::Result<()> {
    let extra = meta! { "topic" => topic };
    logging_service::log_info(
        SERVICE_NAME,
        "consume_until_value",
        "Starting message consumption",
        Some(extra),
    );

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

        let extra = meta! { "topic" => topic, "key" => key, "value" => value.clone() };
        logging_service::log_info(
            SERVICE_NAME,
            "consume_until_value",
            "Got record",
            Some(extra),
        );

        if value == target_value {
            let extra = meta! { "topic" => topic };
            logging_service::log_info(
                SERVICE_NAME,
                "consume_until_value",
                "Target value consumed successfully",
                Some(extra),
            );
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
    let extra = meta! { "topic" => topic, "partitions" => partitions, "replication" => replication };
    logging_service::log_info(
        SERVICE_NAME,
        "ensure_topic_exists",
        "Ensuring topic exists",
        Some(extra),
    );

    let fluvio = Fluvio::connect().await?;
    let admin = fluvio.admin().await;

    let topic_spec =
        fluvio::metadata::topic::TopicSpec::new_computed(partitions, replication, None);
    match admin.create(topic.to_string(), false, topic_spec).await {
        Ok(_) => {
            let extra = meta! { "topic" => topic };
            logging_service::log_info(
                SERVICE_NAME,
                "ensure_topic_exists",
                "Topic created successfully",
                Some(extra),
            );
        },
        Err(e) => {
           let extra = meta! { "topic" => topic, "error" => e.to_string() };
           logging_service::log_info(
               SERVICE_NAME,
               "ensure_topic_exists",
               "Topic creation failed, it may already exist",
               Some(extra),
           );
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
        let extra = meta! { "topic" => TEST_TOPIC };
        logging_service::log_info(
            SERVICE_NAME,
            "test_produce_consume",
            "Starting produce-consume test",
            Some(extra),
        );
        if let Err(e) = ensure_topic_exists(TEST_TOPIC, 1, 1).await {
            let extra = meta! { "topic" => TEST_TOPIC, "error" => e.to_string() };
            logging_service::log_info(
                SERVICE_NAME,
                "test_produce_consume",
                "Produce-consume test failed",
                Some(extra),
            );  
            return Err(e);
        }

        let key = "test-key";
        let value = "test-value";

        let produce_handle = tokio::spawn(produce_message(TEST_TOPIC, key, value));
        let consume_handle = tokio::spawn(consume_until_value(TEST_TOPIC, value));

        let result = timeout(
            Duration::from_millis(TIMEOUT_MS),
            async {
                let produce_result = produce_handle.await;
                let consume_result = consume_handle.await;
                (produce_result, consume_result)
            },  
        ).await;

        match result {
            Ok((produce_result, consume_result)) => {
                let produce_outcome = match produce_result {
                    Ok(res) => res,
                    Err(e) => {
                        let extra = meta! { "topic" => TEST_TOPIC, "error" => e.to_string() };
                        logging_service::log_error(
                            SERVICE_NAME,
                            "test_produce_consume",
                            "Produce task panicked",
                            Some(extra),
                        );  
                        return Err(anyhow::anyhow!("Produce task panicked: {e}"));
                    }
                };
                let consume_outcome = match consume_result {
                    Ok(res) => res,
                    Err(e) => {
                        let extra = meta! { "topic" => TEST_TOPIC, "error" => e.to_string() };
                        logging_service::log_error(
                            SERVICE_NAME,
                            "test_produce_consume",
                            "Consume task panicked",
                            Some(extra),
                        );  
                        return Err(anyhow::anyhow!("Consume task panicked: {e}"));
                    }
                };

                if let Err(e) = produce_outcome {
                    let extra = meta! { "topic" => TEST_TOPIC, "error" => e.to_string() };
                    logging_service::log_error(
                        SERVICE_NAME,
                        "test_produce_consume",
                        "Produce failed",
                        Some(extra),
                    );  
                    return Err(e);
                }

                if let Err(e) = consume_outcome {
                    let extra = meta! { "topic" => TEST_TOPIC, "error" => e.to_string() };
                    logging_service::log_error(
                        SERVICE_NAME,
                        "test_produce_consume",
                        "Consume failed",
                        Some(extra),
                    );  
                    return Err(e);
                }

                let extra = meta! { "topic" => TEST_TOPIC };
                logging_service::log_info(
                    SERVICE_NAME,
                    "test_produce_consume",
                    "Produce-consume test succeeded",
                    Some(extra),
                );
                Ok(())
            }
            Err(_) => {
                let extra = meta! { "topic" => TEST_TOPIC, "timeout_ms" => TIMEOUT_MS };
                logging_service::log_error(
                    SERVICE_NAME,
                    "test_produce_consume",
                    "Test timed out",
                    Some(extra),
                );  
                Err(anyhow::anyhow!("Test timed out after {TIMEOUT_MS}ms"))
            }
        }
    }
}