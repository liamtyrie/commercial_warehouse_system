use fluvio::consumer::ConsumerConfigExtBuilder;
use fluvio::{Fluvio, Offset, RecordKey};

use futures::StreamExt;
#[allow(unused_imports)]
use std::time::Duration;
#[allow(unused_imports)]
use tokio::time::timeout;



// Produce a single key-value record to the given topic.
pub async fn produce_message(topic: &str, key: impl Into<RecordKey>, value: &str) -> anyhow::Result<()> {
    let producer = fluvio::producer(topic).await?;
    producer.send(key, value).await?;
    producer.flush().await?;
    Ok(())
}

pub async fn consume_until_value(topic: &str, target_value: &str) -> anyhow::Result<()> {
    let fluvio = Fluvio::connect().await?;

    let mut stream = fluvio.consumer_with_config(ConsumerConfigExtBuilder::default().topic(topic).partition(0).offset_start(Offset::beginning()).build()?,).await?;

    while let Some(Ok(record)) = stream.next().await {
        let key = record.get_key().map(|k| k.as_utf8_lossy_string());
        let value = record.get_value().as_utf8_lossy_string();
        println!("Got record: key={key:?}, value={value}");
        if value == target_value {
            return Ok(());
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
    async fn test_produce_and_consume() {
        let key = "test-key";
        let value = "test-value";

        let produce_handle = tokio::spawn(produce_message(TEST_TOPIC, key, value)); 
        let consume_handle = tokio::spawn(consume_until_value(TEST_TOPIC, value));

        let result = timeout(
            Duration::from_millis(TIMEOUT_MS),
            async {
                let produce_result = produce_handle.await.expect("Produce task panicked");
                let consume_result = consume_handle.await.expect("Consume task panicked");
                (produce_result, consume_result)
            },
        )
        .await;

        let (produce_result, consume_result): (anyhow::Result<()>, anyhow::Result<()>) = match result {
            Ok((produce_result, consume_result)) => (produce_result, consume_result),
            Err(_) => panic!("Test timed out after {TIMEOUT_MS}ms"),
        };

        produce_result.expect("Produce failed");
        consume_result.expect("Consume failed");
    }
}