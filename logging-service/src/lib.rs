pub mod log_event;
use log_event::LogEvent;

use crate::log_event::LogLevel;

pub fn parse_log_line(line: &str) -> Result<LogEvent, serde_json::Error> {
    serde_json::from_str(line)
}

pub fn should_alert(log: &LogEvent) -> bool {
    log.level == LogLevel::ERROR
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_event::{LogEvent, LogLevel};

    #[test]
    fn should_deserialize_a_valid_info_log_line() {
        let log_line = r#"
        {
            "level": "INFO",
            "msg": "Order created successfully",
            "target": "order_service::orders",
            "timestamp": "2025-10-14T14:15:00Z",
            "service_name": "order-service",
            "order_id": "xyz-123",
            "user_id": 42
    }
"#;

        let expected_event = LogEvent {
            level: LogLevel::INFO,
            msg: "Order created successfully".to_string(),
            target: "order_service::orders".to_string(),
            timestamp: "2025-10-14T14:15:00Z".to_string(),
            service_name: "order-service".to_string(),
            extra_fields: [
                ("order_id".to_string(), serde_json::json!("xyz-123")),
                ("user_id".to_string(), serde_json::json!(42)),
            ]
            .into_iter()
            .collect(),
        };

        let result = parse_log_line(log_line).unwrap();
        assert_eq!(result, expected_event)
    }

    #[test]
    fn should_identify_error_logs_for_alerting() {
        let error_log = LogEvent {
            level: LogLevel::ERROR,
            msg: "Order failed".to_string(),
            target: "order_service::orders".to_string(),
            timestamp: "2025-10-15T09:00:00Z".to_string(),
            service_name: "order-service".to_string(),
            extra_fields: [
                ("order_id".to_string(), serde_json::json!("xyz-123")),
                ("user_id".to_string(), serde_json::json!(42)),
            ]
            .into_iter()
            .collect(),
        };
        let info_log = LogEvent {
            level: LogLevel::INFO,
            msg: "Order created successfully".to_string(),
            target: "order_service::orders".to_string(),
            timestamp: "2025-10-15T09:00:00Z".to_string(),
            service_name: "order-service".to_string(),
            extra_fields: [
                ("order_id".to_string(), serde_json::json!("xyz-123")),
                ("user_id".to_string(), serde_json::json!(42)),
            ]
            .into_iter()
            .collect(),
        };

        assert!(should_alert(&error_log));
        assert!(!should_alert(&info_log));
    }
}
