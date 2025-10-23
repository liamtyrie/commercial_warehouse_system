pub mod log_event;
use std::collections::HashMap;
use lazy_static::lazy_static;
use prometheus::{opts, register_int_counter_vec, IntCounterVec};

use log_event::LogEvent;
use crate::log_event::LogLevel;

lazy_static! {
    pub static ref LOG_EVENTS_TOTAL: IntCounterVec = register_int_counter_vec!(
        opts!(
            "log_events_total",
            "Total number of log events processed"
        ),
        &["level", "service_name", "target"]
    )
    .expect("Failed to create LOG_EVENTS_TOTAL metric");
}

#[macro_export]
macro_rules! log {
    (INFO, $service:expr, $target:expr, $msg:expr) => {
        $crate::log_info($service, $target, $msg, None)
    };
    (INFO, $service:expr, $target:expr, $msg:expr, $($key:expr => $value:expr),+ $(,)?) => {{
        let mut extra = std::collections::HashMap::new();
        $(
            extra.insert($key.to_string(), serde_json::json!($value));
        )+
        $crate::log_info($service, $target, $msg, Some(extra))
    }};
    (DEBUG, $service:expr, $target:expr, $msg:expr) => {
        $crate::log_debug($service, $target, $msg, None)
    };
    (DEBUG, $service:expr, $target:expr, $msg:expr, $($key:expr => $value:expr),+ $(,)?) => {{
        let mut extra = std::collections::HashMap::new();
        $(
            extra.insert($key.to_string(), serde_json::json!($value));
        )+
        $crate::log_debug($service, $target, $msg, Some(extra))
    }};
    
    (WARN, $service:expr, $target:expr, $msg:expr) => {
        $crate::log_warn($service, $target, $msg, None)
    };
    (WARN, $service:expr, $target:expr, $msg:expr, $($key:expr => $value:expr),+ $(,)?) => {{
        let mut extra = std::collections::HashMap::new();
        $(
            extra.insert($key.to_string(), serde_json::json!($value));
        )+
        $crate::log_warn($service, $target, $msg, Some(extra))
    }};
    
    (ERROR, $service:expr, $target:expr, $msg:expr) => {
        $crate::log_error($service, $target, $msg, None)
    };
    (ERROR, $service:expr, $target:expr, $msg:expr, $($key:expr => $value:expr),+ $(,)?) => {{
        let mut extra = std::collections::HashMap::new();
        $(
            extra.insert($key.to_string(), serde_json::json!($value));
        )+
        $crate::log_error($service, $target, $msg, Some(extra))
    }};
    
    (TRACE, $service:expr, $target:expr, $msg:expr) => {
        $crate::log_trace($service, $target, $msg, None)
    };
    (TRACE, $service:expr, $target:expr, $msg:expr, $($key:expr => $value:expr),+ $(,)?) => {{
        let mut extra = std::collections::HashMap::new();
        $(
            extra.insert($key.to_string(), serde_json::json!($value));
        )+
        $crate::log_trace($service, $target, $msg, Some(extra))
    }};
}

pub fn parse_log_line(line: &str) -> Result<LogEvent, serde_json::Error> {
    serde_json::from_str(line)
}

pub fn should_alert(log: &LogEvent) -> bool {
    log.level == LogLevel::ERROR
}

pub fn enrich_event(event: &mut LogEvent, timestamp: &str) {
    event.extra_fields.insert(
        "processed_at".to_string(),
        serde_json::Value::String(timestamp.to_string()),
    );
}


const SENSITIVE_KEYS: &[&str] = &["password", "employee_id", "user_token", "api_key"];
fn scrub_value_recursive(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, val) in map.iter_mut() {
                if SENSITIVE_KEYS.iter().any(|&k| k.eq_ignore_ascii_case(key)) {
                    *val = serde_json::json!("[REDACTED]");
                } else {
                    scrub_value_recursive(val);
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                scrub_value_recursive(v);
            }
        }
        _ => {}
    }
}

pub fn scrub_pii(event: &mut LogEvent) {
    for (key, value) in event.extra_fields.iter_mut() {
        if SENSITIVE_KEYS.iter().any(|&k| k.eq_ignore_ascii_case(key)) {
            *value = serde_json::json!("[REDACTED]");
        } else {
            scrub_value_recursive(value)
        }
    }
}

pub fn log_info(service_name: &str, target: &str, message: &str, extra_fields: Option<HashMap<String, serde_json::Value>>) {
    emit_log(
        LogLevel::INFO,
        service_name,
        target,
        message,
        extra_fields,
    );
}

pub fn log_warn(service_name: &str, target: &str, message: &str, extra_fields: Option<HashMap<String, serde_json::Value>>) {
    emit_log(
        LogLevel::WARN,
        service_name,
        target,
        message,
        extra_fields,
    );
}

pub fn log_error(service_name: &str, target: &str, message: &str, extra_fields: Option<HashMap<String, serde_json::Value>>) {
    emit_log(
        LogLevel::ERROR,
        service_name,
        target,
        message,
        extra_fields,
    );
}

pub fn log_trace(service_name: &str, target: &str, message: &str, extra_fields: Option<HashMap<String, serde_json::Value>>) {
    emit_log(
        LogLevel::TRACE,
        service_name,
        target,
        message,
        extra_fields,
    );
}  

fn emit_log(
    level: LogLevel,
    service_name: &str,
    target: &str,
    message: &str,
    extra_fields: Option<HashMap<String, serde_json::Value>>,
) {
    LOG_EVENTS_TOTAL.with_label_values(&[level.as_str(), service_name, target]).inc();
    let log_event = LogEvent {
        level,
        msg: message.to_string(),
        target: target.to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        service_name: service_name.to_string(),
        extra_fields: extra_fields.unwrap_or_default(), 
    };

    let log_json = serde_json::to_string(&log_event).unwrap_or_else(|_| "Failed to serialize log".to_string());

    if matches!(log_event.level, LogLevel::ERROR) {
        eprintln!("{}", log_json);
    } else {
        println!("{}", log_json);
    }   
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

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

    #[test]
    fn should_add_processed_at_a_timestamp_to_event() {
        let timestamp = "2025-10-16T15:44:00Z";

        let mut event = LogEvent {
            level: LogLevel::INFO,
            msg: "User logged in".to_string(),
            target: "auth_service::login".to_string(),
            timestamp: "2025-10-16T15:43:00Z".to_string(),
            service_name: "auth-service".to_string(),
            extra_fields: HashMap::new(),
        };

        enrich_event(&mut event, timestamp);

        assert!(event.extra_fields.contains_key("processed_at"));
        assert_eq!(
            event.extra_fields.get("processed_at").unwrap(),
            &serde_json::json!(timestamp)
        );
    }

    #[test]
    fn should_scrub_sensitive_fields_by_key() {
        let mut event = LogEvent {
            level: LogLevel::WARN,
            msg: "Failed login attempt for employee".to_string(),
            target: "auth_service::login".to_string(),
            timestamp: "2025-10-16T18:00:00Z".to_string(),
            service_name: "auth-service".to_string(),
            extra_fields: [
                ("employee_id".to_string(), serde_json::json!("AMZ-1138")),
                ("ip_address".to_string(), serde_json::json!("10.20.1.103")),
                (
                    "login_details".to_string(),
                    serde_json::json!({
                        "user_agent": "AMZ-Terminal/1.2",
                        "credentials": {
                            "password": "12345qwerty!"
                        }
                    }),
                ),
            ]
            .into_iter()
            .collect(),
        };

        scrub_pii(&mut event);

        assert_eq!(
            event.extra_fields.get("employee_id").unwrap(),
            &serde_json::json!("[REDACTED]")
        );

        assert_eq!(
            event.extra_fields.get("ip_address").unwrap(),
            &serde_json::json!("10.20.1.103")
        );

        let nested_password = &event.extra_fields["login_details"]["credentials"]["password"];
        assert_eq!(nested_password, &serde_json::json!("[REDACTED]"));
    }
}
