use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum LogLevel {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct LogEvent {
    pub level: LogLevel,
    pub msg: String,
    pub target: String,
    pub timestamp: String,

    pub service_name: String,

    #[serde(flatten)]
    pub extra_fields: HashMap<String, serde_json::Value>,
}