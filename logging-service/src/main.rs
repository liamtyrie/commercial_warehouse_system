use std::io::{self, BufRead};
use logging_service::{parse_log_line};

fn main() {
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        match line {
           Ok(log_line) => {
            if log_line.trim().is_empty() {
                continue;
            }

            if !log_line.trim_start().starts_with('{') {
                println!("{}", log_line);
                continue;
            }

            match parse_log_line(&log_line) {
                Ok(event) => {
                    println!("{}", serde_json::to_string_pretty(&event).unwrap())
                }
                Err(e) => {
                    eprintln!("Failed to parse log line: {}. Error: {}", log_line, e);
                    println!("{}", log_line);
                }
            }
           }
           Err(_) => break,
        }
    }
}