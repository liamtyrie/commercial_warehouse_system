use std::io::{self, BufRead};
use logging_service::{parse_log_line};

fn main() {
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        match line {
            Ok(log_line) => {
                match parse_log_line(&log_line) {
                    Ok(event) => {
                        println!("{}", serde_json::to_string(&event).unwrap());

                    }
                    Err(e) => {
                        eprintln!("Failed to parse log line: {}", e);
                    }
                }
            }
            Err(_) => break,
        }
    }
}