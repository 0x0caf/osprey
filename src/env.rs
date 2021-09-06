use std::env;

pub struct Env {}

impl Env {
    pub fn get_value_or_default(key: &str, default: &str) -> String {
        match env::var(key) {
            Ok(v) => v,
            Err(_) => default.to_string(),
        }
    }
}
