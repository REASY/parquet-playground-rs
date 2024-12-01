use std::env;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::SubscriberBuilder;
use tracing_subscriber::fmt::format::{DefaultFields, Format};

pub fn setup(log_level: &str) {
    if env::var_os("RUST_LOG").is_none() {
        let env = format!("parquet_playground_rs={log_level}");
        unsafe {
            env::set_var("RUST_LOG", env);
        }
    }
    let subscriber = get_subscriber();
    subscriber.init();
}

pub fn get_subscriber() -> SubscriberBuilder<DefaultFields, Format, EnvFilter> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_thread_names(true)
}
