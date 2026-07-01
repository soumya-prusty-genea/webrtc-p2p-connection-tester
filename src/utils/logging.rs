use env_logger::{Builder, Env};
use std::io::Write;

pub fn init_logging() {
    let mut builder = Builder::from_env(Env::default().default_filter_or("info"));
    builder
        .format(|buf, record| {
            let timestamp = buf.timestamp_millis();
            let message = format!("{}", record.args());
            writeln!(
                buf,
                "{} {:<5} {}",
                timestamp,
                record.level(),
                message.trim_start()
            )
        })
        .try_init()
        .ok();
}