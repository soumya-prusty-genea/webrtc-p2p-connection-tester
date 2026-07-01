use rust_socketio::ClientBuilder;
use serde_json::json;
use std::env;
use std::thread;
use std::time::Duration;

fn main() {
    let server_url = env::var("SERVER_URL").unwrap_or_else(|_| "http://127.0.0.1:3000".to_string());
    let token = env::var("GATEWAY_JWT").unwrap_or_default();
    let sfu_id = env::var("SFU_ID").unwrap_or_else(|_| "GW-12345".to_string());

    if token.is_empty() {
        eprintln!("GATEWAY_JWT env var is required");
        std::process::exit(2);
    }

    println!("Connecting to {server_url}/gateway with sfu_id={sfu_id}");

    let token_for_header = token.clone();
    let sfu_id_for_open = sfu_id.clone();

    let socket = match ClientBuilder::new(&server_url)
        .namespace("/gateway")
        .opening_header("Authorization", format!("Bearer {token_for_header}"))
        .on("open", move |_, socket| {
            println!("EVENT open");
            let register_payload = json!({ "sfu_id": sfu_id_for_open });
            if let Err(err) = socket.emit("sfu_register", register_payload) {
                eprintln!("emit sfu_register failed: {err}");
            }
        })
        .on("connect", |_, _| {
            println!("EVENT connect");
        })
        .on("connect_error", |payload, _| {
            println!("EVENT connect_error: {payload:?}");
        })
        .on("error", |payload, _| {
            println!("EVENT error: {payload:?}");
        })
        .on("sfu_register_ack", |payload, _| {
            println!("EVENT sfu_register_ack: {payload:?}");
        })
        .connect()
    {
        Ok(client) => client,
        Err(err) => {
            eprintln!("CONNECT FAILED: {err}");
            std::process::exit(1);
        }
    };

    thread::sleep(Duration::from_secs(5));

    let _ = socket.disconnect();

    println!("Done");
}
