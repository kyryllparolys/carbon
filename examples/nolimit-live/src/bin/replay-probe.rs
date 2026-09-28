use {
    futures::StreamExt,
    std::{collections::HashMap, env, time::Duration},
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::{
        geyser::{
            subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
            SubscribeRequestFilterSlots,
        },
        tonic::transport::ClientTlsConfig,
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls default provider");

    let endpoint = env::var("GEYSER_URL")?;
    let token = env::var("X_TOKEN")?;
    let from_slot = env::var("PROBE_FROM_SLOT")?.parse::<u64>()?;
    let timeout_seconds = env::var("PROBE_TIMEOUT_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(20);

    let builder = GeyserGrpcClient::build_from_shared(endpoint)?
        .x_token(Some(token))?
        .tls_config(ClientTlsConfig::new().with_enabled_roots())?;
    let mut client = builder.connect().await?;

    let slots = HashMap::from([(
        "replay_probe".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(true),
            interslot_updates: Some(false),
        },
    )]);
    let request = SubscribeRequest {
        slots,
        accounts: HashMap::new(),
        transactions: HashMap::new(),
        transactions_status: HashMap::new(),
        entry: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        commitment: Some(CommitmentLevel::Processed as i32),
        accounts_data_slice: vec![],
        ping: None,
        from_slot: Some(from_slot),
    };

    let (_request_sink, mut stream) = client.subscribe_with_request(Some(request)).await?;
    let update = tokio::time::timeout(Duration::from_secs(timeout_seconds), stream.next())
        .await
        .map_err(|_| format!("no replay response within {timeout_seconds} seconds"))?
        .ok_or("replay stream closed without a response")??;

    let received_slot = match update.update_oneof {
        Some(UpdateOneof::Slot(update)) => update.slot,
        other => return Err(format!("unexpected first replay update: {other:?}").into()),
    };

    println!("requested_from_slot={from_slot}");
    println!("first_received_slot={received_slot}");
    println!("slot_distance={}", received_slot.saturating_sub(from_slot));
    Ok(())
}
