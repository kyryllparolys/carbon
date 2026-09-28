use {
    async_trait::async_trait,
    carbon_core::{
        datasource::{
            AccountUpdate, Datasource, DatasourceDisconnection, DatasourceId, TransactionUpdate,
            Update, UpdateType,
        },
        error::CarbonResult,
        metrics::{Counter, Histogram, MetricsRegistry},
        transformers::yellowstone::{create_tx_meta, create_tx_versioned},
    },
    chrono::{DateTime, Utc},
    futures::{sink::SinkExt, StreamExt},
    prost::Message,
    solana_account::Account,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{
        collections::HashMap,
        convert::TryFrom,
        io,
        path::{Path, PathBuf},
        sync::LazyLock,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    },
    tokio::{
        fs::{self, File},
        io::{AsyncWriteExt, BufWriter},
        sync::{mpsc, mpsc::Sender},
        task::JoinSet,
    },
    tokio_util::sync::CancellationToken,
    yellowstone_grpc_client::{
        GeyserGrpcBuilder, GeyserGrpcBuilderResult, GeyserGrpcClient, ReconnectConfig,
    },
    yellowstone_grpc_proto::{
        geyser::{
            subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
            SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocks,
            SubscribeRequestFilterTransactions, SubscribeRequestPing, SubscribeUpdate,
            SubscribeUpdateAccountInfo, SubscribeUpdateTransactionInfo,
        },
        tonic::{codec::CompressionEncoding, transport::ClientTlsConfig},
    },
};

static ACCOUNT_PROCESS_TIME_NANOS: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::new(
        "yellowstone_grpc_account_process_time_nanoseconds",
        "Time taken to process account updates in nanoseconds",
        vec![
            1_000.0,
            10_000.0,
            100_000.0,
            1_000_000.0,
            10_000_000.0,
            100_000_000.0,
            1_000_000_000.0,
        ],
    )
});
static ACCOUNT_UPDATES_RECEIVED: Counter = Counter::new(
    "yellowstone_grpc_account_updates_received_total",
    "Total account updates received from Yellowstone gRPC",
);
static ACCOUNT_DELETION_PROCESS_TIME_NANOS: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::new(
        "yellowstone_grpc_account_deletion_process_time_nanoseconds",
        "Time taken to process account deletions in nanoseconds",
        vec![
            1_000.0,
            10_000.0,
            100_000.0,
            1_000_000.0,
            10_000_000.0,
            100_000_000.0,
            1_000_000_000.0,
        ],
    )
});
static ACCOUNT_DELETIONS_RECEIVED: Counter = Counter::new(
    "yellowstone_grpc_account_deletions_received_total",
    "Total account deletions received from Yellowstone gRPC",
);
static TRANSACTION_PROCESS_TIME_NANOS: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::new(
        "yellowstone_grpc_transaction_process_time_nanoseconds",
        "Time taken to process transaction updates in nanoseconds",
        vec![
            1_000.0,
            10_000.0,
            100_000.0,
            1_000_000.0,
            10_000_000.0,
            100_000_000.0,
            1_000_000_000.0,
        ],
    )
});
static TRANSACTION_UPDATES_RECEIVED: Counter = Counter::new(
    "yellowstone_grpc_transaction_updates_received_total",
    "Total transaction updates received from Yellowstone gRPC",
);

fn register_yellowstone_metrics() {
    let registry = MetricsRegistry::global();
    registry.register_counter(&ACCOUNT_UPDATES_RECEIVED);
    registry.register_counter(&ACCOUNT_DELETIONS_RECEIVED);
    registry.register_counter(&TRANSACTION_UPDATES_RECEIVED);
    registry.register_histogram(&ACCOUNT_PROCESS_TIME_NANOS);
    registry.register_histogram(&ACCOUNT_DELETION_PROCESS_TIME_NANOS);
    registry.register_histogram(&TRANSACTION_PROCESS_TIME_NANOS);
}

/// Default timeout for detecting stale connections (30 seconds)
pub const DEFAULT_STREAM_TIMEOUT_SECS: u64 = 30;

/// Initial delay before retrying a failed subscription attempt
const RECONNECT_INITIAL_DELAY_MS: u64 = 100;

/// Upper bound on the retry delay
const RECONNECT_MAX_DELAY_MS: u64 = 3_000;

#[derive(Debug)]
pub struct YellowstoneGrpcGeyserClient {
    pub endpoint: String,
    pub x_token: Option<String>,
    pub commitment: Option<CommitmentLevel>,
    pub account_filters: HashMap<String, SubscribeRequestFilterAccounts>,
    pub transaction_filters: HashMap<String, SubscribeRequestFilterTransactions>,
    pub block_filters: BlockFilters,
    pub geyser_config: YellowstoneGrpcClientConfig,
    pub disconnect_notifier: Option<mpsc::Sender<DatasourceDisconnection>>,
    /// Timeout for detecting hung/stale connections. Default: 30 seconds.
    pub stream_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct YellowstoneGrpcClientConfig {
    pub compression: Option<CompressionEncoding>,
    pub connect_timeout: Option<Duration>,
    pub timeout: Option<Duration>,
    pub max_decoding_message_size: Option<usize>,
    pub tls_config: Option<ClientTlsConfig>,
    pub tcp_nodelay: Option<bool>,
    /// When set, the client reconnects and replays inside the stream instead of
    /// surfacing the disconnect. Off by default.
    pub reconnect: Option<ReconnectConfig>,
    /// Optional exact protobuf capture. Messages are persisted before Carbon
    /// transforms or decodes them, so projections can be rebuilt later.
    pub raw_recorder: Option<RawRecorderConfig>,
    /// Optional first slot to request. After any disconnect the datasource
    /// resumes from the last observed slot, accepting same-slot duplicates so
    /// it cannot skip the tail of a partially delivered slot.
    pub from_slot: Option<u64>,
    /// Optional inclusive upper bound used by finite replay jobs. The stream
    /// stops when the first update after this slot arrives.
    pub stop_at_slot: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct RawRecorderConfig {
    pub directory: PathBuf,
    pub segment_max_bytes: u64,
    pub sync_interval: Duration,
    pub sync_every_messages: u64,
}

impl RawRecorderConfig {
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
            segment_max_bytes: 256 * 1024 * 1024,
            sync_interval: Duration::from_secs(1),
            sync_every_messages: 10_000,
        }
    }
}

impl Default for YellowstoneGrpcClientConfig {
    fn default() -> Self {
        Self {
            compression: None,
            connect_timeout: Some(Duration::from_secs(15)),
            timeout: Some(Duration::from_secs(15)),
            max_decoding_message_size: None,
            tls_config: None,
            tcp_nodelay: None,
            reconnect: None,
            raw_recorder: None,
            from_slot: None,
            stop_at_slot: None,
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct BlockFilters {
    pub filters: HashMap<String, SubscribeRequestFilterBlocks>,
    pub failed_transactions: Option<bool>,
}

impl YellowstoneGrpcGeyserClient {
    /// Creates a new YellowstoneGrpcGeyserClient with optional stream timeout.
    /// If `stream_timeout` is None, defaults to 30 seconds.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint: String,
        x_token: Option<String>,
        commitment: Option<CommitmentLevel>,
        account_filters: HashMap<String, SubscribeRequestFilterAccounts>,
        transaction_filters: HashMap<String, SubscribeRequestFilterTransactions>,
        block_filters: BlockFilters,
        geyser_config: YellowstoneGrpcClientConfig,
        disconnect_notifier: Option<mpsc::Sender<DatasourceDisconnection>>,
        stream_timeout: Option<Duration>,
    ) -> Self {
        YellowstoneGrpcGeyserClient {
            endpoint,
            x_token,
            commitment,
            account_filters,
            transaction_filters,
            block_filters,
            geyser_config,
            disconnect_notifier,
            stream_timeout: stream_timeout
                .unwrap_or(Duration::from_secs(DEFAULT_STREAM_TIMEOUT_SECS)),
        }
    }
}

impl YellowstoneGrpcClientConfig {
    pub const fn new(
        compression: Option<CompressionEncoding>,
        connect_timeout: Option<Duration>,
        timeout: Option<Duration>,
        max_decoding_message_size: Option<usize>,
        tls_config: Option<ClientTlsConfig>,
        tcp_nodelay: Option<bool>,
    ) -> Self {
        YellowstoneGrpcClientConfig {
            compression,
            connect_timeout,
            timeout,
            max_decoding_message_size,
            tls_config,
            tcp_nodelay,
            reconnect: None,
            raw_recorder: None,
            from_slot: None,
            stop_at_slot: None,
        }
    }

    pub fn with_reconnect(self, reconnect: ReconnectConfig) -> Self {
        YellowstoneGrpcClientConfig {
            reconnect: Some(reconnect),
            ..self
        }
    }

    pub fn with_raw_recorder(self, raw_recorder: RawRecorderConfig) -> Self {
        YellowstoneGrpcClientConfig {
            raw_recorder: Some(raw_recorder),
            ..self
        }
    }

    pub fn with_from_slot(self, from_slot: Option<u64>) -> Self {
        YellowstoneGrpcClientConfig { from_slot, ..self }
    }

    pub fn with_stop_at_slot(self, stop_at_slot: Option<u64>) -> Self {
        YellowstoneGrpcClientConfig {
            stop_at_slot,
            ..self
        }
    }

    pub fn geyser_config_builder(
        &self,
        mut builder: GeyserGrpcBuilder,
    ) -> GeyserGrpcBuilderResult<GeyserGrpcBuilder> {
        builder = builder.connect_timeout(self.connect_timeout.unwrap_or(Duration::from_secs(15)));

        builder = builder.timeout(self.timeout.unwrap_or(Duration::from_secs(15)));
        let tls = self
            .tls_config
            .clone()
            .unwrap_or_else(|| ClientTlsConfig::new().with_enabled_roots());
        builder = builder.tls_config(tls)?;

        if let Some(compression) = self.compression {
            builder = builder
                .send_compressed(compression)
                .accept_compressed(compression);
        }
        if let Some(val) = self.max_decoding_message_size {
            builder = builder.max_decoding_message_size(val);
        }

        if let Some(val) = self.tcp_nodelay {
            builder = builder.tcp_nodelay(val);
        }

        if let Some(reconnect) = self.reconnect.clone() {
            builder = builder.set_reconnect_config(reconnect);
        }
        Ok(builder)
    }
}

struct RawRecorder {
    config: RawRecorderConfig,
    writer: Option<BufWriter<File>>,
    partial_path: PathBuf,
    final_path: PathBuf,
    segment_sequence: u64,
    segment_bytes: u64,
    segment_messages: u64,
    total_messages: u64,
    messages_since_sync: u64,
    last_slot: Option<u64>,
    last_sync: Instant,
    compression_tasks: JoinSet<io::Result<()>>,
}

impl RawRecorder {
    async fn open(config: RawRecorderConfig) -> io::Result<Self> {
        fs::create_dir_all(&config.directory).await?;
        let mut recorder = Self {
            config,
            writer: None,
            partial_path: PathBuf::new(),
            final_path: PathBuf::new(),
            segment_sequence: 0,
            segment_bytes: 0,
            segment_messages: 0,
            total_messages: 0,
            messages_since_sync: 0,
            last_slot: None,
            last_sync: Instant::now(),
            compression_tasks: JoinSet::new(),
        };
        recorder.open_segment().await?;
        Ok(recorder)
    }

    async fn open_segment(&mut self) -> io::Result<()> {
        let epoch_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let stem = format!("yellowstone-{epoch_ms}-{:06}", self.segment_sequence);
        self.segment_sequence += 1;
        self.final_path = self.config.directory.join(format!("{stem}.pbld"));
        self.partial_path = self.config.directory.join(format!("{stem}.pbld.partial"));
        self.writer = Some(BufWriter::new(File::create(&self.partial_path).await?));
        self.segment_bytes = 0;
        self.segment_messages = 0;
        self.messages_since_sync = 0;
        self.last_sync = Instant::now();
        Ok(())
    }

    async fn record(&mut self, update: &SubscribeUpdate) -> io::Result<()> {
        let mut bytes = Vec::with_capacity(update.encoded_len() + 10);
        update
            .encode_length_delimited(&mut bytes)
            .map_err(io::Error::other)?;

        if self.segment_bytes > 0
            && self.segment_bytes + bytes.len() as u64 > self.config.segment_max_bytes
        {
            self.finalize_segment().await?;
            self.open_segment().await?;
        }

        self.writer
            .as_mut()
            .expect("raw recorder segment must be open")
            .write_all(&bytes)
            .await?;
        self.segment_bytes += bytes.len() as u64;
        self.segment_messages += 1;
        self.total_messages += 1;
        self.messages_since_sync += 1;
        self.last_slot = update_slot(update).or(self.last_slot);

        if self.messages_since_sync >= self.config.sync_every_messages
            || self.last_sync.elapsed() >= self.config.sync_interval
        {
            self.sync().await?;
        }
        Ok(())
    }

    async fn sync(&mut self) -> io::Result<()> {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush().await?;
            writer.get_ref().sync_data().await?;
        }
        self.write_checkpoint().await?;
        self.messages_since_sync = 0;
        self.last_sync = Instant::now();
        self.reap_compression_tasks()?;
        Ok(())
    }

    async fn write_checkpoint(&self) -> io::Result<()> {
        let checkpoint_path = self.config.directory.join("checkpoint.json");
        let temporary_path = self.config.directory.join("checkpoint.json.tmp");
        let segment = self
            .partial_path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        let last_slot = self
            .last_slot
            .map(|slot| slot.to_string())
            .unwrap_or_else(|| "null".to_string());
        let checkpoint = format!(
            concat!(
                "{{\n",
                "  \"schema_version\": 1,\n",
                "  \"format\": \"yellowstone.SubscribeUpdate.prost-length-delimited\",\n",
                "  \"current_segment\": \"{}\",\n",
                "  \"segment_bytes\": {},\n",
                "  \"segment_messages\": {},\n",
                "  \"process_messages\": {},\n",
                "  \"last_slot\": {},\n",
                "  \"synced_at\": \"{}\"\n",
                "}}\n"
            ),
            segment,
            self.segment_bytes,
            self.segment_messages,
            self.total_messages,
            last_slot,
            Utc::now().to_rfc3339(),
        );
        let mut file = File::create(&temporary_path).await?;
        file.write_all(checkpoint.as_bytes()).await?;
        file.sync_all().await?;
        drop(file);
        fs::rename(&temporary_path, &checkpoint_path).await?;
        Ok(())
    }

    async fn finalize_segment(&mut self) -> io::Result<()> {
        self.sync().await?;
        self.writer.take();
        if self.segment_messages > 0 {
            fs::rename(&self.partial_path, &self.final_path).await?;
            self.queue_compression(self.final_path.clone());
            self.partial_path = PathBuf::from(format!("{}.zst", self.final_path.display()));
            self.write_checkpoint().await?;
        } else if Path::new(&self.partial_path).exists() {
            fs::remove_file(&self.partial_path).await?;
        }
        Ok(())
    }

    fn queue_compression(&mut self, source: PathBuf) {
        self.compression_tasks
            .spawn_blocking(move || compress_zstd_and_remove(source));
    }

    fn reap_compression_tasks(&mut self) -> io::Result<()> {
        while let Some(result) = self.compression_tasks.try_join_next() {
            result.map_err(io::Error::other)??;
        }
        Ok(())
    }

    async fn finish(&mut self) -> io::Result<()> {
        self.finalize_segment().await?;
        while let Some(result) = self.compression_tasks.join_next().await {
            result.map_err(io::Error::other)??;
        }
        Ok(())
    }
}

fn compress_zstd_and_remove(source: PathBuf) -> io::Result<()> {
    let final_path = PathBuf::from(format!("{}.zst", source.display()));
    if final_path.exists() {
        std::fs::remove_file(source)?;
        return Ok(());
    }
    let partial_path = PathBuf::from(format!("{}.partial", final_path.display()));
    let mut input = std::io::BufReader::new(std::fs::File::open(&source)?);
    let output = std::io::BufWriter::new(std::fs::File::create(&partial_path)?);
    let mut encoder = zstd::stream::write::Encoder::new(output, 3)?;
    std::io::copy(&mut input, &mut encoder)?;
    let mut output = encoder.finish()?;
    std::io::Write::flush(&mut output)?;
    output.get_ref().sync_all()?;
    drop(output);
    std::fs::rename(&partial_path, &final_path)?;
    std::fs::remove_file(source)?;
    Ok(())
}

fn update_slot(update: &SubscribeUpdate) -> Option<u64> {
    match &update.update_oneof {
        Some(UpdateOneof::Account(value)) => Some(value.slot),
        Some(UpdateOneof::Transaction(value)) => Some(value.slot),
        Some(UpdateOneof::Block(value)) => Some(value.slot),
        _ => None,
    }
}

#[async_trait]
impl Datasource for YellowstoneGrpcGeyserClient {
    async fn consume(
        &self,
        id: DatasourceId,
        sender: Sender<(Update, DatasourceId)>,
        cancellation_token: CancellationToken,
    ) -> CarbonResult<()> {
        register_yellowstone_metrics();
        let endpoint = self.endpoint.clone();
        let x_token = self.x_token.clone();
        let commitment = self.commitment;
        let account_filters = self.account_filters.clone();
        let transaction_filters = self.transaction_filters.clone();
        let BlockFilters {
            filters,
            failed_transactions: block_failed_transactions,
        } = self.block_filters.clone();
        let retain_block_failed_transactions = block_failed_transactions.unwrap_or(true);
        let raw_recorder_config = self.geyser_config.raw_recorder.clone();
        let initial_from_slot = self.geyser_config.from_slot;
        let stop_at_slot = self.geyser_config.stop_at_slot;

        let builder = GeyserGrpcClient::build_from_shared(endpoint)
            .map_err(|err| carbon_core::error::Error::FailedToConsumeDatasource(err.to_string()))?
            .x_token(x_token)
            .map_err(|err| carbon_core::error::Error::FailedToConsumeDatasource(err.to_string()))?;

        let mut geyser_client = self
            .geyser_config
            .geyser_config_builder(builder)
            .map_err(|err| carbon_core::error::Error::FailedToConsumeDatasource(err.to_string()))?
            .connect()
            .await
            .map_err(|err| carbon_core::error::Error::FailedToConsumeDatasource(err.to_string()))?;

        let disconnect_tx_clone = self.disconnect_notifier.clone();
        let stream_timeout = self.stream_timeout;

        tokio::spawn(async move {
            let mut raw_recorder = match raw_recorder_config {
                Some(config) => match RawRecorder::open(config).await {
                    Ok(recorder) => Some(recorder),
                    Err(error) => {
                        log::error!("Failed to initialize raw Yellowstone recorder: {error}");
                        cancellation_token.cancel();
                        return;
                    }
                },
                None => None,
            };
            let base_subscribe_request = SubscribeRequest {
                slots: HashMap::new(),
                accounts: account_filters,
                transactions: transaction_filters,
                transactions_status: HashMap::new(),
                entry: HashMap::new(),
                blocks: filters,
                blocks_meta: HashMap::new(),
                commitment: commitment.map(|x| x as i32),
                accounts_data_slice: vec![],
                ping: None,
                from_slot: initial_from_slot,
            };

            let id_for_loop = id.clone();

            let mut last_disconnect_time: Option<DateTime<Utc>> = None;
            let mut last_slot_before_disconnect: Option<u64> = None;
            let mut last_processed_slot: u64 = 0;
            let mut reconnect_delay = Duration::from_millis(RECONNECT_INITIAL_DELAY_MS);

            loop {
                let mut subscribe_request = base_subscribe_request.clone();
                if last_processed_slot > 0 {
                    subscribe_request.from_slot = Some(last_processed_slot);
                }
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        log::info!("Cancelling Yellowstone gRPC subscription.");
                        break;
                    }
                    result = geyser_client.subscribe_with_request(Some(subscribe_request.clone())) => {
                        match result {
                            Ok((mut subscribe_tx, mut stream)) => {
                                let mut received_stream_message = false;
                                let mut first_message_after_reconnect = last_disconnect_time.is_some();

                                loop {
                                    if cancellation_token.is_cancelled() {
                                        break;
                                    }

                                    let message_result = tokio::time::timeout(
                                        stream_timeout,
                                        stream.next()
                                    ).await;

                                    let message = match message_result {
                                        Ok(Some(msg)) => msg,
                                        Ok(None) => {
                                            log::warn!("Stream closed");
                                            if last_disconnect_time.is_none() {
                                                last_disconnect_time = Some(Utc::now());
                                                last_slot_before_disconnect = Some(last_processed_slot);
                                                log::warn!("Disconnected at slot {last_processed_slot}");
                                            }
                                            break;
                                        }
                                        Err(_) => {
                                            log::warn!("Stream timeout - no messages for {stream_timeout:?}");
                                            if last_disconnect_time.is_none() {
                                                last_disconnect_time = Some(Utc::now());
                                                last_slot_before_disconnect = Some(last_processed_slot);
                                                log::warn!("Disconnected at slot {last_processed_slot} (timeout)");
                                            }
                                            break;
                                        }
                                    };

                                    match message {
                                        Ok(msg) => {
                                            received_stream_message = true;
                                            reconnect_delay = Duration::from_millis(RECONNECT_INITIAL_DELAY_MS);
                                            if stop_at_slot.is_some_and(|target| {
                                                update_slot(&msg).is_some_and(|slot| slot > target)
                                            }) {
                                                if let Some(recorder) = raw_recorder.as_mut() {
                                                    if let Err(error) = recorder.finish().await {
                                                        log::error!("Failed to finalize finite replay: {error}");
                                                    }
                                                }
                                                log::info!("Finite Yellowstone replay reached target slot {}", stop_at_slot.unwrap_or_default());
                                                return;
                                            }
                                            if let Some(recorder) = raw_recorder.as_mut() {
                                                if let Err(error) = recorder.record(&msg).await {
                                                    log::error!("Raw Yellowstone persistence failed; stopping pipeline: {error}");
                                                    cancellation_token.cancel();
                                                    return;
                                                }
                                            }
                                            if first_message_after_reconnect {
                                                let current_slot = match &msg.update_oneof {
                                                    Some(UpdateOneof::Account(ref update)) => Some(update.slot),
                                                    Some(UpdateOneof::Transaction(ref update)) => Some(update.slot),
                                                    Some(UpdateOneof::Block(ref update)) => Some(update.slot),
                                                    _ => None,
                                                };

                                                if let Some(slot) = current_slot {
                                                    first_message_after_reconnect = false;

                                                    if let (Some(disconnect_time), Some(last_slot)) =
                                                        (last_disconnect_time.take(), last_slot_before_disconnect.take())
                                                    {
                                                        let missed = slot.saturating_sub(last_slot);

                                                        let disconnection = DatasourceDisconnection {
                                                            source: "yellowstone-grpc".to_string(),
                                                            disconnect_time,
                                                            last_slot_before_disconnect: last_slot,
                                                            first_slot_after_reconnect: slot,
                                                            missed_slots: missed,
                                                        };

                                                        if let Some(tx) = &disconnect_tx_clone {
                                                            let _ = tx.try_send(disconnection);
                                                        }

                                                        log::info!("Reconnected. Slots: {last_slot} -> {slot} (missed: {missed})");
                                                    }
                                                }
                                            }

                                            match msg.update_oneof {
                                            Some(UpdateOneof::Account(account_update)) => {
                                                last_processed_slot = account_update.slot;
                                                send_subscribe_account_update_info(
                                                    account_update.account,
                                                    &sender,
                                                    id_for_loop.clone(),
                                                    account_update.slot,
                                                )
                                                .await
                                            }

                                            Some(UpdateOneof::Transaction(transaction_update)) => {
                                                last_processed_slot = transaction_update.slot;
                                                send_subscribe_update_transaction_info(
                                                    transaction_update.transaction,
                                                    &sender,
                                                    id_for_loop.clone(),
                                                    transaction_update.slot,
                                                    None,
                                                )
                                                .await
                                            }
                                            Some(UpdateOneof::Block(block_update)) => {
                                                last_processed_slot = block_update.slot;
                                                let block_time = block_update.block_time.map(|ts| ts.timestamp);

                                                for transaction_update in block_update.transactions {
                                                    if retain_block_failed_transactions || transaction_update.meta.as_ref().map(|meta| meta.err.is_none()).unwrap_or(false) {
                                                        send_subscribe_update_transaction_info(Some(transaction_update), &sender, id_for_loop.clone(), block_update.slot, block_time).await
                                                    }
                                                }

                                                for account_info in block_update.accounts {
                                                    send_subscribe_account_update_info(
                                                        Some(account_info),
                                                        &sender,
                                                        id_for_loop.clone(),
                                                        block_update.slot,
                                                    )
                                                    .await;
                                                }
                                            }

                                            Some(UpdateOneof::Ping(_)) => {
                                                // Sink replays the last request on reconnect.
                                                match subscribe_tx
                                                    .send(SubscribeRequest {
                                                        ping: Some(SubscribeRequestPing { id: 1 }),
                                                        ..subscribe_request.clone()
                                                    })
                                                    .await {
                                                        Ok(()) => (),
                                                        Err(error) => {
                                                            log::error!("Failed to send ping error: {error:?}");
                                                            break;
                                                        },
                                                    }
                                            }

                                            _ => {}
                                        }
                                        }
                                        Err(error) => {
                                            log::error!("Geyser stream error: {error:?}");

                                            if last_disconnect_time.is_none() {
                                                last_disconnect_time = Some(Utc::now());
                                                last_slot_before_disconnect = Some(last_processed_slot);
                                                log::error!("Disconnected at slot {last_processed_slot}");
                                            }

                                            break;
                                        }
                                    }
                                }

                                if !cancellation_token.is_cancelled() {
                                    tokio::time::sleep(reconnect_delay).await;
                                    if !received_stream_message {
                                        reconnect_delay = (reconnect_delay * 2)
                                            .min(Duration::from_millis(RECONNECT_MAX_DELAY_MS));
                                    }
                                }
                            }
                            Err(e) => {
                                log::error!("Failed to subscribe: {e:?}");

                                if last_disconnect_time.is_none() {
                                    last_disconnect_time = Some(Utc::now());
                                    last_slot_before_disconnect = Some(last_processed_slot);
                                }

                                tokio::select! {
                                    _ = cancellation_token.cancelled() => {
                                        log::info!("Cancelling Yellowstone gRPC subscription.");
                                        break;
                                    }
                                    _ = tokio::time::sleep(reconnect_delay) => {}
                                }

                                reconnect_delay = (reconnect_delay * 2)
                                    .min(Duration::from_millis(RECONNECT_MAX_DELAY_MS));
                            }
                        }
                    }
                }
            }

            if let Some(recorder) = raw_recorder.as_mut() {
                if let Err(error) = recorder.finish().await {
                    log::error!("Failed to finalize raw Yellowstone segment: {error}");
                }
            }
        });

        Ok(())
    }

    fn update_types(&self) -> Vec<UpdateType> {
        vec![
            UpdateType::AccountUpdate,
            UpdateType::Transaction,
            UpdateType::AccountDeletion,
        ]
    }
}

async fn send_subscribe_account_update_info(
    account_update_info: Option<SubscribeUpdateAccountInfo>,
    sender: &Sender<(Update, DatasourceId)>,
    id: DatasourceId,
    slot: u64,
) {
    let start_time = std::time::Instant::now();

    if let Some(account_info) = account_update_info {
        let Ok(account_pubkey) = Pubkey::try_from(account_info.pubkey) else {
            return;
        };

        let Ok(account_owner_pubkey) = Pubkey::try_from(account_info.owner) else {
            return;
        };

        let account = Account {
            lamports: account_info.lamports,
            data: account_info.data,
            owner: account_owner_pubkey,
            executable: account_info.executable,
            rent_epoch: account_info.rent_epoch,
        };

        let update = AccountUpdate {
            pubkey: account_pubkey,
            account,
            slot,
            transaction_signature: account_info
                .txn_signature
                .and_then(|sig| Signature::try_from(sig).ok()),
        }
        .into_update();
        let is_deletion = matches!(&update, Update::AccountDeletion(_));

        if let Err(e) = sender.try_send((update, id)) {
            log::error!(
                "Failed to send account event for pubkey {account_pubkey:?} at slot {slot}: {e:?}"
            );
        }

        if is_deletion {
            ACCOUNT_DELETION_PROCESS_TIME_NANOS.record(start_time.elapsed().as_nanos() as f64);
            ACCOUNT_DELETIONS_RECEIVED.inc();
        } else {
            ACCOUNT_PROCESS_TIME_NANOS.record(start_time.elapsed().as_nanos() as f64);
            ACCOUNT_UPDATES_RECEIVED.inc();
        }
    } else {
        log::error!("No account info in UpdateOneof::Account at slot {slot}");
    }
}

async fn send_subscribe_update_transaction_info(
    transaction_info: Option<SubscribeUpdateTransactionInfo>,
    sender: &Sender<(Update, DatasourceId)>,
    id: DatasourceId,
    slot: u64,
    block_time: Option<i64>,
) {
    let start_time = std::time::Instant::now();

    if let Some(transaction_info) = transaction_info {
        let Ok(signature) = Signature::try_from(transaction_info.signature) else {
            return;
        };
        let Some(yellowstone_transaction) = transaction_info.transaction else {
            return;
        };
        let Some(yellowstone_tx_meta) = transaction_info.meta else {
            return;
        };
        let Ok(versioned_transaction) = create_tx_versioned(yellowstone_transaction) else {
            return;
        };
        let meta_original = match create_tx_meta(yellowstone_tx_meta) {
            Ok(meta) => meta,
            Err(err) => {
                log::error!("Failed to create transaction meta: {err:?}");
                return;
            }
        };
        let update = Update::Transaction(Box::new(TransactionUpdate {
            signature,
            transaction: versioned_transaction,
            meta: meta_original,
            is_vote: transaction_info.is_vote,
            slot,
            index: Some(transaction_info.index),
            block_time,
            block_hash: None,
        }));
        if let Err(e) = sender.try_send((update, id)) {
            log::error!(
                "Failed to send transaction update with signature {signature:?} at slot {slot}: {e:?}"
            );
            return;
        }

        TRANSACTION_PROCESS_TIME_NANOS.record(start_time.elapsed().as_nanos() as f64);
        TRANSACTION_UPDATES_RECEIVED.inc();
    } else {
        log::error!("No transaction info in `UpdateOneof::Transaction` at slot {slot}");
    }
}
