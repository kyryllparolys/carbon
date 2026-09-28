use {
    carbon_bonkswap_decoder::{
        instructions::BonkswapInstruction, BonkswapDecoder, PROGRAM_ID as BONKSWAP_ID,
    },
    carbon_boop_decoder::{instructions::BoopInstruction, BoopDecoder, PROGRAM_ID as BOOP_ID},
    carbon_core::{
        datasource::DatasourceId,
        error::{CarbonResult, Error as CarbonError},
        filter::{DatasourceFilter, Filter},
        instruction::InstructionProcessorInputType,
        processor::Processor,
    },
    carbon_heaven_decoder::{
        instructions::HeavenInstruction, HeavenDecoder, PROGRAM_ID as HEAVEN_ID,
    },
    carbon_jupiter_swap_decoder::{
        instructions::JupiterSwapInstruction, JupiterSwapDecoder, PROGRAM_ID as JUPITER_ID,
    },
    carbon_lifinity_amm_v2_decoder::{
        instructions::LifinityAmmV2Instruction, LifinityAmmV2Decoder, PROGRAM_ID as LIFINITY_ID,
    },
    carbon_log_metrics::LogMetrics,
    carbon_meteora_damm_v2_decoder::{
        instructions::MeteoraDammV2Instruction, MeteoraDammV2Decoder,
        PROGRAM_ID as METEORA_DAMM_V2_ID,
    },
    carbon_meteora_dbc_decoder::{
        instructions::MeteoraDbcInstruction, MeteoraDbcDecoder, PROGRAM_ID as METEORA_DBC_ID,
    },
    carbon_meteora_dlmm_decoder::{
        instructions::MeteoraDlmmInstruction, MeteoraDlmmDecoder, PROGRAM_ID as METEORA_DLMM_ID,
    },
    carbon_meteora_pools_decoder::{
        instructions::MeteoraPoolsInstruction, MeteoraPoolsDecoder, PROGRAM_ID as METEORA_POOLS_ID,
    },
    carbon_meteora_vault_decoder::{
        instructions::MeteoraVaultInstruction, MeteoraVaultDecoder, PROGRAM_ID as METEORA_VAULT_ID,
    },
    carbon_moonshot_decoder::{
        instructions::MoonshotInstruction, MoonshotDecoder, PROGRAM_ID as MOONSHOT_ID,
    },
    carbon_mpl_token_metadata_decoder::{
        instructions::MplTokenMetadataInstruction, MplTokenMetadataDecoder,
        PROGRAM_ID as MPL_METADATA_ID,
    },
    carbon_openbook_v2_decoder::{
        instructions::OpenbookV2Instruction, OpenbookV2Decoder, PROGRAM_ID as OPENBOOK_V2_ID,
    },
    carbon_orca_whirlpool_decoder::{
        instructions::OrcaWhirlpoolInstruction, OrcaWhirlpoolDecoder,
        PROGRAM_ID as ORCA_WHIRLPOOL_ID,
    },
    carbon_pump_fees_decoder::{
        instructions::PumpFeesInstruction, PumpFeesDecoder, PROGRAM_ID as PUMP_FEES_ID,
    },
    carbon_pump_swap_decoder::{
        instructions::PumpSwapInstruction, PumpSwapDecoder, PROGRAM_ID as PUMP_SWAP_ID,
    },
    carbon_pumpfun_decoder::{
        instructions::PumpfunInstruction, PumpfunDecoder, PROGRAM_ID as PUMPFUN_ID,
    },
    carbon_raydium_amm_v4_decoder::{
        instructions::RaydiumAmmV4Instruction, RaydiumAmmV4Decoder, PROGRAM_ID as RAYDIUM_AMM_V4_ID,
    },
    carbon_raydium_clmm_decoder::{
        instructions::RaydiumClmmInstruction, RaydiumClmmDecoder, PROGRAM_ID as RAYDIUM_CLMM_ID,
    },
    carbon_raydium_cpmm_decoder::{
        instructions::RaydiumCpmmInstruction, RaydiumCpmmDecoder, PROGRAM_ID as RAYDIUM_CPMM_ID,
    },
    carbon_raydium_launchpad_decoder::{
        instructions::RaydiumLaunchpadInstruction, RaydiumLaunchpadDecoder,
        PROGRAM_ID as RAYDIUM_LAUNCHPAD_ID,
    },
    carbon_raydium_stable_swap_decoder::{
        instructions::RaydiumStableSwapInstruction, RaydiumStableSwapDecoder,
        PROGRAM_ID as RAYDIUM_STABLE_ID,
    },
    carbon_vertigo_decoder::{
        instructions::VertigoInstruction, VertigoDecoder, PROGRAM_ID as VERTIGO_ID,
    },
    carbon_virtuals_decoder::{
        instructions::VirtualsInstruction, VirtualsDecoder, PROGRAM_ID as VIRTUALS_ID,
    },
    carbon_yellowstone_grpc_datasource::{
        RawRecorderConfig, YellowstoneGrpcClientConfig, YellowstoneGrpcGeyserClient,
    },
    chrono::Utc,
    serde::Serialize,
    serde_json::json,
    std::{
        collections::HashMap,
        env, io,
        path::{Path, PathBuf},
        sync::Arc,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    },
    tokio::{
        fs::{self, File, OpenOptions},
        io::{AsyncWriteExt, BufWriter},
        sync::{mpsc, Mutex},
        task::JoinSet,
    },
    tokio_util::sync::CancellationToken,
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::{
        geyser::{CommitmentLevel, SubscribeRequestFilterTransactions},
        tonic::transport::ClientTlsConfig,
    },
};

const DEFAULT_DATA_ROOT: &str = "/Volumes/CHAIN_DATA/solana/live";
const DECODED_SEGMENT_BYTES: u64 = 256 * 1024 * 1024;

#[tokio::main]
async fn main() -> CarbonResult<()> {
    dotenv::dotenv().ok();
    env_logger::init();

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls default provider");

    let endpoint = env::var("GEYSER_URL").expect("GEYSER_URL must be set");
    let token = env::var("X_TOKEN").expect("X_TOKEN must be set");
    let data_root = PathBuf::from(
        env::var("SOLANA_LIVE_DATA_ROOT").unwrap_or_else(|_| DEFAULT_DATA_ROOT.to_string()),
    );
    ensure_apfs_data_root(&data_root).map_err(carbon_error)?;

    let raw_directory = data_root.join("raw/yellowstone/confirmed");
    let decoded_directory = data_root.join("decoded/instructions/confirmed");
    let repair_root = data_root.join("raw/yellowstone/repair-processed");
    let state_directory = data_root.join("state");
    fs::create_dir_all(&raw_directory)
        .await
        .map_err(carbon_error)?;
    fs::create_dir_all(&decoded_directory)
        .await
        .map_err(carbon_error)?;
    fs::create_dir_all(&repair_root)
        .await
        .map_err(carbon_error)?;
    fs::create_dir_all(&state_directory)
        .await
        .map_err(carbon_error)?;

    let repair_restart_gaps = env_flag("REPAIR_RESTART_GAPS", true);
    let previous_confirmed_slot = if repair_restart_gaps && env_flag("RESUME_FROM_CHECKPOINT", true)
    {
        checkpoint_slot(&raw_directory)
    } else {
        None
    };
    if !repair_restart_gaps {
        log::info!("automatic restart-gap repair is disabled");
    }
    let startup_confirmed_slot = current_confirmed_slot(&endpoint, &token).await?;

    let sink = Arc::new(Mutex::new(
        SegmentedJsonlWriter::open(decoded_directory, DECODED_SEGMENT_BYTES)
            .await
            .map_err(carbon_error)?,
    ));
    let cancellation = CancellationToken::new();
    let processor =
        |decoder| JsonlInstructionProcessor::new(decoder, Arc::clone(&sink), cancellation.clone());

    let (confirmed_disconnect_sender, confirmed_disconnect_receiver) = mpsc::channel(128);
    let confirmed_gap_writer = tokio::spawn(write_gaps(
        confirmed_disconnect_receiver,
        state_directory.join("gaps-confirmed.jsonl"),
    ));

    let program_ids = vec![
        PUMPFUN_ID,
        PUMP_SWAP_ID,
        PUMP_FEES_ID,
        RAYDIUM_AMM_V4_ID,
        RAYDIUM_CLMM_ID,
        RAYDIUM_CPMM_ID,
        RAYDIUM_LAUNCHPAD_ID,
        RAYDIUM_STABLE_ID,
        METEORA_DLMM_ID,
        METEORA_DBC_ID,
        METEORA_DAMM_V2_ID,
        METEORA_POOLS_ID,
        METEORA_VAULT_ID,
        ORCA_WHIRLPOOL_ID,
        JUPITER_ID,
        MOONSHOT_ID,
        BOOP_ID,
        BONKSWAP_ID,
        HEAVEN_ID,
        LIFINITY_ID,
        OPENBOOK_V2_ID,
        VERTIGO_ID,
        VIRTUALS_ID,
        MPL_METADATA_ID,
    ]
    .into_iter()
    .map(|program_id| program_id.to_string())
    .collect::<Vec<_>>();

    let mut transaction_filters = HashMap::new();
    transaction_filters.insert(
        "launchpads_dex_metadata".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            // None deliberately keeps both successful and failed transactions.
            failed: None,
            account_include: program_ids,
            account_exclude: vec![],
            account_required: vec![],
            signature: None,
            cuckoo_account_include: None,
            token_accounts: None,
        },
    );

    let mut confirmed_raw_config = RawRecorderConfig::new(&raw_directory);
    confirmed_raw_config.segment_max_bytes = 256 * 1024 * 1024;
    confirmed_raw_config.sync_interval = Duration::from_secs(1);
    confirmed_raw_config.sync_every_messages = 10_000;

    let confirmed_datasource = YellowstoneGrpcGeyserClient::new(
        endpoint.clone(),
        Some(token.clone()),
        Some(CommitmentLevel::Confirmed),
        HashMap::new(),
        transaction_filters.clone(),
        Default::default(),
        YellowstoneGrpcClientConfig::default().with_raw_recorder(confirmed_raw_config),
        Some(confirmed_disconnect_sender.clone()),
        Some(Duration::from_secs(30)),
    );

    let confirmed_id = DatasourceId::new_named("yellowstone-confirmed");
    let confirmed_filter_id = confirmed_id.clone();
    let confirmed_only = move || -> Vec<Box<dyn Filter>> {
        vec![Box::new(DatasourceFilter::new(confirmed_filter_id.clone()))]
    };

    let mut pipeline_builder = carbon_core::pipeline::Pipeline::builder()
        .datasource_with_id(confirmed_datasource, confirmed_id);

    if let Some(previous_slot) = previous_confirmed_slot {
        if previous_slot < startup_confirmed_slot {
            let repair_directory =
                repair_root.join(format!("{previous_slot}-{}", startup_confirmed_slot));
            fs::create_dir_all(&repair_directory)
                .await
                .map_err(carbon_error)?;
            let manifest = json!({
                "schema_version": 1,
                "commitment": "processed",
                "purpose": "finite confirmed-stream restart-gap repair",
                "from_slot_inclusive": previous_slot,
                "to_slot_inclusive": startup_confirmed_slot,
                "created_at": Utc::now().to_rfc3339(),
            });
            write_atomic_json(&repair_directory.join("manifest.json"), &manifest)
                .await
                .map_err(carbon_error)?;

            let mut repair_raw_config = RawRecorderConfig::new(&repair_directory);
            repair_raw_config.segment_max_bytes = 256 * 1024 * 1024;
            repair_raw_config.sync_interval = Duration::from_secs(1);
            repair_raw_config.sync_every_messages = 10_000;
            let repair_datasource = YellowstoneGrpcGeyserClient::new(
                endpoint,
                Some(token),
                Some(CommitmentLevel::Processed),
                HashMap::new(),
                transaction_filters,
                Default::default(),
                YellowstoneGrpcClientConfig::default()
                    .with_raw_recorder(repair_raw_config)
                    .with_from_slot(Some(previous_slot))
                    .with_stop_at_slot(Some(startup_confirmed_slot)),
                None,
                Some(Duration::from_secs(30)),
            );
            pipeline_builder = pipeline_builder.datasource_with_id(
                repair_datasource,
                DatasourceId::new_named("yellowstone-processed-repair"),
            );
            log::info!(
                "repairing restart gap from slot {previous_slot} through {startup_confirmed_slot}"
            );
        }
    }

    log::info!(
        "capturing 24 program families from confirmed live head under {}",
        data_root.display()
    );

    let mut pipeline = pipeline_builder
        .datasource_cancellation_token(cancellation.clone())
        .channel_buffer_size(10_000)
        .metrics(Arc::new(LogMetrics::new()))
        .instruction_with_filters(PumpfunDecoder, processor("pumpfun"), confirmed_only())
        .instruction_with_filters(PumpSwapDecoder, processor("pump_swap"), confirmed_only())
        .instruction_with_filters(PumpFeesDecoder, processor("pump_fees"), confirmed_only())
        .instruction_with_filters(
            RaydiumAmmV4Decoder,
            processor("raydium_amm_v4"),
            confirmed_only(),
        )
        .instruction_with_filters(
            RaydiumClmmDecoder,
            processor("raydium_clmm"),
            confirmed_only(),
        )
        .instruction_with_filters(
            RaydiumCpmmDecoder,
            processor("raydium_cpmm"),
            confirmed_only(),
        )
        .instruction_with_filters(
            RaydiumLaunchpadDecoder,
            processor("raydium_launchpad"),
            confirmed_only(),
        )
        .instruction_with_filters(
            RaydiumStableSwapDecoder,
            processor("raydium_stable_swap"),
            confirmed_only(),
        )
        .instruction_with_filters(
            MeteoraDlmmDecoder,
            processor("meteora_dlmm"),
            confirmed_only(),
        )
        .instruction_with_filters(
            MeteoraDbcDecoder,
            processor("meteora_dbc"),
            confirmed_only(),
        )
        .instruction_with_filters(
            MeteoraDammV2Decoder,
            processor("meteora_damm_v2"),
            confirmed_only(),
        )
        .instruction_with_filters(
            MeteoraPoolsDecoder,
            processor("meteora_pools"),
            confirmed_only(),
        )
        .instruction_with_filters(
            MeteoraVaultDecoder,
            processor("meteora_vault"),
            confirmed_only(),
        )
        .instruction_with_filters(
            OrcaWhirlpoolDecoder,
            processor("orca_whirlpool"),
            confirmed_only(),
        )
        .instruction_with_filters(
            JupiterSwapDecoder,
            processor("jupiter_swap"),
            confirmed_only(),
        )
        .instruction_with_filters(MoonshotDecoder, processor("moonshot"), confirmed_only())
        .instruction_with_filters(BoopDecoder, processor("boop"), confirmed_only())
        .instruction_with_filters(BonkswapDecoder, processor("bonkswap"), confirmed_only())
        .instruction_with_filters(HeavenDecoder, processor("heaven"), confirmed_only())
        .instruction_with_filters(
            LifinityAmmV2Decoder,
            processor("lifinity_amm_v2"),
            confirmed_only(),
        )
        .instruction_with_filters(
            OpenbookV2Decoder,
            processor("openbook_v2"),
            confirmed_only(),
        )
        .instruction_with_filters(VertigoDecoder, processor("vertigo"), confirmed_only())
        .instruction_with_filters(VirtualsDecoder, processor("virtuals"), confirmed_only())
        .instruction_with_filters(
            MplTokenMetadataDecoder,
            processor("mpl_token_metadata"),
            confirmed_only(),
        )
        .build()?;

    let run_result = pipeline.run().await;
    sink.lock().await.finish().await.map_err(carbon_error)?;
    drop(pipeline);
    drop(confirmed_disconnect_sender);
    let _ = tokio::time::timeout(Duration::from_secs(2), confirmed_gap_writer).await;
    run_result
}

fn ensure_apfs_data_root(path: &Path) -> io::Result<()> {
    let mount = Path::new("/Volumes/CHAIN_DATA");
    if path.starts_with(mount) && !mount.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "/Volumes/CHAIN_DATA is not mounted; refusing to fall back to the system disk",
        ));
    }
    Ok(())
}

fn env_flag(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .map(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(default)
}

fn checkpoint_slot(raw_directory: &Path) -> Option<u64> {
    let content = std::fs::read_to_string(raw_directory.join("checkpoint.json")).ok()?;
    serde_json::from_str::<serde_json::Value>(&content)
        .ok()?
        .get("last_slot")?
        .as_u64()
}

async fn current_confirmed_slot(endpoint: &str, token: &str) -> CarbonResult<u64> {
    let builder = GeyserGrpcClient::build_from_shared(endpoint.to_string())
        .map_err(carbon_error)?
        .x_token(Some(token.to_string()))
        .map_err(carbon_error)?
        .tls_config(ClientTlsConfig::new().with_enabled_roots())
        .map_err(carbon_error)?;
    let mut client = builder.connect().await.map_err(carbon_error)?;
    let response = client
        .get_slot(Some(CommitmentLevel::Confirmed))
        .await
        .map_err(carbon_error)?;
    Ok(response.slot)
}

fn carbon_error(error: impl std::fmt::Display) -> CarbonError {
    CarbonError::Custom(error.to_string())
}

#[derive(Clone)]
struct JsonlInstructionProcessor {
    decoder: &'static str,
    sink: Arc<Mutex<SegmentedJsonlWriter>>,
    cancellation: CancellationToken,
}

impl JsonlInstructionProcessor {
    fn new(
        decoder: &'static str,
        sink: Arc<Mutex<SegmentedJsonlWriter>>,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            decoder,
            sink,
            cancellation,
        }
    }
}

impl<T> Processor<InstructionProcessorInputType<'_, T>> for JsonlInstructionProcessor
where
    T: Serialize + Send + Sync,
{
    async fn process(&mut self, input: &InstructionProcessorInputType<'_, T>) -> CarbonResult<()> {
        let transaction = &input.metadata.transaction_metadata;
        let record = json!({
            "schema_version": 1,
            "observed_at": Utc::now().to_rfc3339(),
            "commitment": "confirmed",
            "slot": transaction.slot,
            "signature": transaction.signature.to_string(),
            "transaction_index": transaction.index,
            "block_time": transaction.block_time,
            "fee_payer": transaction.fee_payer.to_string(),
            "success": transaction.meta.status.is_ok(),
            "fee_lamports": transaction.meta.fee,
            "decoder": self.decoder,
            "instruction_index": input.metadata.index,
            "stack_height": input.metadata.stack_height,
            "absolute_path": input.metadata.absolute_path,
            "decoded": input.decoded_instruction,
        });
        let bytes = serde_json::to_vec(&record).map_err(carbon_error)?;

        if let Err(error) = self
            .sink
            .lock()
            .await
            .write_record(&bytes, transaction.slot)
            .await
        {
            self.cancellation.cancel();
            return Err(carbon_error(format!(
                "decoded instruction persistence failed: {error}"
            )));
        }
        Ok(())
    }
}

struct SegmentedJsonlWriter {
    directory: PathBuf,
    segment_max_bytes: u64,
    writer: Option<BufWriter<File>>,
    partial_path: PathBuf,
    final_path: PathBuf,
    sequence: u64,
    bytes: u64,
    records: u64,
    process_records: u64,
    records_since_sync: u64,
    last_slot: Option<u64>,
    last_sync: Instant,
    compression_tasks: JoinSet<io::Result<()>>,
}

impl SegmentedJsonlWriter {
    async fn open(directory: PathBuf, segment_max_bytes: u64) -> io::Result<Self> {
        fs::create_dir_all(&directory).await?;
        let mut writer = Self {
            directory,
            segment_max_bytes,
            writer: None,
            partial_path: PathBuf::new(),
            final_path: PathBuf::new(),
            sequence: 0,
            bytes: 0,
            records: 0,
            process_records: 0,
            records_since_sync: 0,
            last_slot: None,
            last_sync: Instant::now(),
            compression_tasks: JoinSet::new(),
        };
        writer.open_segment().await?;
        Ok(writer)
    }

    async fn open_segment(&mut self) -> io::Result<()> {
        let epoch_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let stem = format!("instructions-{epoch_ms}-{:06}", self.sequence);
        self.sequence += 1;
        self.final_path = self.directory.join(format!("{stem}.jsonl"));
        self.partial_path = self.directory.join(format!("{stem}.jsonl.partial"));
        self.writer = Some(BufWriter::new(File::create(&self.partial_path).await?));
        self.bytes = 0;
        self.records = 0;
        self.records_since_sync = 0;
        self.last_sync = Instant::now();
        Ok(())
    }

    async fn write_record(&mut self, record: &[u8], slot: u64) -> io::Result<()> {
        let record_bytes = record.len() as u64 + 1;
        if self.bytes > 0 && self.bytes + record_bytes > self.segment_max_bytes {
            self.finalize_segment().await?;
            self.open_segment().await?;
        }
        let writer = self.writer.as_mut().expect("decoded segment must be open");
        writer.write_all(record).await?;
        writer.write_all(b"\n").await?;
        self.bytes += record_bytes;
        self.records += 1;
        self.process_records += 1;
        self.records_since_sync += 1;
        self.last_slot = Some(slot);
        if self.records_since_sync >= 10_000 || self.last_sync.elapsed() >= Duration::from_secs(1) {
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
        self.records_since_sync = 0;
        self.last_sync = Instant::now();
        self.reap_compression_tasks()?;
        Ok(())
    }

    async fn write_checkpoint(&self) -> io::Result<()> {
        let checkpoint = json!({
            "schema_version": 1,
            "current_segment": self.partial_path.file_name().and_then(|value| value.to_str()),
            "segment_bytes": self.bytes,
            "segment_records": self.records,
            "process_records": self.process_records,
            "last_slot": self.last_slot,
            "synced_at": Utc::now().to_rfc3339(),
        });
        write_atomic_json(&self.directory.join("checkpoint.json"), &checkpoint).await
    }

    async fn finalize_segment(&mut self) -> io::Result<()> {
        self.sync().await?;
        self.writer.take();
        if self.records > 0 {
            fs::rename(&self.partial_path, &self.final_path).await?;
            self.queue_compression(self.final_path.clone());
            self.partial_path = PathBuf::from(format!("{}.zst", self.final_path.display()));
            self.write_checkpoint().await?;
        } else if self.partial_path.exists() {
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

async fn write_atomic_json(path: &Path, value: &serde_json::Value) -> io::Result<()> {
    let temporary = path.with_extension("json.tmp");
    let mut file = File::create(&temporary).await?;
    let bytes = serde_json::to_vec_pretty(value).map_err(io::Error::other)?;
    file.write_all(&bytes).await?;
    file.write_all(b"\n").await?;
    file.sync_all().await?;
    drop(file);
    fs::rename(temporary, path).await
}

async fn write_gaps(
    mut receiver: mpsc::Receiver<carbon_core::datasource::DatasourceDisconnection>,
    path: PathBuf,
) {
    let Some(parent) = path.parent() else {
        return;
    };
    if let Err(error) = fs::create_dir_all(parent).await {
        log::error!("failed to create gap directory: {error}");
        return;
    }
    let file = match OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
    {
        Ok(file) => file,
        Err(error) => {
            log::error!("failed to open gap journal: {error}");
            return;
        }
    };
    let mut writer = BufWriter::new(file);
    while let Some(gap) = receiver.recv().await {
        let record = json!({
            "schema_version": 1,
            "source": gap.source,
            "disconnect_time": gap.disconnect_time.to_rfc3339(),
            "last_slot_before_disconnect": gap.last_slot_before_disconnect,
            "first_slot_after_reconnect": gap.first_slot_after_reconnect,
            "missed_slots": gap.missed_slots,
            "recorded_at": Utc::now().to_rfc3339(),
        });
        let bytes = match serde_json::to_vec(&record) {
            Ok(bytes) => bytes,
            Err(error) => {
                log::error!("failed to encode gap record: {error}");
                continue;
            }
        };
        if writer.write_all(&bytes).await.is_err()
            || writer.write_all(b"\n").await.is_err()
            || writer.flush().await.is_err()
            || writer.get_ref().sync_data().await.is_err()
        {
            log::error!("failed to persist gap record to {}", path.display());
            return;
        }
    }
}

// Keep the concrete instruction enums referenced so compiler errors point at
// a specific decoder when an upstream schema changes.
#[allow(dead_code)]
fn decoder_schema_types(
    _: PumpfunInstruction,
    _: PumpSwapInstruction,
    _: PumpFeesInstruction,
    _: RaydiumAmmV4Instruction,
    _: RaydiumClmmInstruction,
    _: RaydiumCpmmInstruction,
    _: RaydiumLaunchpadInstruction,
    _: RaydiumStableSwapInstruction,
    _: MeteoraDlmmInstruction,
    _: MeteoraDbcInstruction,
    _: MeteoraDammV2Instruction,
    _: MeteoraPoolsInstruction,
    _: MeteoraVaultInstruction,
    _: OrcaWhirlpoolInstruction,
    _: JupiterSwapInstruction,
    _: MoonshotInstruction,
    _: BoopInstruction,
    _: BonkswapInstruction,
    _: HeavenInstruction,
    _: LifinityAmmV2Instruction,
    _: OpenbookV2Instruction,
    _: VertigoInstruction,
    _: VirtualsInstruction,
    _: MplTokenMetadataInstruction,
) {
}
