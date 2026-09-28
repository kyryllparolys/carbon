use {
    carbon_bonkswap_decoder::{BonkswapDecoder, PROGRAM_ID as BONKSWAP_ID},
    carbon_boop_decoder::{BoopDecoder, PROGRAM_ID as BOOP_ID},
    carbon_core::instruction::InstructionDecoder,
    carbon_heaven_decoder::{HeavenDecoder, PROGRAM_ID as HEAVEN_ID},
    carbon_jupiter_swap_decoder::{JupiterSwapDecoder, PROGRAM_ID as JUPITER_ID},
    carbon_lifinity_amm_v2_decoder::{LifinityAmmV2Decoder, PROGRAM_ID as LIFINITY_ID},
    carbon_meteora_damm_v2_decoder::{MeteoraDammV2Decoder, PROGRAM_ID as METEORA_DAMM_V2_ID},
    carbon_meteora_dbc_decoder::{MeteoraDbcDecoder, PROGRAM_ID as METEORA_DBC_ID},
    carbon_meteora_dlmm_decoder::{MeteoraDlmmDecoder, PROGRAM_ID as METEORA_DLMM_ID},
    carbon_meteora_pools_decoder::{MeteoraPoolsDecoder, PROGRAM_ID as METEORA_POOLS_ID},
    carbon_meteora_vault_decoder::{MeteoraVaultDecoder, PROGRAM_ID as METEORA_VAULT_ID},
    carbon_moonshot_decoder::{MoonshotDecoder, PROGRAM_ID as MOONSHOT_ID},
    carbon_mpl_token_metadata_decoder::{MplTokenMetadataDecoder, PROGRAM_ID as MPL_METADATA_ID},
    carbon_openbook_v2_decoder::{OpenbookV2Decoder, PROGRAM_ID as OPENBOOK_V2_ID},
    carbon_orca_whirlpool_decoder::{OrcaWhirlpoolDecoder, PROGRAM_ID as ORCA_WHIRLPOOL_ID},
    carbon_pump_fees_decoder::{PumpFeesDecoder, PROGRAM_ID as PUMP_FEES_ID},
    carbon_pump_swap_decoder::{PumpSwapDecoder, PROGRAM_ID as PUMP_SWAP_ID},
    carbon_pumpfun_decoder::{PumpfunDecoder, PROGRAM_ID as PUMPFUN_ID},
    carbon_raydium_amm_v4_decoder::{RaydiumAmmV4Decoder, PROGRAM_ID as RAYDIUM_AMM_V4_ID},
    carbon_raydium_clmm_decoder::{RaydiumClmmDecoder, PROGRAM_ID as RAYDIUM_CLMM_ID},
    carbon_raydium_cpmm_decoder::{RaydiumCpmmDecoder, PROGRAM_ID as RAYDIUM_CPMM_ID},
    carbon_raydium_launchpad_decoder::{
        RaydiumLaunchpadDecoder, PROGRAM_ID as RAYDIUM_LAUNCHPAD_ID,
    },
    carbon_raydium_stable_swap_decoder::{
        RaydiumStableSwapDecoder, PROGRAM_ID as RAYDIUM_STABLE_ID,
    },
    carbon_vertigo_decoder::{VertigoDecoder, PROGRAM_ID as VERTIGO_ID},
    carbon_virtuals_decoder::{VirtualsDecoder, PROGRAM_ID as VIRTUALS_ID},
    chrono::Utc,
    serde::Serialize,
    serde_json::{json, Value},
    solana_instruction::{AccountMeta, Instruction},
    solana_pubkey::Pubkey,
    std::{
        collections::{BTreeMap, HashMap},
        env,
        ffi::OsString,
        fs::{self, File, OpenOptions},
        io::{BufRead, BufReader, BufWriter, Write},
        path::{Path, PathBuf},
        str::FromStr,
    },
};

fn decode_value<'a, D, T>(decoder: &D, instruction: &'a Instruction) -> Option<Value>
where
    D: InstructionDecoder<'a, InstructionType = T>,
    T: Serialize,
{
    decoder
        .decode_instruction(instruction)
        .and_then(|decoded| serde_json::to_value(decoded).ok())
}

fn decode_instruction(
    program_id: Pubkey,
    instruction: &Instruction,
) -> Option<(&'static str, Value)> {
    macro_rules! decode {
        ($id:expr, $name:literal, $decoder:expr) => {
            if program_id == $id {
                return decode_value(&$decoder, instruction).map(|value| ($name, value));
            }
        };
    }
    decode!(PUMPFUN_ID, "pumpfun", PumpfunDecoder);
    decode!(PUMP_SWAP_ID, "pump_swap", PumpSwapDecoder);
    decode!(PUMP_FEES_ID, "pump_fees", PumpFeesDecoder);
    decode!(RAYDIUM_AMM_V4_ID, "raydium_amm_v4", RaydiumAmmV4Decoder);
    decode!(RAYDIUM_CLMM_ID, "raydium_clmm", RaydiumClmmDecoder);
    decode!(RAYDIUM_CPMM_ID, "raydium_cpmm", RaydiumCpmmDecoder);
    decode!(
        RAYDIUM_LAUNCHPAD_ID,
        "raydium_launchpad",
        RaydiumLaunchpadDecoder
    );
    decode!(
        RAYDIUM_STABLE_ID,
        "raydium_stable_swap",
        RaydiumStableSwapDecoder
    );
    decode!(METEORA_DLMM_ID, "meteora_dlmm", MeteoraDlmmDecoder);
    decode!(METEORA_DBC_ID, "meteora_dbc", MeteoraDbcDecoder);
    decode!(METEORA_DAMM_V2_ID, "meteora_damm_v2", MeteoraDammV2Decoder);
    decode!(METEORA_POOLS_ID, "meteora_pools", MeteoraPoolsDecoder);
    decode!(METEORA_VAULT_ID, "meteora_vault", MeteoraVaultDecoder);
    decode!(ORCA_WHIRLPOOL_ID, "orca_whirlpool", OrcaWhirlpoolDecoder);
    decode!(JUPITER_ID, "jupiter_swap", JupiterSwapDecoder);
    decode!(MOONSHOT_ID, "moonshot", MoonshotDecoder);
    decode!(BOOP_ID, "boop", BoopDecoder);
    decode!(BONKSWAP_ID, "bonkswap", BonkswapDecoder);
    decode!(HEAVEN_ID, "heaven", HeavenDecoder);
    decode!(LIFINITY_ID, "lifinity_amm_v2", LifinityAmmV2Decoder);
    decode!(OPENBOOK_V2_ID, "openbook_v2", OpenbookV2Decoder);
    decode!(VERTIGO_ID, "vertigo", VertigoDecoder);
    decode!(VIRTUALS_ID, "virtuals", VirtualsDecoder);
    decode!(
        MPL_METADATA_ID,
        "mpl_token_metadata",
        MplTokenMetadataDecoder
    );
    None
}

fn u64_value(value: Option<&Value>) -> Option<u64> {
    value.and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
}

fn input_reader(path: &Path) -> Result<Box<dyn BufRead>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    if path.extension().is_some_and(|extension| extension == "zst") {
        let decoder = zstd::stream::read::Decoder::new(file)?;
        return Ok(Box::new(BufReader::new(decoder)));
    }
    Ok(Box::new(BufReader::new(file)))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input_path = PathBuf::from(
        env::args()
            .nth(1)
            .ok_or("usage: sqd-carbon-decode INPUT[.zst] OUTPUT")?,
    );
    let output_path = PathBuf::from(
        env::args()
            .nth(2)
            .ok_or("usage: sqd-carbon-decode INPUT[.zst] OUTPUT")?,
    );
    if output_path.exists() {
        return Err(format!("refusing to overwrite {}", output_path.display()).into());
    }
    let mut partial_name = OsString::from(output_path.as_os_str());
    partial_name.push(".partial");
    let partial_path = PathBuf::from(partial_name);
    let input = input_reader(&input_path)?;
    let output_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&partial_path)?;
    let mut output = BufWriter::new(output_file);
    let mut blocks = 0_u64;
    let mut instructions = 0_u64;
    let mut decoded = 0_u64;
    let mut by_decoder = BTreeMap::<String, u64>::new();

    for line in input.lines() {
        let block: Value = serde_json::from_str(&line?)?;
        let header = block.get("header").ok_or("SQD block missing header")?;
        let slot = u64_value(header.get("number")).ok_or("SQD block missing slot")?;
        let block_time = header.get("timestamp").and_then(Value::as_i64);
        let block_hash = header.get("hash").and_then(Value::as_str);
        let transactions = block
            .get("transactions")
            .and_then(Value::as_array)
            .ok_or("SQD block missing transactions")?;
        let transaction_by_index = transactions
            .iter()
            .filter_map(|tx| Some((u64_value(tx.get("transactionIndex"))?, tx)))
            .collect::<HashMap<_, _>>();

        for source_instruction in block
            .get("instructions")
            .and_then(Value::as_array)
            .ok_or("SQD block missing instructions")?
        {
            instructions += 1;
            let program_id = Pubkey::from_str(
                source_instruction
                    .get("programId")
                    .and_then(Value::as_str)
                    .ok_or("SQD instruction missing programId")?,
            )?;
            let accounts = source_instruction
                .get("accounts")
                .and_then(Value::as_array)
                .ok_or("SQD instruction missing accounts")?
                .iter()
                .map(|account| {
                    Pubkey::from_str(account.as_str().ok_or("invalid SQD instruction account")?)
                        .map(|pubkey| AccountMeta::new_readonly(pubkey, false))
                        .map_err(Into::into)
                })
                .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
            let data = bs58::decode(
                source_instruction
                    .get("data")
                    .and_then(Value::as_str)
                    .ok_or("SQD instruction missing data")?,
            )
            .into_vec()?;
            let instruction = Instruction {
                program_id,
                accounts,
                data,
            };
            let Some((decoder, decoded_instruction)) = decode_instruction(program_id, &instruction)
            else {
                continue;
            };

            let transaction_index = u64_value(source_instruction.get("transactionIndex"));
            let transaction =
                transaction_index.and_then(|index| transaction_by_index.get(&index).copied());
            let signature = transaction
                .and_then(|tx| tx.get("signatures"))
                .and_then(Value::as_array)
                .and_then(|values| values.first())
                .and_then(Value::as_str);
            let fee_payer = transaction
                .and_then(|tx| tx.get("feePayer"))
                .and_then(Value::as_str)
                .or_else(|| {
                    transaction
                        .and_then(|tx| tx.get("accountKeys"))
                        .and_then(Value::as_array)
                        .and_then(|values| values.first())
                        .and_then(Value::as_str)
                });
            let success = transaction
                .and_then(|tx| tx.get("err"))
                .is_some_and(Value::is_null);
            let fee = transaction.and_then(|tx| u64_value(tx.get("fee")));
            let absolute_path = source_instruction
                .get("instructionAddress")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let instruction_index = absolute_path.last().and_then(Value::as_u64);
            let stack_height = absolute_path.len();
            let record = json!({
                "schema_version": 1,
                "source": "sqd",
                "observed_at": Utc::now().to_rfc3339(),
                "commitment": "finalized",
                "slot": slot,
                "signature": signature,
                "transaction_index": transaction_index,
                "block_time": block_time,
                "block_hash": block_hash,
                "fee_payer": fee_payer,
                "success": success,
                "fee_lamports": fee,
                "account_meta_complete": false,
                "decoder": decoder,
                "instruction_index": instruction_index,
                "stack_height": stack_height,
                "absolute_path": absolute_path,
                "decoded": decoded_instruction,
            });
            serde_json::to_writer(&mut output, &record)?;
            output.write_all(b"\n")?;
            decoded += 1;
            *by_decoder.entry(decoder.to_string()).or_default() += 1;
        }
        blocks += 1;
    }
    output.flush()?;
    output.get_ref().sync_all()?;
    drop(output);
    fs::rename(&partial_path, &output_path)?;
    eprintln!(
        "{}",
        serde_json::to_string(&json!({
            "source": "sqd",
            "blocks": blocks,
            "instructions_seen": instructions,
            "decoded_instructions": decoded,
            "by_decoder": by_decoder,
            "output": output_path,
        }))?
    );
    Ok(())
}
