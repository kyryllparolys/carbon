# Yellowstone live capture and SQD replay

`nolimit-live-carbon-example` uses the same 24 Carbon program decoders for two
sources:

- `nolimit-live-carbon-example` consumes the NoLimitNodes Yellowstone stream.
- `sqd-carbon-decode` consumes bounded SQD Solana block JSONL exported by the
  sibling `solana-chain-indexer` checkout.

The SQD exporter retains matched launchpad, DEX, router, and metadata
instructions together with their transaction metadata, native and token
balance changes, logs, and failures. Sibling and inner instructions outside the
configured programs are omitted by default because Carbon does not decode
them. Set `SQD_INCLUDE_TRANSACTION_INSTRUCTIONS=true` or
`SQD_INCLUDE_INNER_INSTRUCTIONS=true` when a diagnostic needs those extra
records.

## Bounded replay

```bash
cd ../solana-chain-indexer
node -r ts-node/register scripts/export-sqd-carbon.ts \
  /path/to/raw.jsonl FROM_SLOT TO_SLOT

cd ../carbon
cargo run --release -p nolimit-live-carbon-example \
  --bin sqd-carbon-decode -- \
  /path/to/raw.jsonl /path/to/decoded.jsonl
```

The decoder also reads `.jsonl.zst` input. Both stages use a `.partial` output
and publish the requested final path only after a complete, flushed result.
They refuse to overwrite an existing output or partial file.

SQD does not attach signer and writable flags to each instruction account in
this targeted representation. The decoders do not need those flags to parse
the instruction, but serialized `remaining` accounts contain placeholder
`false` flags. SQD output marks this explicitly with
`account_meta_complete: false`; named accounts, instruction variants, and
decoded instruction data are unaffected.

Deduplicate or reconcile sources by `(slot, signature, absolute_path,
decoder)`. Do not use `transaction_index` as a cross-provider identity because
the tested SQD and Yellowstone feeds sometimes assign different indices to the
same transaction signature.
