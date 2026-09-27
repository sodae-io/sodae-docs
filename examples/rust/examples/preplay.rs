use std::{env, process, time::Duration};

use anyhow::Context;
use sodae_examples::shredstream::{
    SubscribeEntriesRequest, shredstream_proxy_client::ShredstreamProxyClient,
};
use solana_hash::Hash;
use solana_pubkey::Pubkey;
use solana_transaction::versioned::{TransactionVersion, VersionedTransaction};
use tonic::{
    Request, Status,
    metadata::MetadataValue,
    transport::{ClientTlsConfig, Endpoint},
};
use wincode::{SchemaRead, containers, len::BincodeLen};

const ENDPOINT: &str = "http://ams.rpc.sodae.io:10301";
const FATAL: [&str; 5] = [
    "UNAUTHENTICATED",
    "NOT_ENTITLED",
    "IP_NOT_ALLOWED",
    "QUOTA_EXCEEDED",
    "AUTH_RATE_LIMITED",
];

#[derive(SchemaRead)]
struct Entry {
    _num_hashes: u64,
    _hash: Hash,
    #[wincode(with = "containers::Vec<VersionedTransaction, BincodeLen>")]
    transactions: Vec<VersionedTransaction>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = env::var("SODAE_PREPLAY_URL").unwrap_or_else(|_| ENDPOINT.to_string());
    let token = env::var("SODAE_TOKEN").context("set SODAE_TOKEN to your API token")?;
    let program = env::args()
        .nth(1)
        .map(|p| p.parse::<Pubkey>())
        .transpose()
        .context("the argument must be a program id")?;

    let mut delay = Duration::from_secs(1);
    loop {
        match subscribe(&endpoint, &token, program.as_ref(), &mut delay).await {
            Ok(()) => eprintln!("stream closed by the server"),
            Err(status) => {
                let code = error_code(&status);
                eprintln!("stream error {}: {}", code.unwrap_or("-"), status.message());
                if code.is_some_and(|c| FATAL.contains(&c)) {
                    process::exit(1);
                }
            }
        }
        eprintln!("reconnecting in {}s", delay.as_secs());
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}

async fn subscribe(
    endpoint: &str,
    token: &str,
    program: Option<&Pubkey>,
    delay: &mut Duration,
) -> Result<(), Status> {
    let mut channel = Endpoint::from_shared(endpoint.to_string())
        .map_err(unavailable)?
        .connect_timeout(Duration::from_secs(10))
        .tcp_nodelay(true);
    if endpoint.starts_with("https://") {
        channel = channel
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .map_err(unavailable)?;
    }
    let mut client = ShredstreamProxyClient::new(channel.connect().await.map_err(unavailable)?)
        .max_decoding_message_size(64 * 1024 * 1024);

    let mut request = Request::new(SubscribeEntriesRequest {});
    let token =
        MetadataValue::try_from(token).map_err(|_| Status::invalid_argument("bad token"))?;
    request.metadata_mut().insert("x-token", token);
    let mut stream = client.subscribe_entries(request).await?.into_inner();

    while let Some(message) = stream.message().await? {
        *delay = Duration::from_secs(1);
        let entries: Vec<Entry> = match wincode::deserialize(&message.entries) {
            Ok(entries) => entries,
            Err(error) => {
                eprintln!("slot {}: could not decode entries: {error}", message.slot);
                continue;
            }
        };
        let transactions = entries.iter().flat_map(|entry| &entry.transactions);
        match program {
            None => println!(
                "{} entries={} transactions={}",
                message.slot,
                entries.len(),
                transactions.count()
            ),
            Some(program) => {
                for tx in
                    transactions.filter(|tx| tx.message.static_account_keys().contains(program))
                {
                    print_transaction(message.slot, tx);
                }
            }
        }
    }
    Ok(())
}

fn print_transaction(slot: u64, tx: &VersionedTransaction) {
    let keys = tx.message.static_account_keys();
    let version = match tx.version() {
        TransactionVersion::Legacy(_) => "legacy".to_string(),
        TransactionVersion::Number(n) => n.to_string(),
    };
    println!(
        "{slot} {} signer={} version={version} lookups={}",
        tx.signatures[0],
        keys[0],
        tx.message.address_table_lookups().map_or(0, <[_]>::len),
    );
    for ix in tx.message.instructions() {
        println!(
            "  {} accounts={} data={}B",
            keys[usize::from(ix.program_id_index)],
            ix.accounts.len(),
            ix.data.len()
        );
    }
}

fn error_code(status: &Status) -> Option<&str> {
    status.metadata().get("x-error-code")?.to_str().ok()
}

fn unavailable(error: impl std::fmt::Display) -> Status {
    Status::unavailable(error.to_string())
}
