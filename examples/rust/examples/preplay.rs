use std::{
    collections::{BTreeMap, HashMap},
    env, process,
    time::Duration,
};

use anyhow::Context;
use futures::{SinkExt, StreamExt};
use solana_pubkey::Pubkey;
use tonic::Status;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient, GeyserGrpcClientError};
use yellowstone_grpc_proto::geyser::{
    SubscribeDeshredRequest, SubscribeRequestFilterDeshredTransactions, SubscribeRequestPing,
    SubscribeUpdateDeshredTransactionInfo, subscribe_update_deshred::UpdateOneof,
};

const ENDPOINT: &str = "http://ams.rpc.sodae.io:10301";
const FATAL: [&str; 5] = [
    "UNAUTHENTICATED",
    "NOT_ENTITLED",
    "IP_NOT_ALLOWED",
    "QUOTA_EXCEEDED",
    "AUTH_RATE_LIMITED",
];

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = env::var("SODAE_PREPLAY_URL").unwrap_or_else(|_| ENDPOINT.to_string());
    let token = env::var("SODAE_TOKEN").context("set SODAE_TOKEN to your API token")?;
    let program = env::args()
        .nth(1)
        .map(|p| p.parse::<Pubkey>())
        .transpose()
        .context("the argument must be a program id")?;
    let request = SubscribeDeshredRequest {
        deshred_transactions: HashMap::from([(
            "preplay".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: Some(false),
                account_include: program.iter().map(Pubkey::to_string).collect(),
                ..Default::default()
            },
        )]),
        ..Default::default()
    };

    let mut delay = Duration::from_secs(1);
    loop {
        match subscribe(&endpoint, &token, request.clone(), program.is_some(), &mut delay).await {
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
    request: SubscribeDeshredRequest,
    detailed: bool,
    delay: &mut Duration,
) -> Result<(), Status> {
    let mut builder = GeyserGrpcClient::build_from_shared(endpoint.to_string())
        .map_err(unavailable)?
        .x_token(Some(token.to_string()))
        .map_err(unavailable)?
        .connect_timeout(Duration::from_secs(10))
        .max_decoding_message_size(64 * 1024 * 1024);
    if endpoint.starts_with("https://") {
        builder = builder
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .map_err(unavailable)?;
    }
    let mut client = builder.connect().await.map_err(unavailable)?;
    let (mut sink, mut updates) = client
        .subscribe_deshred_with_request(Some(request))
        .await
        .map_err(|e| match e {
            GeyserGrpcClientError::TonicStatus(status) => status,
            other => unavailable(other),
        })?;

    let mut per_slot: BTreeMap<u64, u64> = BTreeMap::new();
    while let Some(update) = updates.next().await {
        let update = update?;
        *delay = Duration::from_secs(1);
        match update.update_oneof {
            Some(UpdateOneof::Ping(_)) => {
                let ping = SubscribeDeshredRequest {
                    ping: Some(SubscribeRequestPing { id: 1 }),
                    ..Default::default()
                };
                sink.send(ping).await.map_err(unavailable)?;
            }
            Some(UpdateOneof::DeshredTransaction(update)) => {
                let Some(tx) = update.transaction else { continue };
                if detailed {
                    print_transaction(update.slot, &tx);
                } else {
                    *per_slot.entry(update.slot).or_default() += 1;
                    while per_slot.len() > 2 {
                        if let Some((slot, count)) = per_slot.pop_first() {
                            println!("{slot} transactions={count}");
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn print_transaction(slot: u64, tx: &SubscribeUpdateDeshredTransactionInfo) {
    let Some(message) = tx.transaction.as_ref().and_then(|t| t.message.as_ref()) else {
        return;
    };
    let key = |i: usize| {
        message
            .account_keys
            .get(i)
            .map(|k| bs58::encode(k).into_string())
            .unwrap_or_default()
    };
    let version = match (message.versioned, message.config.is_some()) {
        (false, _) => "legacy",
        (true, false) => "0",
        (true, true) => "1",
    };
    println!(
        "{slot} {} signer={} version={version} lookups={} loaded={}",
        bs58::encode(&tx.signature).into_string(),
        key(0),
        message.address_table_lookups.len(),
        tx.loaded_writable_addresses.len() + tx.loaded_readonly_addresses.len(),
    );
    for ix in &message.instructions {
        println!(
            "  {} accounts={} data={}B",
            key(ix.program_id_index as usize),
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
