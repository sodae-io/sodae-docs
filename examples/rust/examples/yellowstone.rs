use std::{collections::HashMap, env, process, time::Duration};

use anyhow::{Context, bail};
use futures::{SinkExt, StreamExt};
use tonic::Status;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient, GeyserGrpcClientError};
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SlotStatus, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeRequestPing,
    subscribe_update::UpdateOneof,
};

const ENDPOINT: &str = "http://ams.rpc.sodae.io:10201";
const PUMP_AMM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const FATAL: [&str; 5] = [
    "UNAUTHENTICATED",
    "NOT_ENTITLED",
    "IP_NOT_ALLOWED",
    "QUOTA_EXCEEDED",
    "AUTH_RATE_LIMITED",
];

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = env::var("SODAE_YELLOWSTONE_URL").unwrap_or_else(|_| ENDPOINT.to_string());
    let token = env::var("SODAE_TOKEN").context("set SODAE_TOKEN to your API token")?;
    let args: Vec<String> = env::args().skip(1).collect();
    let request = build_request(&args)?;

    let mut delay = Duration::from_secs(1);
    loop {
        match subscribe(&endpoint, &token, request.clone(), &mut delay).await {
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

fn build_request(args: &[String]) -> anyhow::Result<SubscribeRequest> {
    let mode = args.first().map(String::as_str).unwrap_or("transactions");
    let targets = args.get(1..).unwrap_or_default();
    let mut request = SubscribeRequest {
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    };
    match mode {
        "transactions" => {
            let programs = if targets.is_empty() {
                vec![PUMP_AMM.to_string()]
            } else {
                targets.to_vec()
            };
            request.transactions = HashMap::from([(
                "transactions".to_string(),
                SubscribeRequestFilterTransactions {
                    vote: Some(false),
                    failed: Some(false),
                    account_include: programs,
                    ..Default::default()
                },
            )]);
        }
        "accounts" => {
            if targets.is_empty() {
                bail!("usage: yellowstone accounts <pubkey>...");
            }
            request.accounts = HashMap::from([(
                "accounts".to_string(),
                SubscribeRequestFilterAccounts {
                    account: targets.to_vec(),
                    ..Default::default()
                },
            )]);
        }
        "slots" => {
            request.slots = HashMap::from([(
                "slots".to_string(),
                SubscribeRequestFilterSlots {
                    filter_by_commitment: Some(false),
                    ..Default::default()
                },
            )]);
        }
        other => bail!("unknown mode {other}; use transactions, accounts or slots"),
    }
    Ok(request)
}

async fn subscribe(
    endpoint: &str,
    token: &str,
    request: SubscribeRequest,
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
    let (mut sink, mut updates) =
        client
            .subscribe_with_request(Some(request))
            .await
            .map_err(|e| match e {
                GeyserGrpcClientError::TonicStatus(status) => status,
                other => unavailable(other),
            })?;

    while let Some(update) = updates.next().await {
        let update = update?;
        *delay = Duration::from_secs(1);
        match update.update_oneof {
            Some(UpdateOneof::Ping(_)) => {
                let ping = SubscribeRequest {
                    ping: Some(SubscribeRequestPing { id: 1 }),
                    ..Default::default()
                };
                sink.send(ping).await.map_err(unavailable)?;
            }
            Some(UpdateOneof::Transaction(update)) => {
                if let Some(tx) = update.transaction {
                    println!(
                        "{} {}",
                        update.slot,
                        bs58::encode(tx.signature).into_string()
                    );
                }
            }
            Some(UpdateOneof::Account(update)) => {
                if let Some(account) = update.account {
                    println!(
                        "{} {} lamports={} data={}B owner={}",
                        update.slot,
                        bs58::encode(account.pubkey).into_string(),
                        account.lamports,
                        account.data.len(),
                        bs58::encode(account.owner).into_string(),
                    );
                }
            }
            Some(UpdateOneof::Slot(update)) => {
                let status = SlotStatus::try_from(update.status)
                    .map(|s| s.as_str_name())
                    .unwrap_or("UNKNOWN");
                println!("{} {}", update.slot, status);
            }
            _ => {}
        }
    }
    Ok(())
}

fn error_code(status: &Status) -> Option<&str> {
    status.metadata().get("x-error-code")?.to_str().ok()
}

fn unavailable(error: impl std::fmt::Display) -> Status {
    Status::unavailable(error.to_string())
}
