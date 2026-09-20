mod config;
mod explorer;
use anyhow::{Result, ensure};
use clap::{Parser, Subcommand};
use std::{io::Write, path::PathBuf};
use tg_backup_protocol::{Format, Page, Query};
#[derive(Parser)]
#[command(version, about = "Read-only HTTP client for tg_backup")]
struct Cli {
    #[arg(long, global = true)]
    url: Option<String>,
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[arg(long, global = true, default_value = "default")]
    profile: String,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Interactively explore and export the remote archive.
    Tui,
    Query(Query),
    History {
        key: String,
        #[command(flatten)]
        query: Query,
    },
    Status(tg_backup_protocol::StatusOptions),
    Work {
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    Setup {
        #[arg(long)]
        no_secure_storage: bool,
        #[arg(long, default_value="secret-service", value_parser=["secret-service","tpm","file","environment"])]
        storage: String,
    },
    Credential {
        #[arg(value_parser=["set","remove","status"])]
        action: String,
    },
    Coverage,
    Jobs,
    Attachments,
    Attachment {
        hash: String,
        #[arg(long)]
        output: PathBuf,
    },
    Export {
        #[command(flatten)]
        query: Query,
        #[arg(long, value_enum, default_value = "ndjson")]
        format: Format,
        #[arg(long)]
        output: Option<PathBuf>,
        #[arg(long)]
        attachments: Option<PathBuf>,
        #[arg(long, value_enum, default_value = "original")]
        media: tg_backup_protocol::MediaSelection,
    },
}
#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config_path = cli.config.clone().map(Ok).unwrap_or_else(|| {
        tg_backup_credentials::config_directory().map(|p| p.join("client.toml"))
    })?;
    let mut config = config::Config::load(&config_path)?;
    if let Command::Setup {
        no_secure_storage,
        storage,
    } = &cli.command
    {
        let url = cli
            .url
            .clone()
            .or_else(|| std::env::var("TG_BACKUP_URL").ok())
            .unwrap_or_else(|| "http://127.0.0.1:8080".into());
        config::validate_url(&url)?;
        let parent = config_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("config path needs a directory"))?;
        let account = format!("{}:{}", cli.profile, url);
        let name = blake3::hash(account.as_bytes()).to_hex().to_string();
        use tg_backup_credentials::Secret;
        let token = match if *no_secure_storage {
            "file"
        } else {
            storage.as_str()
        } {
            "file" => Secret::File {
                path: parent.join(format!("{name}.token")),
            },
            "tpm" => Secret::Tpm {
                path: parent.join(format!("{name}.cred")),
            },
            "environment" => Secret::Environment {
                name: "TG_BACKUP_HTTP_TOKEN".into(),
            },
            _ => Secret::SecretService { account },
        };
        if !matches!(token, Secret::Environment { .. }) {
            let value = tg_backup_credentials::prompt_password("API bearer token (hidden): ")?;
            token.store(&value).await?;
        }
        config.profiles.insert(
            cli.profile.clone(),
            config::Profile {
                url,
                token: Some(token),
            },
        );
        config.save(&config_path)?;
        println!("Saved profile {} to {}", cli.profile, config_path.display());
        return Ok(());
    }
    let profile = config.profiles.get(&cli.profile);
    if let Command::Credential { action } = &cli.command {
        let secret = profile
            .and_then(|p| p.token.as_ref())
            .ok_or_else(|| anyhow::anyhow!("run setup for this profile first"))?;
        match action.as_str() {
            "set" => {
                secret
                    .store(&tg_backup_credentials::prompt_password(
                        "API bearer token (hidden): ",
                    )?)
                    .await?
            }
            "remove" => secret.remove().await?,
            _ => println!(
                "{}",
                if secret.load().await.is_ok() {
                    "available"
                } else {
                    "unavailable"
                }
            ),
        }
        return Ok(());
    }
    let url = cli
        .url
        .clone()
        .or_else(|| std::env::var("TG_BACKUP_URL").ok())
        .or_else(|| profile.map(|p| p.url.clone()))
        .unwrap_or_else(|| "http://127.0.0.1:8080".into());
    config::validate_url(&url)?;
    let url = url.trim_end_matches('/');
    let client = reqwest::Client::new();
    let token = match std::env::var("TG_BACKUP_HTTP_TOKEN").ok() {
        Some(t) => Some(t),
        None => match profile.and_then(|p| p.token.as_ref()) {
            Some(s) => Some(s.load().await?),
            None => None,
        },
    };
    let auth = |r: reqwest::RequestBuilder| {
        if let Some(token) = &token {
            r.bearer_auth(token)
        } else {
            r
        }
    };
    match cli.command {
        Command::Tui => {
            tg_backup_tui::run(std::sync::Arc::new(explorer::HttpBackend::new(
                url.into(),
                token.clone(),
            )?))
            .await?
        }
        Command::Setup { .. } | Command::Credential { .. } => unreachable!(),
        Command::Status(options) => loop {
            use std::io::IsTerminal;
            let value: serde_json::Value = auth(
                client
                    .get(format!("{url}/v2/status"))
                    .query(&[("details", options.details)]),
            )
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
            if options.json || !std::io::stdout().is_terminal() {
                println!("{}", serde_json::to_string(&value)?);
            } else {
                print!("{}", tg_backup_protocol::status_text(&value));
            }
            let Some(seconds) = options.watch else { break };
            tokio::select! { _ = tokio::time::sleep(std::time::Duration::from_secs(seconds)) => {}, _ = tokio::signal::ctrl_c() => break }
        },
        Command::Work { after, limit } => {
            let value: serde_json::Value = auth(
                client
                    .get(format!("{url}/v2/work"))
                    .query(&[("after", after.to_string()), ("limit", limit.to_string())]),
            )
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
            println!("{}", serde_json::to_string_pretty(&value)?);
        }
        Command::Jobs | Command::Coverage | Command::Attachments => {
            let path = match cli.command {
                Command::Jobs => "jobs",
                Command::Attachments => "attachments",
                _ => "coverage",
            };
            let value: serde_json::Value = auth(client.get(format!("{url}/v2/{path}")))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            println!("{}", serde_json::to_string_pretty(&value)?);
        }
        Command::Attachment { hash, output } => {
            download_attachment(&client, url, token.as_deref(), &hash, &output).await?;
        }
        Command::Query(q) => {
            let v: serde_json::Value = auth(client.post(format!("{url}/v2/query")).json(&q))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
        Command::History { key, mut query } => {
            query.key = Some(key);
            query.all_versions = true;
            let v: serde_json::Value = auth(client.post(format!("{url}/v2/query")).json(&query))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
        Command::Export {
            mut query,
            format,
            output,
            attachments,
            media,
        } => {
            let mut out: Box<dyn Write> = if let Some(path) = output {
                Box::new(std::io::BufWriter::new(std::fs::File::create(path)?))
            } else {
                Box::new(std::io::stdout().lock())
            };
            let mut writer = tg_backup_protocol::export::RecordWriter::new(&mut out, format)?;
            loop {
                let response = auth(client.post(format!("{url}/v2/query")).json(&query))
                    .send()
                    .await?;
                ensure!(
                    response.status().is_success(),
                    "API returned {}: {}",
                    response.status(),
                    response.text().await?
                );
                let page: Page = response.json().await?;
                for record in page.records {
                    writer.record(&record)?;
                    if let Some(directory) = &attachments {
                        std::fs::create_dir_all(directory)?;
                        for hash in record.media_hashes(media) {
                            download_attachment(
                                &client,
                                url,
                                token.as_deref(),
                                &hash,
                                &directory.join(&hash),
                            )
                            .await?;
                        }
                    }
                }
                if let Some(cursor) = page.next_cursor {
                    query.cursor = Some(cursor);
                } else {
                    break;
                }
            }
            writer.finish()?;
        }
    }
    Ok(())
}

async fn download_attachment(
    client: &reqwest::Client,
    url: &str,
    token: Option<&str>,
    hash: &str,
    output: &std::path::Path,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    // Validate before forming a URL or output staging name.
    ensure!(
        hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid attachment hash"
    );
    if output.exists() && file_hash(output)? == hash {
        return Ok(());
    }
    let mut request = client.get(format!("{url}/v2/attachments/{hash}"));
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let mut response = request.send().await?.error_for_status()?;
    let staging = output.with_extension(format!("{}.part", uuid::Uuid::new_v4()));
    let mut file = tokio::fs::File::create(&staging).await?;
    let mut digest = blake3::Hasher::new();
    while let Some(bytes) = response.chunk().await? {
        digest.update(&bytes);
        file.write_all(&bytes).await?;
    }
    file.sync_all().await?;
    drop(file);
    if digest.finalize().to_hex().as_str() != hash {
        tokio::fs::remove_file(&staging).await?;
        anyhow::bail!("attachment hash mismatch");
    }
    tokio::fs::rename(staging, output).await?;
    Ok(())
}

fn file_hash(path: &std::path::Path) -> Result<String> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut h = blake3::Hasher::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(h.finalize().to_hex().to_string())
}
