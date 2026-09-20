use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tg_backup::{
    archive::{Archive, Maintenance, Retention},
    config::{Config, EpochPeriod},
    export::Format,
    query::Query,
};

#[derive(Parser)]
#[command(
    version,
    about = "Archive Telegram cloud data as native TL in compressed SQLite epochs"
)]
struct Cli {
    #[arg(long, global = true, default_value = "dataset")]
    dataset: PathBuf,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Interactively explore and export the archive (read-only).
    Tui,
    /// Export an offline interactive social graph (read-only).
    Graph(tg_backup::graph::Options),
    Init {
        #[arg(long, value_enum, default_value = "monthly")]
        epoch: EpochPeriod,
    },
    Setup(tg_backup::setup::Options),
    Run,
    Worker {
        #[arg(long)]
        once: bool,
    },
    #[command(hide = true)]
    WorkerTask,
    WorkerService {
        #[arg(long)]
        socket: PathBuf,
    },
    Work {
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long, default_value_t = 50)]
        limit: usize,
        #[arg(long)]
        resume: Option<i64>,
        #[arg(long,value_parser=["verify","reindex","seal","repack","consolidate"])]
        enqueue: Option<String>,
    },
    Transcode {
        #[arg(long)]
        apply: bool,
        #[arg(long)]
        policy: Option<PathBuf>,
        #[arg(long)]
        automatic: bool,
    },
    Metrics {
        #[arg(long, default_value = "127.0.0.1:9090")]
        bind: std::net::SocketAddr,
    },
    Auth,
    Sync(tg_backup::telegram::SyncOptions),
    Jobs,
    AbortTakeout {
        job: String,
    },
    Serve {
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: std::net::SocketAddr,
    },
    Query(Query),
    History {
        key: String,
        #[command(flatten)]
        query: Query,
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
    Status(tg_backup_protocol::StatusOptions),
    Coverage,
    Maintenance {
        #[command(subcommand)]
        command: MaintenanceCommand,
    },
}
#[derive(Subcommand)]
enum MaintenanceCommand {
    Verify,
    Reindex,
    Seal {
        #[arg(long)]
        apply: bool,
    },
    Compact {
        #[arg(long)]
        apply: bool,
        #[arg(long)]
        policy: Option<PathBuf>,
    },
    Reorganize {
        #[arg(long)]
        apply: bool,
    },
}
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    match cli.command {
        Command::Graph(options) => tg_backup::graph::run(&cli.dataset, &options)?,
        Command::Tui => {
            tg_backup_tui::run(std::sync::Arc::new(tg_backup::explorer::LocalBackend {
                root: cli.dataset,
            }))
            .await?
        }
        Command::Init { epoch } => {
            let a = Archive::init(
                &cli.dataset,
                &Config {
                    epoch,
                    ..Default::default()
                },
            )?;
            print(&a.status()?)?;
        }
        Command::Setup(options) => tg_backup::setup::run(&cli.dataset, options).await?,
        Command::WorkerService { socket } => tg_backup::work::service(cli.dataset, socket).await?,
        Command::WorkerTask => tg_backup::transcode::worker(&cli.dataset)?,
        Command::Worker { once } => {
            tg_backup::work::run(
                std::sync::Arc::new(std::sync::Mutex::new(Archive::open(&cli.dataset, true)?)),
                once,
            )
            .await?
        }
        Command::Transcode {
            apply,
            policy,
            automatic,
        } => {
            let a = Archive::open(&cli.dataset, apply)?;
            let policy = policy
                .map(|p| -> Result<tg_backup::transcode::Policy> {
                    Ok(toml::from_str(&std::fs::read_to_string(p)?)?)
                })
                .transpose()?
                .unwrap_or_else(|| a.config.transcode.clone());
            print(&tg_backup::transcode::enqueue(
                &a, &policy, apply, automatic,
            )?)?;
        }
        Command::Work {
            after,
            limit,
            resume,
            enqueue,
        } => {
            let a = Archive::open(&cli.dataset, resume.is_some() || enqueue.is_some())?;
            if let Some(id) = resume {
                a.resume_work(id)?;
            }
            if let Some(kind) = enqueue {
                a.enqueue_work(
                    &kind,
                    &uuid::Uuid::new_v4().to_string(),
                    &serde_json::json!({}),
                    false,
                )?;
            }
            print(&a.work_page(after, limit)?)?;
        }
        Command::Metrics { bind } => {
            let c = Config::load(&cli.dataset)?;
            tg_backup::metrics::serve(
                cli.dataset,
                bind,
                secret(c.metrics_token, "TG_BACKUP_METRICS_TOKEN").await?,
            )
            .await?;
        }
        Command::Run => {
            let c = Config::load(&cli.dataset)?;
            let api_token = secret(c.api_token, "TG_BACKUP_HTTP_TOKEN").await?;
            let mut api = tokio::spawn(tg_backup::http::serve(
                cli.dataset.clone(),
                c.api_bind.parse()?,
                api_token,
            ));
            let mut metrics = if let Some(bind) = c.metrics_bind {
                Some(tokio::spawn(tg_backup::metrics::serve(
                    cli.dataset.clone(),
                    bind.parse()?,
                    secret(c.metrics_token, "TG_BACKUP_METRICS_TOKEN").await?,
                )))
            } else {
                None
            };
            let sync = tg_backup::telegram::sync(
                &cli.dataset,
                tg_backup::telegram::SyncOptions {
                    continuous: true,
                    ..Default::default()
                },
            );
            tokio::pin!(sync);
            let result = tokio::select! {
                result=&mut sync=>result,
                result=&mut api=>result.map_err(anyhow::Error::from).and_then(|v|v),
                result=async {
                    match metrics.as_mut() {
                        Some(task) => task.await,
                        None => std::future::pending().await,
                    }
                }=>result.map_err(anyhow::Error::from).and_then(|v|v),
            };
            api.abort();
            if let Some(task) = metrics {
                task.abort();
            }
            result?;
        }
        Command::Auth => tg_backup::telegram::authenticate(&cli.dataset).await?,
        Command::Sync(options) => tg_backup::telegram::sync(&cli.dataset, options).await?,
        Command::AbortTakeout { job } => {
            tg_backup::telegram::abort_takeout(&cli.dataset, &job).await?
        }
        Command::Serve { bind } => {
            tg_backup::http::serve(
                cli.dataset.clone(),
                bind,
                secret(
                    Config::load(&cli.dataset)?.api_token,
                    "TG_BACKUP_HTTP_TOKEN",
                )
                .await?,
            )
            .await?
        }
        Command::Query(query) => {
            let a = Archive::open(&cli.dataset, false)?;
            let mut value = serde_json::to_value(a.query(&query)?)?;
            tg_backup::query::public_json(&mut value);
            print(&value)?;
        }
        Command::History { key, mut query } => {
            query.key = Some(key);
            query.all_versions = true;
            let a = Archive::open(&cli.dataset, false)?;
            let mut value = serde_json::to_value(a.query(&query)?)?;
            tg_backup::query::public_json(&mut value);
            print(&value)?;
        }
        Command::Export {
            query,
            format,
            output,
            attachments,
            media,
        } => {
            let a = Archive::open(&cli.dataset, false)?;
            let out: Box<dyn std::io::Write> = if let Some(path) = output {
                Box::new(std::io::BufWriter::new(std::fs::File::create(path)?))
            } else {
                Box::new(std::io::stdout().lock())
            };
            tg_backup::export::export_media(
                &a,
                &query,
                format,
                out,
                attachments.as_deref(),
                media,
            )?;
        }
        Command::Jobs => print(&Archive::open(&cli.dataset, false)?.list_table("jobs")?)?,
        Command::Coverage => print(&Archive::open(&cli.dataset, false)?.list_table("coverage")?)?,
        Command::Status(options) => loop {
            use std::io::IsTerminal;
            let value = Archive::open(&cli.dataset, false)?.operational_status(options.details)?;
            if options.json || !std::io::stdout().is_terminal() {
                println!("{}", serde_json::to_string(&value)?);
            } else {
                print!("{}", tg_backup_protocol::status_text(&value));
            }
            let Some(seconds) = options.watch else { break };
            tokio::select! {_=tokio::time::sleep(std::time::Duration::from_secs(seconds))=>{},_=tokio::signal::ctrl_c()=>break}
        },
        Command::Maintenance { command } => {
            let mut a = Archive::open(&cli.dataset, true)?;
            match command {
                MaintenanceCommand::Verify => {
                    print(&tg_backup::work::manual(&mut a, "verify").await?)?
                }
                MaintenanceCommand::Reindex => {
                    print(&tg_backup::work::manual(&mut a, "reindex").await?)?;
                }
                MaintenanceCommand::Seal { apply } => {
                    if apply {
                        print(&tg_backup::work::manual(&mut a, "seal").await?)?;
                    } else {
                        print(&a.maintain(&Maintenance {
                            seal: true,
                            ..Default::default()
                        })?)?;
                    }
                }
                MaintenanceCommand::Compact { apply, policy } => {
                    let retention = policy
                        .map(|p| -> Result<Retention> {
                            Ok(toml::from_str(&std::fs::read_to_string(p)?)?)
                        })
                        .transpose()?
                        .unwrap_or_default();
                    if apply {
                        if retention.lossy() {
                            drop(a);
                            print(&tg_backup::work::compact(&cli.dataset, retention).await?)?;
                        } else {
                            print(&tg_backup::work::manual(&mut a, "repack").await?)?;
                        }
                    } else {
                        print(&a.maintain(&Maintenance {
                            retention,
                            ..Default::default()
                        })?)?;
                    }
                }
                MaintenanceCommand::Reorganize { apply } => {
                    if apply {
                        print(&tg_backup::work::manual(&mut a, "consolidate").await?)?;
                    } else {
                        print(&a.maintain(&Maintenance {
                            consolidate_yearly: true,
                            seal: true,
                            ..Default::default()
                        })?)?;
                    }
                }
            }
        }
    }
    Ok(())
}
fn print(value: &serde_json::Value) -> Result<()> {
    serde_json::to_writer_pretty(std::io::stdout().lock(), value)?;
    println!();
    Ok(())
}

async fn secret(
    config: Option<tg_backup_credentials::Secret>,
    name: &str,
) -> Result<Option<String>> {
    if let Ok(token) = std::env::var(name) {
        return Ok(Some(token));
    }
    if let Ok(path) = std::env::var(format!("{name}_FILE")) {
        return Ok(Some(
            tg_backup_credentials::Secret::File { path: path.into() }
                .load()
                .await?,
        ));
    }
    match config {
        Some(s) => Ok(Some(s.load().await?)),
        None => Ok(None),
    }
}
