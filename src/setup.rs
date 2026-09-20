use crate::{archive::Archive, config::Config};
use anyhow::{Result, ensure};
use std::{
    io::{IsTerminal, Write},
    path::{Path, PathBuf},
};
#[derive(clap::Args)]
pub struct Options {
    /// Use private token files without probing system secure storage.
    #[arg(long)]
    pub no_secure_storage: bool,
    /// Initialize defaults without prompts; configure credentials later.
    #[arg(long)]
    pub non_interactive: bool,
    #[arg(long)]
    pub skip_login: bool,
}
fn ask(label: &str, default: &str) -> Result<String> {
    eprint!("{label} [{default}]: ");
    std::io::stderr().flush()?;
    let mut line = String::new();
    ensure!(
        std::io::stdin().read_line(&mut line)? > 0,
        "setup cancelled (end of input)"
    );
    Ok(if line.trim().is_empty() {
        default.into()
    } else {
        line.trim().into()
    })
}
fn yes(label: &str, default: bool) -> Result<bool> {
    Ok(matches!(
        ask(label, if default { "yes" } else { "no" })?.as_str(),
        "y" | "yes"
    ))
}
pub async fn run(root: &Path, o: Options) -> Result<()> {
    ensure!(
        !root.join("catalog.sqlite3").exists(),
        "dataset exists; edit its config or use auth to resume login"
    );
    let mut c = Config::default();
    if !o.non_interactive {
        ensure!(
            std::io::stdin().is_terminal(),
            "use --non-interactive without a terminal"
        );
        eprintln!("tg-backup setup — Ctrl-C cancels before saving. Telegram cloud data only.");
        c.history_selector = ask("History selector", "true")?;
        c.attachment_selector = ask("Attachment selector", "true")?;
        c.max_epoch_bytes = ask("Maximum epoch bytes (0 disables)", "4294967296")?.parse()?;
        c.transcode.enabled = yes("Enable automatic media transcoding (keep originals)", false)?;
        if c.transcode.enabled {
            c.transcode.min_age_days = ask("Minimum Telegram content age in days", "0")?.parse()?;
        }
        c.resources.cpus = ask("Expensive work CPU budget", "2")?.parse()?;
        let window = ask("Automatic work window (all or HH:MM-HH:MM)", "all")?;
        if window != "all" {
            let (start, end) = window
                .split_once('-')
                .ok_or_else(|| anyhow::anyhow!("invalid window"))?;
            c.schedule.windows.push(crate::work::Window {
                days: (1..=7).collect(),
                start: start.into(),
                end: end.into(),
            });
        }
        if yes("Enable localhost Prometheus on port 9090", false)? {
            c.metrics_bind = Some("127.0.0.1:9090".into());
        }
        c.api_bind = ask("API bind address", "127.0.0.1:8080")?;
        eprintln!(
            "Dataset: {}\n{}",
            root.display(),
            toml::to_string_pretty(&c)?
        );
        ensure!(yes("Save this configuration", true)?, "setup cancelled");
    }
    c.resources.validate()?;
    c.schedule.validate()?;
    c.transcode.validate()?;
    crate::selector::Selector::parse(&c.history_selector)?;
    crate::selector::Selector::parse(&c.attachment_selector)?;
    let bind: std::net::SocketAddr = c.api_bind.parse()?;
    if !bind.ip().is_loopback() {
        ensure!(
            !o.non_interactive,
            "configure remote API auth after initialization"
        );
        let token = tg_backup_credentials::prompt_password("API bearer token: ")?;
        let secret = tg_backup_credentials::Secret::File {
            path: absolute(root)?.join("api.token"),
        };
        secret.store(&token).await?;
        c.api_token = Some(secret);
    }
    drop(Archive::init(root, &c)?);
    if !o.non_interactive && !o.skip_login {
        let id = ask("Telegram API ID (my.telegram.org)", "")?.parse()?;
        let hash = tg_backup_credentials::prompt_password("Telegram API hash: ")?;
        let secret = if o.no_secure_storage {
            tg_backup_credentials::Secret::File {
                path: absolute(root)?.join("api-hash.token"),
            }
        } else {
            #[cfg(target_os = "linux")]
            {
                tg_backup_credentials::Secret::SecretService {
                    account: format!("telegram:{}", absolute(root)?.display()),
                }
            }
            #[cfg(not(target_os = "linux"))]
            {
                tg_backup_credentials::Secret::File {
                    path: absolute(root)?.join("api-hash.token"),
                }
            }
        };
        secret.store(&hash).await?;
        let auth = crate::telegram::AuthConfig {
            api_id: id,
            api_hash: secret,
        };
        tg_backup_credentials::private_write(
            &root.join("auth.toml"),
            toml::to_string(&auth)?.as_bytes(),
        )?;
        crate::telegram::authenticate(root).await?;
    }
    eprintln!(
        "Saved {}. Start with tg-backup --dataset {} run",
        root.display(),
        root.display()
    );
    Ok(())
}
fn absolute(p: &Path) -> Result<PathBuf> {
    Ok(if p.is_absolute() {
        p.into()
    } else {
        std::env::current_dir()?.join(p)
    })
}
