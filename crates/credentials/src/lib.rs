//! Secrets are referenced in ordinary configuration, never embedded in it.
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    io::Write,
    path::{Path, PathBuf},
};
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)]
pub enum Secret {
    Environment { name: String },
    File { path: PathBuf },
    SecretService { account: String },
    Tpm { path: PathBuf },
}
pub use rpassword::prompt_password;
pub fn config_directory() -> Result<PathBuf> {
    Ok(std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|p| PathBuf::from(p).join(".config")))
        .context("set XDG_CONFIG_HOME or HOME")?
        .join("tg-backup"))
}
/// Publish a private file atomically, including on first creation.
pub fn private_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    let tmp = parent.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
    let mut opts = std::fs::OpenOptions::new();
    opts.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let result = (|| -> Result<()> {
        let mut f = opts.open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)?;
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(tmp);
    }
    result
}
async fn tpm(operation: &str, input: &[u8]) -> Result<Vec<u8>> {
    let mut command = tokio::process::Command::new("systemd-creds");
    command.args(["--user", "--name=tg-backup-token"]);
    if operation == "encrypt" {
        command.arg("--with-key=tpm2");
    }
    let mut child = command
        .args([operation, "-", "-"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("TPM storage requires systemd-creds >=256 and an accessible TPM2")?;
    child
        .stdin
        .take()
        .context("credential stdin")?
        .write_all(input)
        .await?;
    let output = child.wait_with_output().await?;
    ensure!(
        output.status.success(),
        "TPM operation failed; no insecure fallback was used"
    );
    Ok(output.stdout)
}
impl Secret {
    pub async fn load(&self) -> Result<String> {
        let bytes = match self {
            Self::Environment { name } => std::env::var(name)?.into_bytes(),
            Self::File { path } => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    ensure!(
                        std::fs::metadata(path)?.permissions().mode() & 0o077 == 0,
                        "token file must have mode 0600 or stricter"
                    );
                }
                std::fs::read(path)?
            }
            Self::Tpm { path } => tpm("decrypt", &std::fs::read(path)?).await?,
            Self::SecretService { account } => service_get(account).await?,
        };
        let value = String::from_utf8(bytes)?
            .trim_end_matches(['\r', '\n'])
            .to_owned();
        ensure!(!value.is_empty(), "empty credential");
        Ok(value)
    }
    pub async fn store(&self, value: &str) -> Result<()> {
        ensure!(!value.is_empty(), "empty credential");
        match self {
            Self::Environment { .. } => {
                anyhow::bail!("set the environment variable outside this process")
            }
            Self::File { path } => private_write(path, value.as_bytes()),
            Self::Tpm { path } => private_write(path, &tpm("encrypt", value.as_bytes()).await?),
            Self::SecretService { account } => service_set(account, Some(value)).await,
        }
    }
    pub async fn remove(&self) -> Result<()> {
        match self {
            Self::Environment { .. } => {
                anyhow::bail!("unset the environment variable outside this process")
            }
            Self::File { path } | Self::Tpm { path } => {
                std::fs::remove_file(path)?;
                Ok(())
            }
            Self::SecretService { account } => service_set(account, None).await,
        }
    }
}
#[cfg(target_os = "linux")]
async fn service_get(account: &str) -> Result<Vec<u8>> {
    use secret_service::{EncryptionType, SecretService};
    let service = SecretService::connect(EncryptionType::Dh).await?;
    let result = service
        .search_items(std::collections::HashMap::from([
            ("application", "tg-backup"),
            ("account", account),
        ]))
        .await?;
    let item = result
        .unlocked
        .into_iter()
        .chain(result.locked)
        .next()
        .context("credential not found")?;
    item.unlock().await?;
    Ok(item.get_secret().await?)
}
#[cfg(target_os = "linux")]
async fn service_set(account: &str, value: Option<&str>) -> Result<()> {
    use secret_service::{EncryptionType, SecretService};
    let service = SecretService::connect(EncryptionType::Dh).await?;
    let attrs =
        std::collections::HashMap::from([("application", "tg-backup"), ("account", account)]);
    if let Some(value) = value {
        let collection = service.get_default_collection().await?;
        collection.unlock().await?;
        collection
            .create_item(
                "tg-backup API token",
                attrs,
                value.as_bytes(),
                true,
                "text/plain",
            )
            .await?;
    } else {
        let result = service.search_items(attrs).await?;
        for item in result.unlocked.into_iter().chain(result.locked) {
            item.unlock().await?;
            item.delete().await?;
        }
    }
    Ok(())
}
#[cfg(not(target_os = "linux"))]
async fn service_get(_: &str) -> Result<Vec<u8>> {
    anyhow::bail!("Secret Service is supported on Linux; select file or environment storage")
}
#[cfg(not(target_os = "linux"))]
async fn service_set(_: &str, _: Option<&str>) -> Result<()> {
    anyhow::bail!("Secret Service is supported on Linux; select file or environment storage")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn private_file_round_trip_remove_and_permission_rejection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        let secret = Secret::File { path: path.clone() };
        secret.store("sensitive").await.unwrap();
        assert_eq!(secret.load().await.unwrap(), "sensitive");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(secret.load().await.is_err());
        }
        secret.remove().await.unwrap();
        assert!(!path.exists());
    }
}
