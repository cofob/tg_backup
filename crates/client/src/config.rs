use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path};
use tg_backup_credentials::{Secret, private_write};
#[derive(Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub profiles: BTreeMap<String, Profile>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub url: String,
    pub token: Option<Secret>,
}
impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(s) => Ok(toml::from_str(&s).context("invalid client config")?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e.into()),
        }
    }
    pub fn save(&self, path: &Path) -> Result<()> {
        private_write(path, toml::to_string_pretty(self)?.as_bytes())
    }
}
pub fn validate_url(url: &str) -> Result<()> {
    let url = reqwest::Url::parse(url)?;
    ensure!(
        ["http", "https"].contains(&url.scheme()),
        "URL must use HTTP or HTTPS"
    );
    ensure!(
        url.username().is_empty() && url.password().is_none(),
        "credentials belong in token storage"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn profiles_keep_secret_references_and_private_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("client.toml");
        let mut config = Config::default();
        config.profiles.insert(
            "home".into(),
            Profile {
                url: "https://archive.example".into(),
                token: Some(Secret::File {
                    path: dir.path().join("token"),
                }),
            },
        );
        config.save(&path).unwrap();
        let loaded = Config::load(&path).unwrap();
        assert_eq!(loaded.profiles["home"].url, "https://archive.example");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        assert!(validate_url("https://user:password@archive.example").is_err());
        assert!(validate_url("file:///tmp/x").is_err());
    }
}
