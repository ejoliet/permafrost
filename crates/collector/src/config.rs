//! Configuration: TOML file < env vars < CLI flags (README §Configuration Reference).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::Parser;
use serde::Deserialize;

/// Ingest refuses new batches once buffered bytes exceed this (README §Error Handling).
pub const BACKPRESSURE_MAX_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("reading config file {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("parsing config file {path}: {source}")]
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("{var}: {message}")]
    BadValue { var: &'static str, message: String },
}

/// AIDEV-NOTE: every field is `Option` at parse time so the three sources can be
/// layered explicitly. clap's built-in `env` support would collapse flag and env
/// into one value, which makes "flags win over env" unverifiable.
#[derive(Debug, Parser)]
#[command(
    name = "permafrost",
    version,
    about = "Loki-compatible ingest, Parquet segments, served over iroh"
)]
pub struct Cli {
    /// TOML config file; every key mirrors the long flag name.
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// Loki push API bind address.
    #[arg(long)]
    pub listen: Option<SocketAddr>,
    /// Parquet segments + iroh blob store.
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
    /// Directory of `.wasm` rule modules, hot-reloaded on change.
    #[arg(long)]
    pub rules_dir: Option<PathBuf>,
    /// Slack incoming webhook.
    #[arg(long)]
    pub slack_webhook: Option<String>,
    /// Base URL of the static query page.
    #[arg(long)]
    pub ui_base_url: Option<String>,
    /// Time-based flush trigger, e.g. `1h`.
    #[arg(long)]
    pub segment_max_age: Option<String>,
    /// Size-based flush trigger, e.g. `64MiB`.
    #[arg(long)]
    pub segment_max_bytes: Option<String>,
    /// Delete segments older than this, e.g. `720h`.
    #[arg(long)]
    pub retention: Option<String>,
    /// wasmtime fuel per rule per batch.
    #[arg(long)]
    pub rule_fuel: Option<u64>,
    /// iroh node identity; generated if absent.
    #[arg(long)]
    pub secret_key_file: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct FileConfig {
    listen: Option<SocketAddr>,
    data_dir: Option<PathBuf>,
    rules_dir: Option<PathBuf>,
    slack_webhook: Option<String>,
    ui_base_url: Option<String>,
    segment_max_age: Option<String>,
    segment_max_bytes: Option<String>,
    retention: Option<String>,
    rule_fuel: Option<u64>,
    secret_key_file: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub listen: SocketAddr,
    pub data_dir: PathBuf,
    pub rules_dir: PathBuf,
    pub slack_webhook: Option<String>,
    pub ui_base_url: Option<String>,
    pub segment_max_age: Duration,
    pub segment_max_bytes: u64,
    pub retention: Duration,
    pub rule_fuel: u64,
    pub secret_key_file: PathBuf,
}

impl Config {
    pub fn load(cli: Cli) -> Result<Self, ConfigError> {
        let file = match &cli.config {
            Some(path) => {
                let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
                    path: path.clone(),
                    source,
                })?;
                toml::from_str(&raw).map_err(|source| ConfigError::Parse {
                    path: path.clone(),
                    source,
                })?
            }
            None => FileConfig::default(),
        };

        let listen = pick(cli.listen, env_parse("PERMAFROST_LISTEN")?, file.listen)
            .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 3100)));
        let data_dir = pick(cli.data_dir, env_path("PERMAFROST_DATA_DIR"), file.data_dir)
            .unwrap_or_else(|| PathBuf::from("./data"));
        let rules_dir = pick(
            cli.rules_dir,
            env_path("PERMAFROST_RULES_DIR"),
            file.rules_dir,
        )
        .unwrap_or_else(|| PathBuf::from("./rules"));

        let segment_max_age = duration(
            "PERMAFROST_SEGMENT_MAX_AGE",
            pick(
                cli.segment_max_age,
                env_str("PERMAFROST_SEGMENT_MAX_AGE"),
                file.segment_max_age,
            ),
            Duration::from_secs(3600),
        )?;
        let retention = duration(
            "PERMAFROST_RETENTION",
            pick(
                cli.retention,
                env_str("PERMAFROST_RETENTION"),
                file.retention,
            ),
            Duration::from_secs(720 * 3600),
        )?;
        let segment_max_bytes = match pick(
            cli.segment_max_bytes,
            env_str("PERMAFROST_SEGMENT_MAX_BYTES"),
            file.segment_max_bytes,
        ) {
            Some(raw) => parse_bytes(&raw).ok_or_else(|| ConfigError::BadValue {
                var: "PERMAFROST_SEGMENT_MAX_BYTES",
                message: format!("cannot parse {raw:?} as a byte size"),
            })?,
            None => 64 * 1024 * 1024,
        };

        let secret_key_file = pick(
            cli.secret_key_file,
            env_path("PERMAFROST_SECRET_KEY_FILE"),
            file.secret_key_file,
        )
        .unwrap_or_else(|| data_dir.join("iroh.key"));

        Ok(Config {
            listen,
            data_dir,
            rules_dir,
            slack_webhook: pick(
                cli.slack_webhook,
                env_str("PERMAFROST_SLACK_WEBHOOK"),
                file.slack_webhook,
            ),
            ui_base_url: pick(
                cli.ui_base_url,
                env_str("PERMAFROST_UI_BASE_URL"),
                file.ui_base_url,
            ),
            segment_max_age,
            segment_max_bytes,
            retention,
            rule_fuel: pick(
                cli.rule_fuel,
                env_parse("PERMAFROST_RULE_FUEL")?,
                file.rule_fuel,
            )
            .unwrap_or(10_000_000),
            secret_key_file,
        })
    }

    pub fn segments_dir(&self) -> PathBuf {
        self.data_dir.join("segments")
    }

    pub fn blobs_dir(&self) -> PathBuf {
        self.data_dir.join("blobs")
    }
}

fn pick<T>(flag: Option<T>, env: Option<T>, file: Option<T>) -> Option<T> {
    flag.or(env).or(file)
}

fn env_str(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|s| !s.is_empty())
}

fn env_path(var: &str) -> Option<PathBuf> {
    env_str(var).map(PathBuf::from)
}

fn env_parse<T: std::str::FromStr>(var: &'static str) -> Result<Option<T>, ConfigError>
where
    T::Err: std::fmt::Display,
{
    match env_str(var) {
        None => Ok(None),
        Some(raw) => raw
            .parse()
            .map(Some)
            .map_err(|e: T::Err| ConfigError::BadValue {
                var,
                message: format!("{e} (got {raw:?})"),
            }),
    }
}

fn duration(
    var: &'static str,
    raw: Option<String>,
    default: Duration,
) -> Result<Duration, ConfigError> {
    match raw {
        None => Ok(default),
        Some(raw) => humantime::parse_duration(&raw).map_err(|e| ConfigError::BadValue {
            var,
            message: format!("{e} (got {raw:?})"),
        }),
    }
}

/// `64MiB`, `512MB`, `1024`. Binary and decimal suffixes both accepted because
/// operators type whichever they remember.
pub fn parse_bytes(raw: &str) -> Option<u64> {
    let raw = raw.trim();
    let split = raw
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(raw.len());
    let (num, suffix) = raw.split_at(split);
    let num: f64 = num.parse().ok()?;
    let mult: u64 = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" => 1_000,
        "ki" | "kib" => 1 << 10,
        "m" | "mb" => 1_000_000,
        "mi" | "mib" => 1 << 20,
        "g" | "gb" => 1_000_000_000,
        "gi" | "gib" => 1 << 30,
        _ => return None,
    };
    let bytes = num * mult as f64;
    (bytes.is_finite() && bytes >= 0.0).then_some(bytes as u64)
}

/// Redact anything that looks like a secret before it reaches a log line
/// (README §Constraints: never log webhook URLs or ticket contents).
pub fn redact(url: &str) -> String {
    match url.split_once("://") {
        Some((scheme, rest)) => {
            let host = rest.split('/').next().unwrap_or("");
            format!("{scheme}://{host}/<redacted>")
        }
        None => "<redacted>".to_string(),
    }
}

pub fn ensure_dirs(cfg: &Config) -> std::io::Result<()> {
    for dir in [&cfg.data_dir, &cfg.segments_dir(), &cfg.blobs_dir()] {
        std::fs::create_dir_all(dir)?;
    }
    Ok(())
}

pub fn exists(path: &Path) -> bool {
    path.exists()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_sizes_accept_binary_and_decimal_suffixes() {
        assert_eq!(parse_bytes("1024"), Some(1024));
        assert_eq!(parse_bytes("64MiB"), Some(64 * 1024 * 1024));
        assert_eq!(parse_bytes("64mb"), Some(64_000_000));
        assert_eq!(parse_bytes("1.5GiB"), Some(1_610_612_736));
        assert_eq!(parse_bytes("512 MiB"), Some(512 * 1024 * 1024));
        assert_eq!(parse_bytes("banana"), None);
        assert_eq!(parse_bytes("10furlongs"), None);
    }

    #[test]
    fn defaults_match_the_documented_table() {
        let cfg = Config::load(Cli::parse_from(["permafrost"])).expect("defaults are valid");
        assert_eq!(cfg.listen.to_string(), "0.0.0.0:3100");
        assert_eq!(cfg.data_dir, PathBuf::from("./data"));
        assert_eq!(cfg.rules_dir, PathBuf::from("./rules"));
        assert_eq!(cfg.segment_max_age, Duration::from_secs(3600));
        assert_eq!(cfg.segment_max_bytes, 64 * 1024 * 1024);
        assert_eq!(cfg.retention, Duration::from_secs(720 * 3600));
        assert_eq!(cfg.rule_fuel, 10_000_000);
        assert_eq!(cfg.secret_key_file, PathBuf::from("./data/iroh.key"));
    }

    #[test]
    fn flags_win_over_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("permafrost.toml");
        std::fs::write(&path, "listen = \"127.0.0.1:9999\"\nrule-fuel = 5\n").expect("write");
        let cli = Cli::parse_from([
            "permafrost",
            "--config",
            path.to_str().expect("utf8 path"),
            "--listen",
            "127.0.0.1:1234",
        ]);
        let cfg = Config::load(cli).expect("valid config");
        assert_eq!(
            cfg.listen.to_string(),
            "127.0.0.1:1234",
            "flag must beat file"
        );
        assert_eq!(
            cfg.rule_fuel, 5,
            "file value survives when no flag is given"
        );
    }

    #[test]
    fn bad_values_are_named_not_panicked() {
        let cli = Cli::parse_from(["permafrost", "--segment-max-bytes", "twelve"]);
        let err = Config::load(cli).expect_err("must reject");
        assert!(
            err.to_string().contains("PERMAFROST_SEGMENT_MAX_BYTES"),
            "{err}"
        );
    }

    #[test]
    fn redaction_keeps_host_drops_path() {
        let r = redact("https://hooks.slack.com/services/T000/B000/xxxxSECRETxxxx");
        assert_eq!(r, "https://hooks.slack.com/<redacted>");
        assert!(!r.contains("SECRET"));
        assert_eq!(redact("nonsense"), "<redacted>");
    }
}
