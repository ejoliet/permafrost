//! wasmtime host for ingest rules: ABI, fuel/memory sandboxing, hot reload
//! (README §Interface Contracts 6).

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};
use wasmtime::{Engine, Instance, Module, Store, StoreLimits, StoreLimitsBuilder};

use crate::ingest::Batch;

/// Hard ceiling on a rule's linear memory (README §Interface Contracts 6).
pub const RULE_MEMORY_LIMIT: usize = 16 * 1024 * 1024;

/// A rule may report at most this many sample matches; extras are dropped.
pub const MAX_SAMPLES: usize = 10;

#[derive(Debug, thiserror::Error)]
pub enum RuleError {
    #[error("reading rules dir {path}: {source}")]
    Dir {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("rule {rule}: compiling module: {message}")]
    Compile { rule: String, message: String },
    #[error("rule {rule}: {message}")]
    Trap { rule: String, message: String },
    #[error("rule {rule}: exhausted its fuel budget")]
    FuelExhausted { rule: String },
    #[error("rule {rule}: missing required export `{export}`")]
    Abi { rule: String, export: &'static str },
    #[error("rule {rule}: returned invalid JSON: {message}")]
    Output { rule: String, message: String },
}

impl RuleError {
    fn rule(&self) -> &str {
        match self {
            RuleError::Dir { .. } => "<none>",
            RuleError::Compile { rule, .. }
            | RuleError::Trap { rule, .. }
            | RuleError::FuelExhausted { rule }
            | RuleError::Abi { rule, .. }
            | RuleError::Output { rule, .. } => rule,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    #[default]
    Warn,
    Error,
    Crit,
}

impl Severity {
    pub fn emoji(self) -> &'static str {
        match self {
            Severity::Warn => "⚠️",
            Severity::Error => "🔴",
            Severity::Crit => "🚨",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchSample {
    pub ts: i64,
    pub line: String,
}

/// A rule's `check` output, as defined by the ABI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleOutcome {
    pub rule: String,
    #[serde(default)]
    pub severity: Severity,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub matches: Vec<MatchSample>,
    /// Labels of the batch that triggered the rule; filled in by the host, not
    /// the rule, so a rule cannot forge them into the deep link.
    #[serde(skip)]
    pub labels: Vec<(String, String)>,
}

/// What one evaluation pass produced. A disabled rule is news the operator
/// needs, so it travels the same path as a match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleEvent {
    Match(Box<RuleOutcome>),
    Disabled { rule: String, reason: String },
}

struct LoadedRule {
    name: String,
    module: Module,
}

struct RuleStore {
    limits: StoreLimits,
}

pub struct RuleEngine {
    engine: Engine,
    dir: PathBuf,
    fuel: u64,
    rules: RwLock<Vec<LoadedRule>>,
    /// Rules that trapped or ran out of fuel. Entries stay until the file
    /// changes, so a broken rule is reported once, not once per batch.
    disabled: Mutex<HashMap<String, String>>,
}

impl RuleEngine {
    pub fn new(dir: PathBuf, fuel: u64) -> Result<Arc<Self>, RuleError> {
        let mut config = wasmtime::Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config).map_err(|e| RuleError::Compile {
            rule: "<engine>".into(),
            message: e.to_string(),
        })?;
        let engine = Arc::new(Self {
            engine,
            dir,
            fuel,
            rules: RwLock::new(Vec::new()),
            disabled: Mutex::new(HashMap::new()),
        });
        engine.reload()?;
        Ok(engine)
    }

    pub fn rule_names(&self) -> Vec<String> {
        self.rules
            .read()
            .map(|r| r.iter().map(|r| r.name.clone()).collect())
            .unwrap_or_default()
    }

    /// Compile every `.wasm` in the rules dir. A missing directory is not an
    /// error: running without rules is a legitimate configuration.
    pub fn reload(&self) -> Result<(), RuleError> {
        let mut loaded = Vec::new();
        let mut names = BTreeSet::new();
        if self.dir.exists() {
            let entries = std::fs::read_dir(&self.dir).map_err(|source| RuleError::Dir {
                path: self.dir.clone(),
                source,
            })?;
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_none_or(|e| e != "wasm") {
                    continue;
                }
                let name = rule_name(&path);
                match Module::from_file(&self.engine, &path) {
                    Ok(module) => {
                        names.insert(name.clone());
                        loaded.push(LoadedRule { name, module });
                    }
                    Err(err) => {
                        tracing::error!(rule = %name, error = %err, "skipping unloadable rule");
                    }
                }
            }
        }
        loaded.sort_by(|a, b| a.name.cmp(&b.name));
        tracing::info!(count = loaded.len(), rules = ?names, "rules loaded");

        if let Ok(mut disabled) = self.disabled.lock() {
            // A reload means the operator edited something: give every rule
            // another chance rather than requiring a restart.
            disabled.clear();
        }
        if let Ok(mut rules) = self.rules.write() {
            *rules = loaded;
        }
        Ok(())
    }

    /// Run every enabled rule over one batch.
    ///
    /// AIDEV-NOTE: synchronous and CPU-bound, called from the axum handler. Rules are
    /// fuel-capped in the low milliseconds, so blocking the worker beats the
    /// complexity of a rule thread pool. Revisit if rule count grows.
    pub fn evaluate(&self, batch: &Batch) -> Vec<RuleEvent> {
        if batch.rows() == 0 {
            return Vec::new();
        }
        let input = batch.to_rule_json().to_string();
        let labels: Vec<(String, String)> = batch
            .streams
            .first()
            .map(|s| {
                s.labels
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default();

        let names: Vec<String> = {
            let Ok(rules) = self.rules.read() else {
                return Vec::new();
            };
            rules.iter().map(|r| r.name.clone()).collect()
        };

        let mut events = Vec::new();
        for name in names {
            if self.disabled.lock().is_ok_and(|d| d.contains_key(&name)) {
                continue;
            }
            let module = {
                let Ok(rules) = self.rules.read() else {
                    continue;
                };
                match rules.iter().find(|r| r.name == name) {
                    Some(r) => r.module.clone(),
                    None => continue,
                }
            };
            match self.run_one(&name, &module, &input) {
                Ok(None) => {}
                Ok(Some(mut outcome)) => {
                    outcome.rule = if outcome.rule.is_empty() {
                        name
                    } else {
                        outcome.rule
                    };
                    outcome.matches.truncate(MAX_SAMPLES);
                    outcome.labels = labels.clone();
                    events.push(RuleEvent::Match(Box::new(outcome)));
                }
                Err(err) => {
                    let reason = err.to_string();
                    tracing::error!(rule = %err.rule(), error = %reason, "disabling rule");
                    let newly_disabled = self
                        .disabled
                        .lock()
                        .map(|mut d| d.insert(name.clone(), reason.clone()).is_none())
                        .unwrap_or(false);
                    if newly_disabled {
                        events.push(RuleEvent::Disabled { rule: name, reason });
                    }
                }
            }
        }
        events
    }

    fn run_one(
        &self,
        name: &str,
        module: &Module,
        input: &str,
    ) -> Result<Option<RuleOutcome>, RuleError> {
        let trap = |e: wasmtime::Error| -> RuleError {
            if e.downcast_ref::<wasmtime::Trap>() == Some(&wasmtime::Trap::OutOfFuel) {
                RuleError::FuelExhausted {
                    rule: name.to_string(),
                }
            } else {
                RuleError::Trap {
                    rule: name.to_string(),
                    message: e.to_string(),
                }
            }
        };

        let limits = StoreLimitsBuilder::new()
            .memory_size(RULE_MEMORY_LIMIT)
            .build();
        let mut store = Store::new(&self.engine, RuleStore { limits });
        store.limiter(|s| &mut s.limits);
        store.set_fuel(self.fuel).map_err(trap)?;

        let instance = Instance::new(&mut store, module, &[]).map_err(trap)?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or(RuleError::Abi {
                rule: name.to_string(),
                export: "memory",
            })?;
        let alloc = instance
            .get_typed_func::<u32, u32>(&mut store, "alloc")
            .map_err(|_| RuleError::Abi {
                rule: name.to_string(),
                export: "alloc",
            })?;
        let check = instance
            .get_typed_func::<(u32, u32), u64>(&mut store, "check")
            .map_err(|_| RuleError::Abi {
                rule: name.to_string(),
                export: "check",
            })?;

        let len = input.len() as u32;
        let ptr = alloc.call(&mut store, len).map_err(trap)?;
        memory
            .write(&mut store, ptr as usize, input.as_bytes())
            .map_err(|e| RuleError::Trap {
                rule: name.to_string(),
                message: e.to_string(),
            })?;

        let packed = check.call(&mut store, (ptr, len)).map_err(trap)?;
        if packed == 0 {
            return Ok(None);
        }

        // ABI: high 32 bits are the pointer, low 32 the length.
        let out_ptr = (packed >> 32) as usize;
        let out_len = (packed & 0xffff_ffff) as usize;
        let data = memory.data(&store);
        let bytes = data
            .get(out_ptr..out_ptr.saturating_add(out_len))
            .ok_or_else(|| RuleError::Output {
                rule: name.to_string(),
                message: format!("result {out_ptr}..{out_len} is outside linear memory"),
            })?;
        let outcome: RuleOutcome =
            serde_json::from_slice(bytes).map_err(|e| RuleError::Output {
                rule: name.to_string(),
                message: e.to_string(),
            })?;
        Ok(Some(outcome))
    }
}

fn rule_name(path: &Path) -> String {
    path.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unnamed")
        .to_string()
}

/// Watch the rules dir and recompile on change. Returns the watcher, which must
/// stay alive for events to keep arriving.
pub fn watch(engine: Arc<RuleEngine>) -> Result<notify::RecommendedWatcher, notify::Error> {
    use notify::{RecursiveMode, Watcher};

    let dir = engine.dir.clone();
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        let Ok(event) = res else { return };
        if !event
            .paths
            .iter()
            .any(|p| p.extension().is_some_and(|e| e == "wasm"))
        {
            return;
        }
        if let Err(err) = engine.reload() {
            tracing::error!(%err, "rule hot reload failed");
        }
    })?;
    if dir.exists() {
        watcher.watch(&dir, RecursiveMode::NonRecursive)?;
        tracing::info!(dir = %dir.display(), "watching rules directory");
    }
    Ok(watcher)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::{Entry, Stream};
    use std::collections::BTreeMap;

    fn batch(line: &str) -> Batch {
        Batch {
            streams: vec![Stream {
                labels: BTreeMap::from([("job".to_string(), "api".to_string())]),
                entries: vec![Entry {
                    ts_ns: 42,
                    line: line.to_string(),
                }],
            }],
        }
    }

    /// A rule written in WAT so the host tests never depend on the example
    /// crates having been compiled first.
    fn wat_rule(body: &str) -> String {
        format!(
            r#"(module
  (memory (export "memory") 2)
  (global $bump (mut i32) (i32.const 1024))
  (func (export "alloc") (param $len i32) (result i32)
    (local $p i32)
    (local.set $p (global.get $bump))
    (global.set $bump (i32.add (global.get $bump) (local.get $len)))
    (local.get $p))
  {body})"#
        )
    }

    fn write_rule(dir: &Path, name: &str, wat: &str) {
        let bytes = wat::parse_str(wat).expect("valid wat");
        std::fs::write(dir.join(format!("{name}.wasm")), bytes).expect("write rule");
    }

    #[test]
    fn matching_rule_produces_an_outcome_with_host_supplied_labels() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Writes a fixed JSON result at offset 0 and returns packed ptr/len.
        let json = r#"{"rule":"demo","severity":"crit","message":"boom","matches":[{"ts":42,"line":"panic"}]}"#;
        write_rule(
            dir.path(),
            "demo",
            &wat_rule(&format!(
                r#"(data (i32.const 0) "{escaped}")
  (func (export "check") (param i32 i32) (result i64)
    (i64.const {len}))"#,
                escaped = json.replace('"', "\\22"),
                len = json.len() as i64
            )),
        );

        let engine = RuleEngine::new(dir.path().to_path_buf(), 10_000_000).expect("engine");
        assert_eq!(engine.rule_names(), vec!["demo".to_string()]);

        let events = engine.evaluate(&batch("panic"));
        let RuleEvent::Match(outcome) = &events[0] else {
            panic!("expected a match: {events:?}")
        };
        assert_eq!(outcome.rule, "demo");
        assert_eq!(outcome.severity, Severity::Crit);
        assert_eq!(
            outcome.matches,
            vec![MatchSample {
                ts: 42,
                line: "panic".into()
            }]
        );
        assert_eq!(outcome.labels, vec![("job".to_string(), "api".to_string())]);
    }

    #[test]
    fn zero_return_means_no_match() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_rule(
            dir.path(),
            "quiet",
            &wat_rule(r#"(func (export "check") (param i32 i32) (result i64) (i64.const 0))"#),
        );
        let engine = RuleEngine::new(dir.path().to_path_buf(), 10_000_000).expect("engine");
        assert!(engine.evaluate(&batch("all fine")).is_empty());
    }

    #[test]
    fn runaway_rule_is_fuel_killed_and_disabled_exactly_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_rule(
            dir.path(),
            "hostile",
            &wat_rule(
                r#"(func (export "check") (param i32 i32) (result i64)
    (loop $spin (br $spin))
    (i64.const 0))"#,
            ),
        );
        let engine = RuleEngine::new(dir.path().to_path_buf(), 100_000).expect("engine");

        let first = engine.evaluate(&batch("anything"));
        match &first[0] {
            RuleEvent::Disabled { rule, reason } => {
                assert_eq!(rule, "hostile");
                assert!(
                    reason.contains("fuel"),
                    "expected fuel exhaustion, got {reason}"
                );
            }
            other => panic!("expected the rule to be disabled, got {other:?}"),
        }

        // Second batch: still disabled, and no repeat notification.
        assert!(
            engine.evaluate(&batch("anything")).is_empty(),
            "must not re-alert"
        );

        // Reload gives the operator's edit a fresh chance.
        engine.reload().expect("reload");
        assert_eq!(
            engine.evaluate(&batch("anything")).len(),
            1,
            "reload re-enables"
        );
    }

    #[test]
    fn trapping_rule_is_disabled_not_fatal() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_rule(
            dir.path(),
            "boom",
            &wat_rule(r#"(func (export "check") (param i32 i32) (result i64) (unreachable))"#),
        );
        let engine = RuleEngine::new(dir.path().to_path_buf(), 10_000_000).expect("engine");
        assert!(matches!(
            engine.evaluate(&batch("x"))[0],
            RuleEvent::Disabled { .. }
        ));
    }

    #[test]
    fn rule_missing_an_abi_export_is_disabled() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_rule(dir.path(), "half", &wat_rule(""));
        let engine = RuleEngine::new(dir.path().to_path_buf(), 10_000_000).expect("engine");
        match &engine.evaluate(&batch("x"))[0] {
            RuleEvent::Disabled { reason, .. } => assert!(reason.contains("check"), "{reason}"),
            other => panic!("expected disabled, got {other:?}"),
        }
    }

    #[test]
    fn garbage_output_is_disabled_rather_than_propagated() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_rule(
            dir.path(),
            "junk",
            &wat_rule(
                r#"(data (i32.const 0) "not json")
  (func (export "check") (param i32 i32) (result i64) (i64.const 8))"#,
            ),
        );
        let engine = RuleEngine::new(dir.path().to_path_buf(), 10_000_000).expect("engine");
        assert!(matches!(
            engine.evaluate(&batch("x"))[0],
            RuleEvent::Disabled { .. }
        ));
    }

    #[test]
    fn out_of_bounds_result_pointer_cannot_read_past_memory() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_rule(
            dir.path(),
            "oob",
            &wat_rule(
                // ptr = 0xffff_ffff, len = 16
                r#"(func (export "check") (param i32 i32) (result i64) (i64.const -4294967280))"#,
            ),
        );
        let engine = RuleEngine::new(dir.path().to_path_buf(), 10_000_000).expect("engine");
        assert!(matches!(
            engine.evaluate(&batch("x"))[0],
            RuleEvent::Disabled { .. }
        ));
    }

    #[test]
    fn missing_rules_dir_is_not_an_error() {
        let engine = RuleEngine::new(PathBuf::from("/nonexistent/permafrost-rules"), 1_000)
            .expect("empty engine");
        assert!(engine.rule_names().is_empty());
        assert!(engine.evaluate(&batch("x")).is_empty());
    }

    #[test]
    fn empty_batches_skip_evaluation() {
        let engine = RuleEngine::new(PathBuf::from("/nonexistent"), 1_000).expect("engine");
        assert!(engine.evaluate(&Batch::default()).is_empty());
    }
}
