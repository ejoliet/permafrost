//! Buffering, flush triggers and Parquet segment writing (README §Interface Contracts 2).

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, MapBuilder, MapFieldNames, StringBuilder, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use permafrost_protocol::LabelIndex;
use tokio::sync::{Mutex, mpsc};

use crate::ingest::Batch;

/// Labels copied into their own column so the browser can filter without
/// touching the `labels` map (README §Interface Contracts 2).
pub const PROMOTED_LABELS: [&str; 3] = ["job", "host", "level"];

/// Target row group size. The browser fetches row groups, so this bounds the
/// smallest useful range read.
const ROW_GROUP_TARGET_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum SegmentWriteError {
    #[error("writing segment {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("encoding segment: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("writing parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
}

/// One flushed segment, ready to be announced as a blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentMeta {
    pub path: PathBuf,
    pub start_ns: i64,
    pub end_ns: i64,
    pub rows: u64,
    pub bytes: u64,
    pub label_index: LabelIndex,
}

#[derive(Debug, Clone)]
struct Row {
    ts_ns: i64,
    line: String,
    labels: BTreeMap<String, String>,
}

#[derive(Default)]
struct Buffer {
    rows: Vec<Row>,
    /// Wall clock of the first row buffered, for the age-based trigger.
    opened_at: Option<SystemTime>,
}

pub struct SegmentWriter {
    dir: PathBuf,
    max_age: Duration,
    max_bytes: u64,
    buffer: Mutex<Buffer>,
    buffered_bytes: AtomicU64,
    flushed: AtomicU64,
    announce: mpsc::UnboundedSender<SegmentMeta>,
}

impl SegmentWriter {
    /// Returns the writer plus the stream of flushed segments; the p2p task owns
    /// the receiver. An unbounded channel is safe here: one message per flush.
    pub fn new(
        dir: PathBuf,
        max_age: Duration,
        max_bytes: u64,
    ) -> (Arc<Self>, mpsc::UnboundedReceiver<SegmentMeta>) {
        let (announce, rx) = mpsc::unbounded_channel();
        let writer = Arc::new(Self {
            dir,
            max_age,
            max_bytes,
            buffer: Mutex::new(Buffer::default()),
            buffered_bytes: AtomicU64::new(0),
            flushed: AtomicU64::new(0),
            announce,
        });
        (writer, rx)
    }

    pub fn buffered_bytes(&self) -> u64 {
        self.buffered_bytes.load(Ordering::Relaxed)
    }

    pub fn flushed_count(&self) -> u64 {
        self.flushed.load(Ordering::Relaxed)
    }

    /// Buffer a decoded batch, flushing if either trigger fires.
    pub async fn append(&self, batch: &Batch) -> Result<Option<SegmentMeta>, SegmentWriteError> {
        {
            let mut buf = self.buffer.lock().await;
            for stream in &batch.streams {
                for entry in &stream.entries {
                    buf.rows.push(Row {
                        ts_ns: entry.ts_ns,
                        line: entry.line.clone(),
                        labels: stream.labels.clone(),
                    });
                }
            }
            if buf.opened_at.is_none() && !buf.rows.is_empty() {
                buf.opened_at = Some(SystemTime::now());
            }
        }
        self.buffered_bytes
            .fetch_add(batch.bytes(), Ordering::Relaxed);

        if self.should_flush().await {
            self.flush().await
        } else {
            Ok(None)
        }
    }

    async fn should_flush(&self) -> bool {
        if self.buffered_bytes() >= self.max_bytes {
            return true;
        }
        let buf = self.buffer.lock().await;
        buf.opened_at
            .is_some_and(|t| t.elapsed().unwrap_or_default() >= self.max_age)
    }

    /// Flush on the age trigger only; called by the background ticker.
    pub async fn flush_if_stale(&self) -> Result<Option<SegmentMeta>, SegmentWriteError> {
        if self.should_flush().await {
            self.flush().await
        } else {
            Ok(None)
        }
    }

    /// Write everything buffered as one Parquet segment across all streams.
    pub async fn flush(&self) -> Result<Option<SegmentMeta>, SegmentWriteError> {
        let rows = {
            let mut buf = self.buffer.lock().await;
            buf.opened_at = None;
            std::mem::take(&mut buf.rows)
        };
        self.buffered_bytes.store(0, Ordering::Relaxed);
        if rows.is_empty() {
            return Ok(None);
        }

        std::fs::create_dir_all(&self.dir).map_err(|source| SegmentWriteError::Io {
            path: self.dir.clone(),
            source,
        })?;
        let meta = write_segment(&self.dir, rows)?;
        self.flushed.fetch_add(1, Ordering::Relaxed);
        // Receiver gone means shutdown; losing the announce is not worth failing
        // a flush that already hit disk.
        let _ = self.announce.send(meta.clone());
        Ok(Some(meta))
    }
}

/// Sort by `ts`, promote common labels, and write one zstd Parquet file.
fn write_segment(dir: &Path, mut rows: Vec<Row>) -> Result<SegmentMeta, SegmentWriteError> {
    rows.sort_by_key(|r| r.ts_ns);
    let start_ns = rows.first().map(|r| r.ts_ns).unwrap_or_default();
    let end_ns = rows.last().map(|r| r.ts_ns).unwrap_or_default();

    let mut ts = Vec::with_capacity(rows.len());
    let mut lines = StringBuilder::new();
    let mut labels = MapBuilder::new(
        // AIDEV-NOTE: the Parquet MAP convention is key_value/key/value; arrow-rs
        // defaults to entries/keys/values, which some readers do not map back
        // to a logical MAP.
        Some(MapFieldNames {
            entry: "key_value".into(),
            key: "key".into(),
            value: "value".into(),
        }),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    let mut promoted: Vec<StringBuilder> = PROMOTED_LABELS
        .iter()
        .map(|_| StringBuilder::new())
        .collect();
    let mut label_index: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut uncompressed: u64 = 0;

    for row in &rows {
        ts.push(row.ts_ns);
        lines.append_value(&row.line);
        uncompressed += row.line.len() as u64 + 8;
        for (k, v) in &row.labels {
            labels.keys().append_value(k);
            labels.values().append_value(v);
            uncompressed += (k.len() + v.len()) as u64;
        }
        labels.append(true)?;
        for (i, name) in PROMOTED_LABELS.iter().enumerate() {
            match row.labels.get(*name) {
                Some(v) => {
                    promoted[i].append_value(v);
                    label_index
                        .entry((*name).to_string())
                        .or_default()
                        .insert(v.clone());
                }
                None => promoted[i].append_null(),
            }
        }
    }

    let mut columns: Vec<(&str, ArrayRef, bool)> = vec![
        (
            "ts",
            Arc::new(TimestampNanosecondArray::from(ts).with_timezone("UTC".to_string()))
                as ArrayRef,
            false,
        ),
        ("line", Arc::new(lines.finish()) as ArrayRef, false),
        ("labels", Arc::new(labels.finish()) as ArrayRef, false),
    ];
    for (i, name) in PROMOTED_LABELS.iter().enumerate() {
        columns.push((name, Arc::new(promoted[i].finish()) as ArrayRef, true));
    }

    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, array, nullable)| Field::new(*name, array.data_type().clone(), *nullable))
            .collect::<Vec<_>>(),
    ));
    debug_assert_eq!(
        schema.field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        "ts must stay a UTC nanosecond timestamp"
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        columns.into_iter().map(|(_, array, _)| array).collect(),
    )?;

    // Rows per group that lands near the 8 MB target for this segment's shape.
    let avg_row = (uncompressed / rows.len() as u64).max(1);
    let rows_per_group = (ROW_GROUP_TARGET_BYTES / avg_row).clamp(1_024, 1_000_000) as usize;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_max_row_group_row_count(Some(rows_per_group))
        .build();

    let path = dir.join(format!("seg-{start_ns:020}-{end_ns:020}.parquet"));
    let file = std::fs::File::create(&path).map_err(|source| SegmentWriteError::Io {
        path: path.clone(),
        source,
    })?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    let bytes = std::fs::metadata(&path)
        .map_err(|source| SegmentWriteError::Io {
            path: path.clone(),
            source,
        })?
        .len();

    Ok(SegmentMeta {
        path,
        start_ns,
        end_ns,
        rows: rows.len() as u64,
        bytes,
        label_index: label_index
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().collect::<Vec<_>>()))
            .collect(),
    })
}

/// Drop segments whose newest row is older than `retention`. Returns the paths
/// removed so the caller can prune the manifest.
pub fn purge_expired(dir: &Path, retention: Duration, now: SystemTime) -> Vec<PathBuf> {
    let cutoff_ns = now
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .saturating_sub(retention)
        .as_nanos() as i64;

    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut removed = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        // AIDEV-NOTE: the end timestamp is in the filename precisely so retention
        // never has to open and parse a Parquet footer.
        let Some(end_ns) = parse_segment_end_ns(&path) else {
            continue;
        };
        if end_ns < cutoff_ns && std::fs::remove_file(&path).is_ok() {
            removed.push(path);
        }
    }
    removed
}

fn parse_segment_end_ns(path: &Path) -> Option<i64> {
    let name = path.file_name()?.to_str()?;
    let rest = name.strip_prefix("seg-")?.strip_suffix(".parquet")?;
    rest.split_once('-')?.1.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::{Entry, Stream};
    use arrow::array::Array;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    /// (timestamp, line, labels) — spelled out so clippy's complex-type lint
    /// has something to point at.
    type Row<'a> = (i64, &'a str, &'a [(&'a str, &'a str)]);

    fn batch(rows: &[Row<'_>]) -> Batch {
        Batch {
            streams: rows
                .iter()
                .map(|(ts, line, labels)| Stream {
                    labels: labels
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                    entries: vec![Entry {
                        ts_ns: *ts,
                        line: (*line).to_string(),
                    }],
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn flush_writes_a_sorted_segment_with_promoted_labels() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (writer, mut rx) = SegmentWriter::new(
            dir.path().to_path_buf(),
            Duration::from_secs(3600),
            u64::MAX,
        );

        writer
            .append(&batch(&[
                (300, "third", &[("job", "api"), ("level", "error")]),
                (100, "first", &[("job", "api"), ("level", "info")]),
                (200, "second", &[("job", "worker"), ("pod", "p1")]),
            ]))
            .await
            .expect("append");
        assert!(writer.buffered_bytes() > 0);

        let meta = writer
            .flush()
            .await
            .expect("flush")
            .expect("segment written");
        assert_eq!(writer.buffered_bytes(), 0);
        assert_eq!(meta.rows, 3);
        assert_eq!((meta.start_ns, meta.end_ns), (100, 300));
        assert_eq!(
            meta.label_index.get("job"),
            Some(&vec!["api".to_string(), "worker".to_string()])
        );
        assert_eq!(
            meta.label_index.get("level"),
            Some(&vec!["error".to_string(), "info".to_string()]),
            "only promoted labels are indexed"
        );
        assert!(
            !meta.label_index.contains_key("pod"),
            "pod is not a promoted label"
        );
        assert_eq!(rx.try_recv().expect("announced").path, meta.path);

        let file = std::fs::File::open(&meta.path).expect("open segment");
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("parquet reader")
            .build()
            .expect("build");
        let rb = reader.next().expect("one batch").expect("readable");
        assert_eq!(rb.num_rows(), 3);
        assert_eq!(
            rb.schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            vec!["ts", "line", "labels", "job", "host", "level"]
        );
        assert_eq!(
            rb.schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );

        let ts = rb
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts column");
        assert_eq!(ts.values(), &[100, 200, 300], "rows must be sorted by ts");

        let lines = arrow::array::cast::as_string_array(rb.column(1));
        assert_eq!(lines.value(0), "first");
        let job = arrow::array::cast::as_string_array(rb.column(3));
        assert_eq!(job.value(1), "worker");
        let host = arrow::array::cast::as_string_array(rb.column(4));
        assert!(
            host.is_null(0),
            "absent promoted labels are null, not empty string"
        );
    }

    #[tokio::test]
    async fn size_trigger_flushes_without_an_explicit_call() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (writer, _rx) =
            SegmentWriter::new(dir.path().to_path_buf(), Duration::from_secs(3600), 16);
        let meta = writer
            .append(&batch(&[(
                1,
                "a line long enough to exceed the tiny cap",
                &[("job", "api")],
            )]))
            .await
            .expect("append");
        assert!(meta.is_some(), "size trigger must flush");
        assert_eq!(writer.flushed_count(), 1);
    }

    #[tokio::test]
    async fn age_trigger_flushes_on_the_ticker() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (writer, _rx) =
            SegmentWriter::new(dir.path().to_path_buf(), Duration::from_millis(1), u64::MAX);
        assert!(writer.flush_if_stale().await.expect("no rows").is_none());
        writer
            .append(&batch(&[(1, "x", &[])]))
            .await
            .expect("append");
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(writer.flush_if_stale().await.expect("flush").is_some());
    }

    #[tokio::test]
    async fn flushing_an_empty_buffer_is_a_no_op() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (writer, _rx) =
            SegmentWriter::new(dir.path().to_path_buf(), Duration::from_secs(1), u64::MAX);
        assert!(writer.flush().await.expect("flush").is_none());
        assert_eq!(std::fs::read_dir(dir.path()).expect("readdir").count(), 0);
    }

    #[test]
    fn retention_deletes_only_segments_past_the_cutoff() {
        let dir = tempfile::tempdir().expect("tempdir");
        let now = UNIX_EPOCH + Duration::from_secs(10_000);
        let old = dir
            .path()
            .join(format!("seg-{:020}-{:020}.parquet", 0, 1_000_000_000i64));
        let fresh = dir.path().join(format!(
            "seg-{:020}-{:020}.parquet",
            0, 9_999_000_000_000i64
        ));
        let other = dir.path().join("notes.txt");
        for p in [&old, &fresh, &other] {
            std::fs::write(p, b"x").expect("write");
        }

        let removed = purge_expired(dir.path(), Duration::from_secs(3600), now);
        assert_eq!(removed, vec![old.clone()]);
        assert!(!old.exists());
        assert!(
            fresh.exists(),
            "segment inside the retention window survives"
        );
        assert!(other.exists(), "unrelated files are never touched");
    }
}
