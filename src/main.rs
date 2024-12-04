mod columns_builder;
mod errors;
mod logger;
pub mod model;
mod schema;

use crate::columns_builder::{Builders, ColumnsBuilder};
use crate::errors::AppError;
use crate::model::Metric;
use clap::{Parser, ValueEnum};
use flate2::read::GzDecoder;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;
use serde_json::from_slice;
use std::collections::HashSet;
use std::io::Read;
use std::time::Instant;
use tracing::info;

#[derive(ValueEnum, Clone, Debug)]
pub enum StatisticsMode {
    /// Compute no statistics
    None,
    /// Compute chunk-level statistics but not page-level
    Chunk,
    /// Compute page-level and chunk-level statistics
    Page,
}

#[derive(Parser, Debug, Clone)]
#[clap()]
struct AppArgs {
    /// Input path to JSON with measurements
    #[clap(long)]
    input_json_file_path: String,
    /// Output path to Parquet
    #[clap(long)]
    output_parquet_file_path: String,
    /// Controls statistics for Parquet
    #[clap(long)]
    statistics_mode: StatisticsMode,
}

fn main() -> errors::Result<()> {
    let t: Instant = Instant::now();
    logger::setup("INFO");

    let args = AppArgs::parse();
    info!("Received args: {:?}", args);

    let metric = read_metric(args.input_json_file_path.as_str())?;
    let all_tags: Vec<String> = {
        let mut all_tags: HashSet<&str> = HashSet::new();
        metric.series.iter().for_each(|s| {
            s.tags.iter().for_each(|t| {
                all_tags.insert(t.as_str());
            });
        });
        let mut xs: Vec<&str> = all_tags.iter().copied().collect();
        xs.sort();
        xs.iter().map(|c| Builders::as_hex(c)).collect()
    };
    info!(
        "`{}` with {} series, total number of tags: {}",
        metric.metric,
        metric.series.len(),
        all_tags.len()
    );
    let mut builders = Builders::new(all_tags.as_slice());
    let schema = builders.schema.clone();
    for s in &metric.series {
        builders.append(s)?;
    }

    let enabled_statistics = match &args.statistics_mode {
        StatisticsMode::None => EnabledStatistics::None,
        StatisticsMode::Chunk => EnabledStatistics::Chunk,
        StatisticsMode::Page => EnabledStatistics::Page,
    };
    let file = std::fs::File::create(args.output_parquet_file_path)?;
    let compression = Compression::ZSTD(ZstdLevel::try_new(3)?);
    let sums_double_col = get_list_column_path("sums_double");
    let sums_long_col = get_list_column_path("sums_long");
    let count_col = get_list_column_path("count");
    let props = WriterProperties::builder()
        .set_statistics_enabled(enabled_statistics)
        // Not much benefit on having status on sums and count, disable it
        .set_column_statistics_enabled(sums_double_col.clone(), EnabledStatistics::None)
        .set_column_statistics_enabled(sums_long_col, EnabledStatistics::None)
        .set_column_statistics_enabled(count_col, EnabledStatistics::None)
        .set_compression(compression)
        .set_dictionary_enabled(true)
        // We want to use BYTE_STREAM_SPLIT for doubles
        .set_column_encoding(sums_double_col, Encoding::BYTE_STREAM_SPLIT)
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .build();
    let wrt_opts = ArrowWriterOptions::new()
        .with_properties(props)
        .with_skip_arrow_metadata(true);
    let mut writer = ArrowWriter::try_new_with_options(file, schema.clone(), wrt_opts)?;
    let batch = builders.get_batch()?;
    writer.write(&batch)?;
    let md = writer.close()?;

    let total_elapsed_secs = (t.elapsed().as_millis() as f64) / 1000.0;
    let avg = md.num_rows as f64 / total_elapsed_secs;
    info!(
        "Wrote {0} rows to parquet in {total_elapsed_secs:.3} seconds, average throughput {avg:.3} msg/s",
        md.num_rows
    );

    Ok(())
}

fn get_list_column_path(column: &str) -> ColumnPath {
    ColumnPath::from(vec![
        column.to_owned(),
        "list".to_owned(),
        "item".to_owned(),
    ])
}

fn read_metric(path: &str) -> Result<Metric, AppError> {
    let metric = {
        let mut bytes = Vec::new();
        let mut f = std::fs::File::open(path)?;
        f.read_to_end(&mut bytes)?;
        if path.ends_with(".gz") {
            let mut decoder = GzDecoder::new(bytes.as_slice());
            let mut uncompressed = Vec::new();
            decoder.read_to_end(&mut uncompressed)?;
            bytes = uncompressed;
        }
        from_slice::<Metric>(&bytes)?
    };
    Ok(metric)
}
