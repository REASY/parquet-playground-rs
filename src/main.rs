mod columns_builder;
mod errors;
mod logger;
pub mod model;
mod schema;

use crate::columns_builder::{Builders, ColumnsBuilder};
use crate::errors::AppError;
use crate::model::Metric;
use clap::{Parser, ValueEnum};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
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
                all_tags.insert(&t);
            });
        });
        let mut xs: Vec<&str> = all_tags.iter().map(|x| *x).collect();
        xs.sort();
        xs.iter().map(|c| Builders::as_hex(*c)).collect()
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
        builders.append(&s)?;
    }

    let enabled_statistics = match &args.statistics_mode {
        StatisticsMode::None => EnabledStatistics::None,
        StatisticsMode::Chunk => EnabledStatistics::Chunk,
        StatisticsMode::Page => EnabledStatistics::Page,
    };
    let file = std::fs::File::create(args.output_parquet_file_path)?;
    let compression = Compression::ZSTD(ZstdLevel::try_new(3)?);
    let props = WriterProperties::builder()
        .set_statistics_enabled(enabled_statistics)
        .set_compression(compression)
        .set_dictionary_enabled(true)
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

fn read_metric(path: &str) -> Result<Metric, AppError> {
    let metric = {
        let mut bytes = Vec::new();
        let mut f = std::fs::File::open(path)?;
        f.read_to_end(&mut bytes)?;
        from_slice::<Metric>(&bytes)?
    };
    Ok(metric)
}
