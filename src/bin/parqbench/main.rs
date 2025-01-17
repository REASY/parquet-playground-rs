#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use arrow::array::Array;
use arrow::datatypes::DataType;
use clap::Parser;
use parquet::column::reader::get_typed_column_reader;
use parquet::data_type::{ByteArrayType, DoubleType, Int64Type};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Row;
use parquet_playground_rs::errors::AppError;
use parquet_playground_rs::schema::parquet_metadata_to_arrow_schema;
use parquet_playground_rs::vec_pq_reader::{read_f64_column, read_i64_column, read_string_column};
use parquet_playground_rs::{errors, logger};
use std::fs::File;
use std::time::Instant;
use tracing::info;

#[derive(Parser, Debug, Clone)]
#[clap()]
struct AppArgs {
    /// Input path to Parquet
    #[clap(long)]
    input_parquet_file_path: String,
}

fn main() -> errors::Result<()> {
    logger::setup("parqbench", "INFO");

    let args = AppArgs::parse();
    info!("Received args: {:?}", args);
    const ITERS: usize = 100;

    let t: Instant = Instant::now();
    let mut total: usize = 0;
    for _i in 0..ITERS {
        let xs = read_parquet_v2(args.input_parquet_file_path.as_str())?;
        total += xs.len();
    }
    let total_elapsed_ms = t.elapsed().as_millis() as f64;
    let avg = total_elapsed_ms / ITERS as f64;
    info!(
        "Total time is {total_elapsed_ms:.3} ms, average reading time is {avg:.3} ms, the number of rows in parquet is {}", total / ITERS
    );

    Ok(())
}

fn read_parquet(path: &str) -> Result<Vec<Row>, AppError> {
    let f = File::open(path)?;
    let reader = SerializedFileReader::new(f)?;
    let mut xs: Vec<Row> = Vec::new();
    let t: Instant = Instant::now();
    let iter = reader.into_iter();
    let total_elapsed_ms = t.elapsed().as_millis() as f64;
    info!("into_iter is {total_elapsed_ms} ms");

    for maybe_row in iter {
        let row = maybe_row?;
        xs.push(row);
    }
    Ok(xs)
}

fn read_parquet_v2(path: &str) -> Result<Vec<Box<dyn Array>>, AppError> {
    let f = File::open(path)?;
    let reader = SerializedFileReader::new(f)?;

    let schema = parquet_metadata_to_arrow_schema(reader.metadata());
    let rg = reader.get_row_group(0)?;

    let mut col_idx: usize = 0;
    let mut result: Vec<Box<dyn Array>> = Vec::new();
    const BATCH_SIZE: usize = 1000;
    for c in schema.fields.iter() {
        let col_rdr = rg.get_column_reader(col_idx)?;
        let col_desc = rg.metadata().column(col_idx).column_descr();
        match c.data_type() {
            DataType::Int64 => {
                let col_rdr = get_typed_column_reader::<Int64Type>(col_rdr);
                let arr = read_i64_column(col_rdr, col_desc, BATCH_SIZE)?;
                result.push(arr);
            }
            DataType::Float64 => {
                let col_rdr = get_typed_column_reader::<DoubleType>(col_rdr);
                let arr = read_f64_column(col_rdr, col_desc, BATCH_SIZE)?;
                result.push(arr);
            }
            DataType::Utf8 | DataType::Binary => {
                let col_rdr = get_typed_column_reader::<ByteArrayType>(col_rdr);
                let arr = read_string_column(col_rdr, col_desc, BATCH_SIZE)?;
                result.push(arr);
            }
            DataType::List(field) => match field.data_type() {
                DataType::Int64 => {
                    let col_rdr = get_typed_column_reader::<Int64Type>(col_rdr);
                    let arr = read_i64_column(col_rdr, col_desc, BATCH_SIZE)?;
                    result.push(arr);
                }
                DataType::Float64 => {
                    let col_rdr = get_typed_column_reader::<DoubleType>(col_rdr);
                    let arr = read_f64_column(col_rdr, col_desc, BATCH_SIZE)?;
                    result.push(arr);
                }
                x => panic!("{:?}", x),
            },
            x => panic!("{:?}", x),
        }
        col_idx += 1;
    }
    Ok(result)
}
