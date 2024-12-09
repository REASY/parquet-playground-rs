#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use clap::Parser;
use parquet::column::reader::{ColumnReader, ColumnReaderImpl};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Row;
use parquet_playground_rs::errors::AppError;
use parquet_playground_rs::{errors, logger};
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;
use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, ListArray, ListBuilder, StringArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Float64Type};
use parquet::data_type::{DoubleType, Int64Type};
use parquet::schema::types::ColumnDescriptor;
use tracing::info;
use parquet_playground_rs::schema::{parquet_metadata_to_arrow_schema, ThisSchema};

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
    for maybe_row in reader {
        let row = maybe_row?;
        xs.push(row);
    }
    Ok(xs)
}

fn read_parquet_v2(path: &str) -> Result<Vec<Row>, AppError> {
    let f = File::open(path)?;
    let reader = SerializedFileReader::new(f)?;

    let schema = parquet_metadata_to_arrow_schema(reader.metadata());

    // Iterate through row groups
    for row_group_index in 0..reader.num_row_groups() {
        let row_group_reader = reader.get_row_group(row_group_index)?;
        let num_rows = row_group_reader.metadata().num_rows() as usize;
        let mut columns: Vec<ArrayRef> = Vec::new();
        // Read each column in the row group
        for i in 0..row_group_reader.num_columns() {
            let mut column_reader = row_group_reader.get_column_reader(i)?;
            match column_reader {
                ColumnReader::ByteArrayColumnReader(ref mut r) => {
                    let mut values = Vec::with_capacity(num_rows);
                    let mut def_levels = Vec::with_capacity(num_rows);
                    let (num_read, _, _) = r.read_records(num_rows, Some(&mut def_levels), None, &mut values)?;
                    let array = StringArray::from_iter(values.into_iter().take(num_read).map(|v| Some(String::from_utf8_lossy(v.data()).into_owned())));
                    println!("array: {:?}", array);
                    columns.push(Arc::new(array));
                },
                ColumnReader::Int64ColumnReader(ref mut r) => {
                    let list_array = create_int64_list_array2(r);
                    columns.push(Arc::new(list_array) as ArrayRef);
                },
                ColumnReader::DoubleColumnReader(ref mut r) => {
                    let list_array = create_f64_list_array2(r);
                    println!("create_f64_list_array2: {:?}", list_array);
                    columns.push(Arc::new(list_array) as ArrayRef);
                },
                _ => unreachable!("Unexpected column type"),
            }
        }

        for c in &columns {
            println!("{}: {}", c.data_type(), c.len())
        }

        // Create RecordBatch for this row group
        let batch = arrow::record_batch::RecordBatch::try_new(Arc::new(schema.clone()), columns)?;

        // Process the batch (here we just print it, but you can do more)
        println!("Row Group {}: {:?}", row_group_index, batch);
    }

    let mut xs: Vec<Row> = Vec::new();
    Ok(xs)
}

fn create_int64_list_array2(reader: &mut ColumnReaderImpl<Int64Type>) -> ListArray {
    // Buffers for reading data
    let mut values = vec![0i64; 4351]; // Buffer for actual values
    let mut def_levels = vec![0i16; 4351]; // Buffer for definition levels
    let mut rep_levels = vec![0i16; 4351]; // Buffer for repetition levels

    // Arrow ListBuilder to construct the ListArray
    let mut list_builder = ListBuilder::new(Int64Array::builder(4351));

    // Read batches of data
    while let Ok((value_count, level_count)) = reader.read_batch(
        4351, // Maximum number of values to read in one batch
        Some(&mut def_levels), // Read definition levels
        Some(&mut rep_levels), // Read repetition levels
        &mut values,           // Read actual values
    ) {
        if value_count == 0 {
            break; // No more values to read
        }

        let mut value_index = 0; // Index in the `values` array
        for i in 0..level_count {
            if rep_levels[i] == 0 {
                // Start a new list
                list_builder.append(true);
            }

            if def_levels[i] == 0 {
                // Null value in the list
                list_builder.values().append_null();
            } else {
                // Valid value in the list
                list_builder
                    .values()
                    .append_value(values[value_index]);
                value_index += 1; // Move to the next value in the `values` array
            }
        }
    }

    // Finish building the ListArray
    list_builder.finish()
}

fn create_f64_list_array2(reader: &mut ColumnReaderImpl<DoubleType>) -> ListArray {
    // Buffers for reading data
    let mut values = vec![0f64; 4351]; // Buffer for actual values
    let mut def_levels = vec![0i16; 4351]; // Buffer for definition levels
    let mut rep_levels = vec![0i16; 4351]; // Buffer for repetition levels

    // Arrow ListBuilder to construct the ListArray
    let mut list_builder = ListBuilder::new(Float64Array::builder(4351));

    // Read batches of data
    while let Ok((value_count, level_count)) = reader.read_batch(
        4351, // Maximum number of values to read in one batch
        Some(&mut def_levels), // Read definition levels
        Some(&mut rep_levels), // Read repetition levels
        &mut values,           // Read actual values
    ) {
        if value_count == 0 {
            break; // No more values to read
        }

        let mut value_index = 0; // Index in the `values` array
        for i in 0..level_count {
            if rep_levels[i] == 0 {
                // Start a new list
                list_builder.append(true);
            }

            if def_levels[i] == 0 {
                // Null value in the list
                list_builder.values().append_null();
            } else {
                // Valid value in the list
                list_builder
                    .values()
                    .append_value(values[value_index]);
                value_index += 1; // Move to the next value in the `values` array
            }
        }
    }

    // Finish building the ListArray
    list_builder.finish()
}


fn calculate_list_offsets(rep_levels: &[i16], num_read: usize) -> Vec<i32> {
    let mut offsets = Vec::with_capacity(num_read + 1);
    offsets.push(0);
    let mut current_offset = 0;
    for &level in rep_levels.iter().take(num_read) {
        if level == 0 {
            offsets.push(current_offset);
        }
        current_offset += 1;
    }
    if offsets.len() <= num_read {
        offsets.push(current_offset);
    }
    offsets
}