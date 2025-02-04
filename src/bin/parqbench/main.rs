#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(target_env = "msvc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

use arrow::array::{
    Array, ArrowPrimitiveType, AsArray, BinaryArray, GenericStringArray, ListArray, PrimitiveArray,
};
use arrow::datatypes::Int64Type;
use arrow::datatypes::{ArrowNativeType, DataType, Float64Type};
use clap::{Parser, ValueEnum};
use parquet::column::reader::get_typed_column_reader;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Field, Row};
use parquet_playground_rs::errors::AppError;
use parquet_playground_rs::flatbuffers::from_flatbuffers;
use parquet_playground_rs::flatbuffers::histogram_flatbuffers::Histogram;
use parquet_playground_rs::schema::parquet_metadata_to_arrow_schema;
use parquet_playground_rs::vec_pq_reader::{
    read_binary_column, read_f64_column, read_i64_column, read_string_column,
};
use parquet_playground_rs::{errors, logger};
use std::fs::File;
use std::time::Instant;
use tracing::info;

#[derive(ValueEnum, Clone, Debug)]
pub enum ParquetReaderType {
    /// Use RowByRow Parquet reader, `RowIter`
    RowByRow,
    /// Use `GenericColumnReader` to read the whole column
    Columnar,
}

#[derive(Parser, Debug, Clone)]
#[clap()]
struct AppArgs {
    /// Input path to Parquet
    #[clap(long)]
    input_parquet_file_path: String,
    /// The type of Parquet reader
    #[clap(long)]
    parquet_reader_type: ParquetReaderType,
    /// Flag to control whether it should use read data from Parquet
    #[clap(long)]
    use_data: bool,
    /// Number of iterations to run the benchmark
    #[clap(long, default_value_t = 100)]
    iterations: usize,
}

fn touch_primitive_array<T: ArrowPrimitiveType>(arr: &PrimitiveArray<T>) -> usize {
    let mut total: usize = 0;
    for i in 0..arr.len() {
        if arr.is_null(i) {
            total += 1;
        } else {
            let v = arr.value(i);
            total += v.as_usize();
        }
    }
    total
}

fn touch_string_array(arr: &GenericStringArray<i32>) -> usize {
    let mut total: usize = 0;
    for i in 0..arr.len() {
        if arr.is_null(i) {
            total += 1;
        } else {
            let v = arr.value(i);
            total += v.len();
        }
    }
    total
}

fn touch_histogam(hist: Histogram) -> usize {
    let mut total: usize = 0;
    for x in hist.ts().unwrap() {
        total += x as usize;
    }
    for x in hist.sums_long().unwrap() {
        total += x as usize;
    }
    for x in hist.sums_double().unwrap() {
        total += x as usize;
    }
    for x in hist.count().unwrap() {
        total += x as usize;
    }
    total
}

fn touch_list_array(arr: &ListArray) -> usize {
    let mut total: usize = 0;
    for i in 0..arr.len() {
        if arr.is_null(i) {
            total += 1;
        } else {
            total += touch_array(&arr.value(i));
        }
    }
    total
}

fn touch_array(array: &dyn Array) -> usize {
    let mut total: usize = 0;

    match array.data_type() {
        DataType::Null => {
            total += 1;
        }
        DataType::Int64 => {
            let arr: &PrimitiveArray<Int64Type> = array.as_primitive();
            total += touch_primitive_array(arr);
        }
        DataType::Float64 => {
            let arr: &PrimitiveArray<Float64Type> = array.as_primitive();
            total += touch_primitive_array(arr);
        }
        DataType::Utf8 => {
            let arr: &GenericStringArray<i32> = array.as_string();
            total += touch_string_array(arr);
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    total += 1;
                } else {
                    let buf = arr.value(i);
                    // Let's convert BYTE_ARRAY to actual FlatBuffer object and traverse it
                    let hist: Histogram = from_flatbuffers(buf);
                    total += touch_histogam(hist);
                }
            }
        }
        DataType::List(list) => {
            list.as_ref();
            match list.data_type() {
                DataType::Null => {
                    total += 1;
                }
                DataType::Int64 => {
                    let arr = array.as_any().downcast_ref::<ListArray>().unwrap();
                    total += touch_list_array(arr);
                }
                DataType::Float64 => {
                    let arr = array.as_any().downcast_ref::<ListArray>().unwrap();
                    total += touch_list_array(arr);
                }
                DataType::Utf8 => {
                    let arr = array.as_any().downcast_ref::<ListArray>().unwrap();
                    total += touch_list_array(arr);
                }
                x => panic!("Unexpected data type of List {:?}", x),
            }
        }
        x => panic!("Unexpected data type {:?}", x),
    }
    total
}

fn touch_row(row: &Row) -> usize {
    let mut total: usize = 0;
    for (_, field) in row.get_column_iter() {
        match field {
            Field::Null => {
                total += 1;
            }
            Field::Long(v) => {
                total += *v as usize;
            }
            Field::Double(v) => {
                total += *v as usize;
            }
            Field::Str(str) => {
                total += str.len();
            }
            Field::Bytes(bytes) => {
                let hist = from_flatbuffers(bytes.data());
                total += touch_histogam(hist);
            }
            Field::ListInternal(list) => {
                for e in list.elements() {
                    match e {
                        Field::Null => {
                            total += 1;
                        }
                        Field::Long(v) => {
                            total += *v as usize;
                        }
                        Field::Double(v) => {
                            total += *v as usize;
                        }
                        Field::Str(str) => {
                            total += str.len();
                        }
                        x => panic!("Unexpected data type of ListInternal {:?}", x),
                    }
                }
            }
            x => panic!("Unexpected data type {:?}", x),
        }
    }
    total
}

fn main() -> errors::Result<()> {
    logger::setup("parqbench", "INFO");

    let args = AppArgs::parse();
    info!("Received args: {:?}", args);

    let t: Instant = Instant::now();
    let mut counter: usize = 0;
    for _ in 0..args.iterations {
        match args.parquet_reader_type {
            ParquetReaderType::RowByRow => {
                let xs = read_parquet(args.input_parquet_file_path.as_str())?;
                if args.use_data {
                    for row in &xs {
                        counter += touch_row(row);
                    }
                } else {
                    counter += xs.len();
                }
            }
            ParquetReaderType::Columnar => {
                let row_group_arrays = read_parquet_v2(args.input_parquet_file_path.as_str())?;
                if args.use_data {
                    for row_group in &row_group_arrays {
                        for array in row_group {
                            counter += touch_array(array.as_ref());
                        }
                    }
                } else {
                    // The first column is not a List, so we use it to calculate the total number of rows
                    for row_group in &row_group_arrays {
                        for array in row_group {
                            counter += array.len();
                        }
                    }
                }
            }
        };
    }
    let total_elapsed_ms = t.elapsed().as_millis() as f64;
    let avg = total_elapsed_ms / args.iterations as f64;
    info!(
        "Total time is {total_elapsed_ms:.3} ms, average reading time is {avg:.3} ms. Counter is {}", counter
    );

    Ok(())
}

fn read_parquet(path: &str) -> Result<Vec<Row>, AppError> {
    let f = File::open(path)?;
    let reader = SerializedFileReader::new(f)?;
    let mut xs: Vec<Row> = Vec::new();
    let iter = reader.into_iter();
    for maybe_row in iter {
        let row = maybe_row?;
        xs.push(row);
    }
    Ok(xs)
}

fn read_parquet_v2(path: &str) -> Result<Vec<Vec<Box<dyn Array>>>, AppError> {
    let f = File::open(path)?;
    let reader = SerializedFileReader::new(f)?;
    let mut result: Vec<Vec<Box<dyn Array>>> = Vec::new();
    let schema = parquet_metadata_to_arrow_schema(reader.metadata());
    for row_group in 0..reader.num_row_groups() {
        let rg = reader.get_row_group(row_group)?;
        const BATCH_SIZE: usize = 10000;
        let mut row_group_result: Vec<Box<dyn Array>> = Vec::new();
        for (col_idx, c) in schema.fields.iter().enumerate() {
            let col_rdr = rg.get_column_reader(col_idx)?;
            let col_desc = rg.metadata().column(col_idx).column_descr();
            match c.data_type() {
                DataType::Int64 => {
                    let col_rdr = get_typed_column_reader::<parquet::data_type::Int64Type>(col_rdr);
                    let arr = read_i64_column(col_rdr, col_desc, BATCH_SIZE)?;
                    row_group_result.push(arr);
                }
                DataType::Float64 => {
                    let col_rdr =
                        get_typed_column_reader::<parquet::data_type::DoubleType>(col_rdr);
                    let arr = read_f64_column(col_rdr, col_desc, BATCH_SIZE)?;
                    row_group_result.push(arr);
                }
                DataType::Utf8 => {
                    let col_rdr =
                        get_typed_column_reader::<parquet::data_type::ByteArrayType>(col_rdr);
                    let arr = read_string_column(col_rdr, col_desc, BATCH_SIZE)?;
                    row_group_result.push(arr);
                }
                DataType::Binary => {
                    let col_rdr =
                        get_typed_column_reader::<parquet::data_type::ByteArrayType>(col_rdr);
                    let arr = read_binary_column(col_rdr, col_desc, BATCH_SIZE)?;
                    row_group_result.push(arr);
                }
                DataType::List(field) => match field.data_type() {
                    DataType::Int64 => {
                        let col_rdr =
                            get_typed_column_reader::<parquet::data_type::Int64Type>(col_rdr);
                        let arr = read_i64_column(col_rdr, col_desc, BATCH_SIZE)?;
                        row_group_result.push(arr);
                    }
                    DataType::Float64 => {
                        let col_rdr =
                            get_typed_column_reader::<parquet::data_type::DoubleType>(col_rdr);
                        let arr = read_f64_column(col_rdr, col_desc, BATCH_SIZE)?;
                        row_group_result.push(arr);
                    }
                    x => panic!("{:?}", x),
                },
                x => panic!("{:?}", x),
            }
        }
        result.push(row_group_result);
    }
    Ok(result)
}
