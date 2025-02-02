Write Parquet in Rust using Apache Arrow
------

# Development

The project requires the following tools configured on your developer machine:

- Rust compiler and Cargo, check https://www.rust-lang.org/tools/install on how to install both

# How to run

## js2pq

```bash
cargo build --release && target/release/js2pq --help
    Finished `release` profile [optimized] target(s) in 0.03s
Usage: js2pq [OPTIONS] --input-json-file-path <INPUT_JSON_FILE_PATH> --output-parquet-file-path <OUTPUT_PARQUET_FILE_PATH> --statistics-mode <STATISTICS_MODE>

Options:
      --input-json-file-path <INPUT_JSON_FILE_PATH>
          Input path to JSON with measurements

      --output-parquet-file-path <OUTPUT_PARQUET_FILE_PATH>
          Output path to Parquet

      --statistics-mode <STATISTICS_MODE>
          Controls statistics for Parquet

          Possible values:
          - none:  Compute no statistics
          - chunk: Compute chunk-level statistics but not page-level
          - page:  Compute page-level and chunk-level statistics

      --hexify-tag-columns

      --use-flatbuffers
          
  -h, --help
          Print help (see a summary with '-h')

```

## parqbench
```bash
cargo build --release && target/release/js2pq --help
    Finished `release` profile [optimized] target(s) in 0.03s
Usage: parqbench [OPTIONS] --input-parquet-file-path <INPUT_PARQUET_FILE_PATH> --parquet-reader-type <PARQUET_READER_TYPE>

Options:
      --input-parquet-file-path <INPUT_PARQUET_FILE_PATH>
          Input path to Parquet

      --parquet-reader-type <PARQUET_READER_TYPE>
          The type of Parquet reader

          Possible values:
          - row-by-row: Use RowByRow Parquet reader, `RowIter`
          - columnar:   Use `GenericColumnReader` to read the whole column

      --use-data
          Flag to control whether it should use read data from Parquet

      --iterations <ITERATIONS>
          Number of iterations to run the benchmark
          
          [default: 100]

  -h, --help
          Print help (see a summary with '-h')

```