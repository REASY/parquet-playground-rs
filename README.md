Write Parquet in Rust using Apache Arrow
------

# Development

The project requires the following tools configured on your developer machine:

- Rust compiler and Cargo, check https://www.rust-lang.org/tools/install on how to install both

# How to run

In the root folder of the repository run the following

```bash
cargo build --release && target/release/js2pq --help
    Finished `release` profile [optimized] target(s) in 0.11s
Usage: js2pq.exe --input-json-file-path <INPUT_JSON_FILE_PATH> --output-parquet-file-path <OUTPUT_PARQUET_FILE_PATH> --statistics-mode <STATISTICS_MODE>

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

  -h, --help
          Print help (see a summary with '-h')
```

