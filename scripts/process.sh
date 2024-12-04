#!/bin/bash

set -o errexit -o pipefail -o noclobber -o nounset

#!/bin/bash

# Function to display usage
usage() {
  echo "Usage: $0 <input_folder> <output_folder>"
  echo "  input_folder   Path to the folder containing input JSON files"
  echo "  output_folder  Path to the folder where output Parquet files will be saved"
  exit 1
}

# Check if the correct number of arguments is provided
if [ "$#" -ne 2 ]; then
  echo "Error: Invalid number of arguments."
  usage
fi

# Assign arguments to variables
INPUT_FOLDER="$1"
OUTPUT_FOLDER="$2"

# Check if the input folder exists
if [ ! -d "$INPUT_FOLDER" ]; then
  echo "Error: Input folder '$INPUT_FOLDER' does not exist."
  exit 1
fi

# Check if the output folder exists, create it if it doesn't
if [ ! -d "$OUTPUT_FOLDER" ]; then
  echo "Output folder '$OUTPUT_FOLDER' does not exist. Creating it..."
  mkdir -p "$OUTPUT_FOLDER"
  if [ $? -ne 0 ]; then
    echo "Error: Failed to create output folder '$OUTPUT_FOLDER'."
    exit 1
  fi
fi

# Loop through each file in the input folder
for FILE in "$INPUT_FOLDER"/*; do
  # Check if it's a file
  if [ -f "$FILE" ]; then
    # Extract the base name of the file (e.g., "file.json")
    BASENAME=$(basename "$FILE")
    # Construct the output file path in the output folder
    OUTPUT_FILE="$OUTPUT_FOLDER/${BASENAME%.*}.parquet"
    echo "Processing file: $FILE"
    echo "Output file: $OUTPUT_FILE"
    # Execute the program with the input and output file paths
    ./target/release/parquet-playground-rs --input-json-file-path "$FILE" --output-parquet-file-path "$OUTPUT_FILE" --statistics-mode chunk
    if [ $? -ne 0 ]; then
      echo "Error: Failed to process file '$FILE'."
    else
      echo "Successfully processed file '$FILE'."
    fi
  fi
done

echo "All files processed."