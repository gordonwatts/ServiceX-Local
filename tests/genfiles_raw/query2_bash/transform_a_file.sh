#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <input_file> <output_file> <output_format>"
    exit 1
fi

# Assign arguments to variables
input_file=$1
output_file=$2
output_format=$3

# Your script logic here\
echo "Output format: $output_format"

# Check if the input file exists
if [[ "$input_file" != root* && "$input_file" != http* ]]; then
    if [ ! -f "$input_file" ]; then
        echo "Input file not found!"
        echo "-->   $input_file"
        exit 1
    fi
fi

# If the output file exists, fail.
if [ -f "$output_file" ]; then
    echo "Output file already exists!"
    exit 1
fi

# Touch the output file so it is created.
touch "$output_file"

exit 0
