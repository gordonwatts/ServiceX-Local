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

# Touch the output file so it is created.
touch "$output_file"

# Now lets dump lots of stuff to stderr and stdout
echo "This is a test message to stderr" 1>&2
sleep 0.1
echo "This is a test message to stderr" 1>&2
sleep 0.1
echo "This is a test message to stderr" 1>&2
sleep 0.1
echo "This is a test message to stderr" 1>&2
sleep 0.1
echo "This is a test message to stderr" 1>&2
sleep 0.1
echo "This is a test message to stdout"
sleep 0.1
echo "This is a test message to stdout"
sleep 0.1
echo "This is a test message to stdout"
sleep 0.1
echo "This is a test message to stdout"
sleep 0.1
echo "This is a test message to stdout"
sleep 0.1
echo "This is a test message to stderr" 1>&2
sleep 0.1
echo "This is a test message to stderr" 1>&2
sleep 0.1
echo "This is a test message to stderr" 1>&2
sleep 0.1
echo "This is a test message to stderr" 1>&2
sleep 0.1
echo "This is a test message to stderr" 1>&2
sleep 0.1
echo "This is a test message to stderr" 1>&2
sleep 0.1
echo "This is a test message to stdout"


exit 0
