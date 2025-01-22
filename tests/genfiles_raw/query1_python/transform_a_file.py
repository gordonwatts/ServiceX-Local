import sys
import os

# Check if the correct number of arguments is provided
if len(sys.argv) != 4:
    print("Usage: {} <input_file> <output_file> <output_format>".format(sys.argv[0]))
    sys.exit(1)

# Assign arguments to variables
input_file = sys.argv[1]
output_file = sys.argv[2]
output_format = sys.argv[3]

# Your script logic here
print("Output format: {}".format(output_format))

# Check if the input file exists
if not input_file.startswith("root"):
    if not os.path.isfile(input_file):
        print("Input file not found!")
        print("-->   {}".format(input_file))
        sys.exit(1)

# If the output file exists, fail.
if os.path.isfile(output_file):
    print("Output file already exists!")
    sys.exit(1)

# Touch the output file so it is created.
with open(output_file, "w") as f:
    pass

sys.exit(0)
