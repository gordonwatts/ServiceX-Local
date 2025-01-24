import sys

print("WARNING: this is log line 1")
print("ERROR: this is log line 2")

# Check if the correct number of arguments is provided
if len(sys.argv) != 4:
    print("Usage: {} <input_file> <output_file> <output_format>".format(sys.argv[0]))
    sys.exit(1)

# Assign arguments to variables
input_file = sys.argv[1]
output_file = sys.argv[2]
output_format = sys.argv[3]

# Touch the output file so it is created.
with open(output_file, "w") as f:
    pass
