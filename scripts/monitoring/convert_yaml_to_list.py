"""Converts a YAML file containing a list to a plain text file."""

#!/usr/bin/env python3

import sys

import yaml


def main():
    """
    Convert a YAML file containing a list to a plain text file with one item per line.

    Usage: convert_yaml_to_list.py input.yaml output.txt
    """
    if len(sys.argv) != 3:
        print("Usage: convert_yaml_to_list.py input.yaml output.txt")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    try:
        with open(input_path, "r") as infile:
            data = yaml.safe_load(infile)
    except Exception as e:
        print(f"Error reading YAML file: {e}")
        sys.exit(1)

    if not isinstance(data, list):
        print("Error: YAML content is not a list")
        sys.exit(1)

    try:
        with open(output_path, "w") as outfile:
            for item in data:
                outfile.write(f"{item}\n")
    except Exception as e:
        print(f"Error writing to output file: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
