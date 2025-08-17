"""
Converts a CSV file to a YAML file.

This script reads a CSV file, extracts a specific column, and writes the
values to a YAML file as a list of strings.
"""

import csv

from ruamel.yaml import YAML

yaml = YAML()

# --- Settings ---
csv_file = "../request/GCS_geoMIP_Reflective_iids_G6sulfur.csv"  # Path to your CSV file
column_name = "name"  # Column to extract
yaml_file = "output.yaml"  # Output YAML file path

id_list = []

with open(csv_file, newline="") as csvfile:
    reader = csv.reader(csvfile)

    for row in reader:
        id_list.append(row[0])  # Assuming the first column contains the IDs

# print(id_list)
print(f"Number of IDs: {len(id_list)}")

# --- Dump as YAML list of strings ---
with open(yaml_file, "w", encoding="utf-8") as f:
    yaml.dump(id_list, f)
