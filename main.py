import pandas as pd
import csv
import sqlalchemy as db
import requests
import time
import yaml

import re
from selenium import webdriver
from .database import create_tables, get_genes, update_orthodb_csv

NAME_POSITION = 9

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless")
chrome_options.add_argument("window-size=1920x1080")
driver = webdriver.Chrome(options=chrome_options)


def string_to_yaml(string):
    # Clean the input string
    cleaned_string = string.strip()

    # Split the cleaned string into lines
    lines = cleaned_string.split("\n")

    # Create a dictionary from the lines
    data = {}

    # Split the cleaned string into lines
    current_key = None
    for line in lines:
        if line.startswith(" "):
            # Ignore leading spaces in the line
            line = line.strip()

        if line.endswith(":"):
            # Remove the trailing colon from the key
            current_key = line[:-1]
            data[current_key] = ""
        elif current_key is not None:
            # Append the line to the value of the current key
            if data[current_key]:
                data[current_key] += " "
            data[current_key] += line

    return data


create_tables()

genes = get_genes()
# Step 1

# for gene in genes:
#     if gene.html is None:
#         get_gene_data_from_ncbi(engine, gene)

# Step 2

# persist_gene_ids(genes)

# Step 3
# for gene in genes:
#     if gene.gene_id is not None and gene.orthodb_html is None:
#         html = get_orthodb_html_from_gene(gene)
#         persist_ortho_db_html_from_gene(gene, html)

# Step 4
# for gene in genes:
#     if gene.orthodb_data is None and gene.orthodb_html is not None:
#         data = get_orthodb_attributes_from_gene(gene)

#         if data is not None:
#             persist_ortho_db_data_in_gene(gene, data)
#         else:
#             print("Could not find gene orthodb data", gene.name)
driver.close()


def remove_empty_columns(csv_file):
    # Read CSV file using pandas
    df = pd.read_csv(csv_file)

    # Remove empty columns
    df = df.dropna(axis=1, how="all")

    # Write updated data back to CSV file
    df.to_csv(csv_file, index=False)


def concatenate_fields(csv_file):
    # Read CSV file using pandas
    df = pd.read_csv(csv_file, delimiter=";")

    # Concatenate fields without a column name into 'aliases' column
    df["aliases"] = df[df.columns[df.columns.str.contains("Unnamed")]].apply(
        lambda x: "; ".join(x.dropna().astype(str)), axis=1
    )

    # Remove columns without a column name
    df = df.drop(df.columns[df.columns.str.contains("Unnamed")], axis=1)

    # Write updated data back to CSV file
    df.to_csv(csv_file, index=False)


# Example usage
csv_file = "data/data.csv"  # Path to your CSV file
# concatenate_fields(csv_file)


def list_unnamed_columns(csv_file):
    # Read CSV file using pandas
    df = pd.read_csv(csv_file)

    # Get unnamed columns
    unnamed_columns = df.columns[df.columns.str.contains("Unnamed")].tolist()

    return unnamed_columns


def remove_and_strip_records(csv_file):
    # Read CSV file using pandas with semicolon delimiter
    df = pd.read_csv(csv_file, delimiter=";")

    # Remove specific strings and strip records
    df = df.applymap(
        lambda x: x.strip().replace(".; .; ", "").replace(".;", "")
        if isinstance(x, str)
        else x
    )

    # Replace '.' values with empty string
    df = df.replace(".", "")

    # Write updated data back to CSV file
    df.to_csv(csv_file, index=False)


def append_aliases_as_rows(csv_file):
    # Read CSV file using pandas
    df = pd.read_csv(csv_file)

    # Split aliases column and create new rows
    df["Name"] = df["aliases"].str.split("; ")
    df = df.explode("Name")

    # Remove duplicates and reset index
    df = df.drop_duplicates().reset_index(drop=True)

    # Write updated data back to CSV file
    df.to_csv(csv_file, index=False)


# append_aliases_as_rows(csv_file)

# # Read the CSV file into a DataFrame
# df = pd.read_csv('data/data.csv')

# # Update the geneid column
# updated_df = update_gene_id_in_csv(df)

# # Save the updated DataFrame to a new CSV file
# updated_df.to_csv('data/data.csv', index=False)


def expand_aliases(df):
    new_rows = []  # List to store new rows

    for index, row in df.iterrows():
        aliases = row["aliases"]

        if pd.isna(aliases):
            continue

        # Split aliases by ';' and iterate over the extracted records
        for alias in aliases.split(";"):
            alias = alias.strip()  # Remove leading/trailing whitespace

            # Create a new row by copying existing values and updating the 'aliases' and 'Name' columns
            new_row = row.copy()
            new_row["aliases"] = aliases
            new_row["Name"] = alias

            # Append the new row to the list
            new_rows.append(new_row)

    # Append the new rows to the existing DataFrame
    expanded_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

    return expanded_df


df = pd.read_csv(csv_file)

# expanded_df = expand_aliases(df)
# expanded_df.to_csv('data/data.csv', index=False)


# Count duplicates based on a specific column
# duplicate_counts = df['Name'].duplicated().value_counts()

# # Print the duplicate counts
# print(duplicate_counts)
# df_unique = df.drop_duplicates(subset='Name')

# df_unique.to_csv('data/data.csv', index=False)


update_orthodb_csv(df, csv_file)
# expanded_df.to_csv('data/data_updated.csv', index=False)
