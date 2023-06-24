import csv
import sqlalchemy as db
import requests
import time
import yaml

import re
from selenium import webdriver
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy import String, Text
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Session, Query
from sqlalchemy.orm import sessionmaker

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

NAME_POSITION = 9

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('window-size=1920x1080');
driver = webdriver.Chrome(options=chrome_options)

class Base(DeclarativeBase):
    pass

class Gene(Base):
    __tablename__ = "gene"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30), unique=True)
    gene_id: Mapped[str] = mapped_column(String(30), nullable=True, default=None)
    aliases: Mapped[str] = mapped_column(String(500), nullable=True, default=None)
    html: Mapped[str] = mapped_column(Text, nullable=True, default=None)
    orthodb_html: Mapped[str] = mapped_column(Text, nullable=True, default=None)
    orthodb_data: Mapped[str] = mapped_column(Text, nullable=True, default=None)

    def __repr__(self) -> str:
        return f"Gene(id={self.id!r}, name={self.name!r}, gene_id={self.gene_id!r}, aliases={self.aliases!r})"

engine = create_engine("sqlite:///genes.db", echo=True)
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()


def save_initial_data(engine):
    with Session(engine) as session:
        with open('data/data.csv', newline='') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=';', quotechar='|')
            for row in spamreader:
                name=row[NAME_POSITION]
                if name != 'Name':
                    for i in range(NAME_POSITION, len(row)):
                        if row[i] != '':
                            record = Gene(name=row[i])
                            q_obj = session.query(Gene).where(Gene.name == record.name)

                            if q_obj.first() is None:
                                session.add(record)
                            session.commit()


def get_gene_data_from_ncbi(engine, gene):
    url = "https://www.ncbi.nlm.nih.gov/gene/"
    params = { "term": gene.name }
    response = requests.get(url, params=params)
    body = response.text

    if response.status_code == 200:
        with Session(engine) as session:
            setattr(gene, 'html', body)
            session.query(Gene).filter_by(name=gene.name).update({"html": body})
            print("Processed", gene.name)
            time.sleep(0.2)
            session.commit()
            session.flush()
    else:
        print("error:", response.status_code, " for:", gene.name)
    
def persist_gene_ids(genes):
    for gene in genes:
        if gene.gene_id is None and gene.html is not None:
            gene_id = get_gene_id_from_ncbi_gene(gene)
            if gene_id is not None:
                with Session(engine) as session:
                    setattr(gene, 'gene_id', gene_id)
                    session.query(Gene).filter_by(name=gene.name).update({"gene_id": gene_id})
                    print("Processed", gene.name)
                    session.commit()
                    session.flush()

def persist_ortho_db_html_from_gene(gene, html):
    session.query(Gene).filter_by(name=gene.name).update({"orthodb_html": html})
    session.commit()
    session.flush()

def persist_ortho_db_data_in_gene(gene, data):
    session.query(Gene).filter_by(name=gene.name).update({"orthodb_data": data})
    session.commit()
    session.flush()

def get_orthodb_html_from_gene(gene):
    url = "https://www.orthodb.org/" + "?" + "ncbi="+gene.gene_id
    body = get_lazy_data_from_orthodb(gene, url)
    
    return body


def get_genes():
    return session.query(Gene).all()

def remove_duplicate_tabs_newlines(string):
    cleaned_string = re.sub(r'[\t\n]+', lambda match: match.group(0)[0], string)
    return cleaned_string

def get_orthodb_attributes_from_gene(gene: Gene):
    soup = BeautifulSoup(gene.orthodb_html)
    try:
        if len(soup.select(".s-group-section")) == 2:
            items = soup.select(".s-group-ortho-annotations")
            target = items[0].get_text()

            return remove_duplicate_tabs_newlines(target)
        else:
            print("length is not 2!", gene.name)
            return None
    except:
        print("Cannot parse html!", gene.name)
        return None

def get_lazy_data_from_orthodb(gene: Gene, url : str):
    try:
     driver.get(url)
     class_name = '.s-group-ortho-toggle'
     WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, class_name))).click()

     time.sleep(2)
     page = driver.page_source
     return page
    except:
        print("Error from orthodb, ", gene.name)
        return None

def get_gene_id_from_ncbi_gene(gene):
    soup = BeautifulSoup(gene.html)
    try:
        gene_id_item = soup.select_one(".geneid")
        if gene_id_item is None:
            gene_id_item = soup.select_one(".gene-id")

        return gene_id_item.get_text().split(",")[0].split(":")[1].strip()
    except:
        print("Cannot parse html!", gene.name)
        return None
    
def string_to_yaml(string):
    # Clean the input string
    cleaned_string = string.strip()

    # Split the cleaned string into lines
    lines = cleaned_string.split('\n')
    
    # Create a dictionary from the lines
    data = {}

    # Split the cleaned string into lines
    current_key = None
    for line in lines:
        if line.startswith(' '):
            # Ignore leading spaces in the line
            line = line.strip()

        if line.endswith(':'):
            # Remove the trailing colon from the key
            current_key = line[:-1]
            data[current_key] = ''
        elif current_key is not None:
            # Append the line to the value of the current key
            if data[current_key]:
                data[current_key] += ' '
            data[current_key] += line

    return data

# save_initial_data(engine)
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


import pandas as pd

def remove_empty_columns(csv_file):
    # Read CSV file using pandas
    df = pd.read_csv(csv_file)

    # Remove empty columns
    df = df.dropna(axis=1, how='all')

    # Write updated data back to CSV file
    df.to_csv(csv_file, index=False)

def concatenate_fields(csv_file):
    # Read CSV file using pandas
    df = pd.read_csv(csv_file, delimiter=';')

    # Concatenate fields without a column name into 'aliases' column
    df['aliases'] = df[df.columns[df.columns.str.contains('Unnamed')]].apply(lambda x: '; '.join(x.dropna().astype(str)), axis=1)

    # Remove columns without a column name
    df = df.drop(df.columns[df.columns.str.contains('Unnamed')], axis=1)

    # Write updated data back to CSV file
    df.to_csv(csv_file, index=False)

# Example usage
csv_file = 'data/data.csv'  # Path to your CSV file
# concatenate_fields(csv_file)

def list_unnamed_columns(csv_file):
    # Read CSV file using pandas
    df = pd.read_csv(csv_file)

    # Get unnamed columns
    unnamed_columns = df.columns[df.columns.str.contains('Unnamed')].tolist()

    return unnamed_columns

def remove_and_strip_records(csv_file):
    # Read CSV file using pandas with semicolon delimiter
    df = pd.read_csv(csv_file, delimiter=';')

    # Remove specific strings and strip records
    df = df.applymap(lambda x: x.strip().replace('.; .; ', '').replace('.;', '') if isinstance(x, str) else x)
    
    # Replace '.' values with empty string
    df = df.replace('.', '')

    # Write updated data back to CSV file
    df.to_csv(csv_file, index=False)

def append_aliases_as_rows(csv_file):
    # Read CSV file using pandas
    df = pd.read_csv(csv_file)

    # Split aliases column and create new rows
    df['Name'] = df['aliases'].str.split('; ')
    df = df.explode('Name')

    # Remove duplicates and reset index
    df = df.drop_duplicates().reset_index(drop=True)

    # Write updated data back to CSV file
    df.to_csv(csv_file, index=False)

# append_aliases_as_rows(csv_file)

def find_by_name(name :str):
    try:
        record = session.query(Gene).filter_by(name=name.strip()).one()
    except:
        print("Could not find record", name)
        return None

    return record

def update_gene_id_in_csv(df):
    # Check if the "geneid" column exists
    if 'gene_id' not in df.columns:
        # Create the "geneid" column with empty values
        df['gene_id'] = ''

    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        name = str(row['Name'])
        gene = find_by_name(name)

        if gene is None:
            print("Could not find gene", name)
            continue
        
        # Perform your logic to update the geneid based on the name value
        # Here, we're simply appending '_updated' to the original geneid
        updated_geneid = gene.gene_id

        # Update the geneid column with the new value
        df.loc[index, 'gene_id'] = updated_geneid

        df.to_csv(csv_file, index=False)

    return df

# # Read the CSV file into a DataFrame
# df = pd.read_csv('data/data.csv')

# # Update the geneid column
# updated_df = update_gene_id_in_csv(df)

# # Save the updated DataFrame to a new CSV file
# updated_df.to_csv('data/data.csv', index=False)

def expand_aliases(df):
    new_rows = []  # List to store new rows
    
    for index, row in df.iterrows():
        aliases = row['aliases']
        
        if pd.isna(aliases):
            continue
        
        # Split aliases by ';' and iterate over the extracted records
        for alias in aliases.split(';'):
            alias = alias.strip()  # Remove leading/trailing whitespace
            
            # Create a new row by copying existing values and updating the 'aliases' and 'Name' columns
            new_row = row.copy()
            new_row['aliases'] = aliases
            new_row['Name'] = alias
            
            # Append the new row to the list
            new_rows.append(new_row)
    
    # Append the new rows to the existing DataFrame
    expanded_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    
    return expanded_df

# df = pd.read_csv(csv_file)

# expanded_df = expand_aliases(df)
# expanded_df.to_csv('data/data.csv', index=False)


# Count duplicates based on a specific column
# duplicate_counts = df['Name'].duplicated().value_counts()

# # Print the duplicate counts
# print(duplicate_counts)
# df_unique = df.drop_duplicates(subset='Name')

# df_unique.to_csv('data/data.csv', index=False)

import re
def update_orthodb_csv(df):

    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        name = str(row['Name'])
        gene = find_by_name(name)

        if gene is None or gene.orthodb_data is None:
            print("Could not find gene", name)
            continue
        
        # Perform your logic to update the geneid based on the name value
        # Here, we're simply appending '_updated' to the original geneid

        # data = string_to_yaml(gene.orthodb_data)
        cleaned_string = gene.orthodb_data.strip()

        # Split the cleaned string into lines
        lines = re.split(r'\t|\n', cleaned_string)

        # Create a dictionary from the lines
        data = {}
        current_key = None
        for line in lines:
            if line.startswith(' '):
                # Ignore leading spaces in the line
                line = line.strip()

            if line.endswith(':'):
                # Remove the trailing colon from the key
                current_key = line[:-1]
                data[current_key] = ''
            elif current_key is not None:
                # Append the line to the value of the current key
                if data[current_key]:
                    data[current_key] += ' '
                data[current_key] += line

        for key, value in data.items():
            if key not in df.columns:
                df[key] = ''

            df.loc[index, key] = value
        df.to_csv(csv_file, index=False)

    return df

df = pd.read_csv(csv_file)

update_orthodb_csv(df)
# expanded_df.to_csv('data/data_updated.csv', index=False)