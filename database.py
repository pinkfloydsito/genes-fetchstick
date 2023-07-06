import time
import requests
import csv
import re

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session
from models.gene import Gene



def get_session():
    return ScopedSession()

engine = create_engine("sqlite:///genes.db", echo=True)

Session = sessionmaker(bind=engine)

ScopedSession = scoped_session(Session)

Base = declarative_base()

def create_tables():
    Base.metadata.create_all(engine)

    
def save_initial_data():
    NAME_POSITION = 9
    session = get_session()
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

def get_gene_data_from_ncbi_and_persist(gene):
    url = "https://www.ncbi.nlm.nih.gov/gene/"
    params = { "term": gene.name }
    response = requests.get(url, params=params)
    body = response.text

    if response.status_code == 200:
        session = get_session()
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

def persist_ortho_db_html_from_gene(gene, html):
    session = get_session()

    session.query(Gene).filter_by(name=gene.name).update({"orthodb_html": html})
    session.commit()
    session.flush()


def persist_ortho_db_data_in_gene(gene, data):
    session = get_session()
    session.query(Gene).filter_by(name=gene.name).update({"orthodb_data": data})
    session.commit()
    session.flush()


def get_orthodb_html_from_gene(gene):
    url = "https://www.orthodb.org/" + "?" + "ncbi="+gene.gene_id
    body = get_lazy_data_from_orthodb(gene, url)
    
    return body

def get_lazy_data_from_orthodb(gene: Gene, url : str, driver: webdriver.ChromeOptions):
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

def get_genes():
    session = get_session()

    return session.query(Gene).all()

def find_by_name(name: str):
    session = get_session()

    try:
        record = session.query(Gene).filter_by(name=name.strip()).one()
    except:
        print("Could not find record", name)
        return None

    return record

def update_gene_id_in_csv(df, csv_file):
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

def update_orthodb_csv(df, csv_file):
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