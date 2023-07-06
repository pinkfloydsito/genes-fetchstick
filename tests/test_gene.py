import pytest
from sqlalchemy import create_engine

from models.gene import Gene

def test_gene_model(dbsession):
    # Create a test gene
    gene = Gene(name="Gene1", gene_id="12345", aliases="Alias1, Alias2")

    # Add the gene to the session
    dbsession.add(gene)
    dbsession.commit()

    # Retrieve the gene from the session
    retrieved_gene = dbsession.query(Gene).filter_by(name="Gene1").first()

    # Assert that the retrieved gene matches the original gene
    assert retrieved_gene.name == "Gene1"
    assert retrieved_gene.gene_id == "12345"
    assert retrieved_gene.aliases == "Alias1, Alias2"

    # Update the gene
    retrieved_gene.name = "UpdatedGene"
    dbsession.commit()

    # Retrieve the updated gene from the session
    updated_gene = dbsession.query(Gene).filter_by(name="UpdatedGene").first()

    # Assert that the updated gene has the new name
    assert updated_gene.name == "UpdatedGene"

    # Delete the gene
    dbsession.delete(updated_gene)
    dbsession.commit()

    # Verify that the gene has been deleted
    deleted_gene = dbsession.query(Gene).filter_by(name="UpdatedGene").first()
    assert deleted_gene is None