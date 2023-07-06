from.base import Base

from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import String, Text

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