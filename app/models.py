from sqlalchemy import Column, Date, Integer, String, Text, DateTime, UniqueConstraint
from sqlalchemy.sql import func
from app.config import Base

class PDFFile(Base):
    __tablename__ = "pdf_files"

    id = Column(Integer, primary_key=True, index=True)
    file_name = Column(String, nullable=False, index=True)
    file_path = Column(String, nullable=False, unique=True)  # Enforce uniqueness
    content = Column(Text, nullable=False)  # Processed content (stemmed/cleaned)
    original_content = Column(Text, nullable=False)  # Raw, unprocessed text
    upload_date = Column(DateTime, server_default=func.now())
    document_date = Column(Date, nullable=True)  # New column for document date


    # Add a unique constraint explicitly (optional if `unique=True` is already used)
    __table_args__ = (UniqueConstraint('file_path', name='_file_path_uc'),)
