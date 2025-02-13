import os
from fastapi import FastAPI, Query, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from app.config import engine, Base, SessionLocal
from app.models import PDFFile
from app.utils import backfill_document_dates, populate_database_from_pdfs
from fastapi.middleware.cors import CORSMiddleware
from urllib.parse import unquote
from sqlalchemy import and_, or_

# Initialize FastAPI app
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins (only for development)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.on_event("startup")
def startup_event():
    """
    Start-up event to create database tables and populate data.
    """
    # Create tables
    Base.metadata.create_all(bind=engine)
    print("Database tables created successfully!")

    # Populate the database with PDFs (adjust the directory path as needed)
    pdf_directory = "./documents"  # Replace with the actual path to your PDFs
    populate_database_from_pdfs(pdf_directory)

    print("Running backfill...")
    backfill_document_dates()
    print("Backfill complete!")

@app.get("/")
def read_root():
    return {"message": "Welcome to the University PDF Search Engine"}

from sqlalchemy import or_
from sqlalchemy.sql.expression import true
@app.get("/search")
def search_pdfs(
    query: str = Query(None, description="Search term for PDFs"),
    exact_match: bool = Query(False, description="Search for exact matches"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(10, ge=1, le=100, description="Number of results per page"),
    start_date: str = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(None, description="End date (YYYY-MM-DD)"),
    db: Session = Depends(get_db)
):
    """
    Search PDFs with prioritized results: exact match, first term, and subsequent terms.
    """
    # Function to apply date filters
    def apply_date_filters(query_filter):
        if start_date:
            query_filter &= PDFFile.document_date >= start_date
        if end_date:
            query_filter &= PDFFile.document_date <= end_date
        return query_filter
    
    # Si no hay query, solo buscar por fechas
    if not query:
        date_filter = apply_date_filters(true())  # Comenzamos con True para no afectar el filtro
        date_results = db.query(PDFFile).filter(date_filter).all()

        # Paginación
        total_results = len(date_results)
        paginated_results = date_results[(page - 1) * page_size: page * page_size]

        return {
            "page": page,
            "page_size": page_size,
            "total_results": total_results,
            "results": [
                {
                    "file_name": pdf.file_name,
                    "file_path": pdf.file_path,
                    "snippet": "",  # No hay snippet si no hay query
                }
                for pdf in paginated_results
            ],
        }

    # Split the query into terms
    terms = query.split()

    

    # Step 1: Exact Phrase Search
    if exact_match:
        exact_filter = or_(
            PDFFile.content == query,
            PDFFile.original_content == query
        )
    else:
        exact_filter = or_(
            PDFFile.content.ilike(f"%{query}%"),
            PDFFile.original_content.ilike(f"%{query}%")
        )
    exact_filter = apply_date_filters(exact_filter)
    exact_results = db.query(PDFFile).filter(exact_filter).all()

    # Step 2: First Term Search
    first_term_filter = or_(
        PDFFile.content.ilike(f"%{terms[0]}%"),
        PDFFile.original_content.ilike(f"%{terms[0]}%")
    )
    first_term_filter = apply_date_filters(first_term_filter)
    first_term_results = db.query(PDFFile).filter(first_term_filter).all()

    # Step 3: Subsequent Terms Search (if there are more than one term)
    subsequent_terms_results = []
    if len(terms) > 1:
        subsequent_terms_filter = or_(
            or_(
                PDFFile.content.ilike(f"%{term}%"),
                PDFFile.original_content.ilike(f"%{term}%")
            )
            for term in terms[1:]
        )
        subsequent_terms_filter = apply_date_filters(subsequent_terms_filter)
        subsequent_terms_results = db.query(PDFFile).filter(subsequent_terms_filter).all()

    # Combine results while maintaining priority and avoiding duplicates
    unique_results = {pdf.id: pdf for pdf in exact_results}  # Use a dict to deduplicate
    for pdf in first_term_results:
        unique_results.setdefault(pdf.id, pdf)
    for pdf in subsequent_terms_results:
        unique_results.setdefault(pdf.id, pdf)

    # Convert results back to a list and paginate
    all_results = list(unique_results.values())
    total_results = len(all_results)
    paginated_results = all_results[(page - 1) * page_size: page * page_size]

    # Generate snippets
    def get_snippet(pdf: PDFFile, terms: list[str]) -> str:
        """
        Get a snippet containing query terms from the content or original_content.
        """
        def find_snippet(source: str, terms: list[str]) -> str:
            for term in terms:
                index = source.lower().find(term.lower())
                if index != -1:
                    start = max(index - 50, 0)
                    end = min(index + 50, len(source))
                    return source[start:end]
            return None

        # Try content first
        snippet = find_snippet(pdf.content, terms)
        if snippet:
            return snippet

        # Fallback to original_content
        snippet = find_snippet(pdf.original_content, terms)
        if snippet:
            return snippet

        return ""  # No snippet found

    return {
        "page": page,
        "page_size": page_size,
        "total_results": total_results,
        "results": [
            {
                "file_name": pdf.file_name,
                "file_path": pdf.file_path,
                "snippet": get_snippet(pdf, terms),
            }
            for pdf in paginated_results
        ],
    }






@app.get("/download/{file_name}", response_class=FileResponse)
def download_file(file_name: str):
    """
    Endpoint to download a file by name.
    """
    decoded_file_name = unquote(file_name)
    file_path = os.path.join("./documents", decoded_file_name)

    if not os.path.exists(file_path):
        print(f"DEBUG: File not found at {file_path}")  # Log the file path for debugging
        raise HTTPException(status_code=404, detail="File not found")

    print(f"DEBUG: Serving file {file_path}")  # Log successful file path
    return FileResponse(file_path, media_type="application/pdf", filename=decoded_file_name)

