import datetime
import locale
import os
from pathlib import Path
import re
from pdfminer.high_level import extract_text
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import SnowballStemmer
from app.config import SessionLocal
from app.models import PDFFile

# Ensure NLTK resources are downloaded
import nltk

nltk.download('punkt')
nltk.download('stopwords')

# Set NLTK stopwords for Spanish
STOPWORDS = set(stopwords.words('spanish'))

# Initialize the Spanish stemmer
SPANISH_STEMMER = SnowballStemmer("spanish")

def extract_date_from_content(content: str) -> datetime.date:
    """
    Extract the date from the content, handling irregular spaces, optional "de",
    and cases where the month name is missing.
    """
    try:
        # Regex to match complete dates with day, month, and year
        match = re.search(r"(\d{1,2})\s+(?:de\s+)?(\w+)?\s+(?:de\s+)?(\d{4})", content)
        if match:
            day, month, year = match.groups()
            if not month:
                # Handle cases where the month is missing (e.g., "1 del 2014")
                month = "enero"  # Default to January or a placeholder
            date_str = f"{day} {month} {year}"
            # Set locale for Spanish month names
            locale.setlocale(locale.LC_TIME, "es_ES.UTF-8")
            # Parse the date
            date_obj = datetime.datetime.strptime(date_str, "%d %B %Y")
            return date_obj.date()  # Return as a date object
    except ValueError as e:
        print(f"Error parsing date '{date_str}': {e}")
    except locale.Error:
        print("Error: Ensure your system supports Spanish locale.")

    # Return None if no valid date is found
    print("No valid date found in content.")
    return None





def preprocess_text(text: str) -> str:
    """
    Preprocess the text by removing Spanish stopwords, stemming, and unnecessary characters.
    :param text: The raw text to preprocess.
    :return: Preprocessed text.
    """
    try:
        # Tokenize the text into words
        tokens = word_tokenize(text)

        # Remove stopwords and non-alphanumeric tokens
        filtered_tokens = [word for word in tokens if word.lower() not in STOPWORDS and word.isalnum()]

        # Apply stemming using the Spanish stemmer
        stemmed_tokens = [SPANISH_STEMMER.stem(word) for word in filtered_tokens]

        # Join the tokens back into a string
        return " ".join(stemmed_tokens)
    except Exception as e:
        print(f"Error during text preprocessing: {e}")
        return ""


def extract_text_from_pdf(file_path: str) -> str:
    """
    Extract raw text from a PDF file.
    :param file_path: Path to the PDF file.
    :return: Raw text.
    """
    try:
        # Extract raw text
        raw_text = extract_text(file_path)
        print(f"Raw text from {file_path}:\n{raw_text[:500]}")  # Log the first 500 characters of the raw text
        if not raw_text.strip():
            print(f"No text extracted from {file_path}")
            return ""
        return raw_text
    except Exception as e:
        print(f"Error extracting text from {file_path}: {e}")
        return ""


def populate_database_from_pdfs(pdf_directory: str):
    db = SessionLocal()
    try:
        pdf_files = Path(pdf_directory).glob("*.pdf")
        for pdf_file in pdf_files:
            existing_file = db.query(PDFFile).filter(PDFFile.file_path == str(pdf_file)).first()
            if existing_file:
                print(f"Skipping {pdf_file.name}: already in the database.")
                continue

            raw_text = extract_text_from_pdf(str(pdf_file))
            if not raw_text:
                continue

            preprocessed_text = preprocess_text(raw_text)
            document_date = extract_date_from_content(raw_text)

            pdf_record = PDFFile(
                file_name=pdf_file.name,
                file_path=str(pdf_file),
                content=preprocessed_text,
                original_content=raw_text,
                document_date=document_date
            )
            db.add(pdf_record)
        db.commit()
        print(f"Database populated with PDFs from {pdf_directory}.")
    except Exception as e:
        db.rollback()
        print(f"Error populating database: {e}")
    finally:
        db.close()

def backfill_document_dates():
    db = SessionLocal()
    try:
        pdf_files = db.query(PDFFile).all()
        for pdf in pdf_files:
            if not pdf.document_date:
                document_date = extract_date_from_content(pdf.original_content)
                if document_date:
                    pdf.document_date = document_date
        db.commit()
        print("Backfill complete!")
    except Exception as e:
        db.rollback()
        print(f"Error during backfill: {e}")
    finally:
        db.close()
