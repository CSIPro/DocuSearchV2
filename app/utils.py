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

import fitz  # PyMuPDF
import pytesseract
from pdf2image import convert_from_path
from PIL import Image

# Ensure NLTK resources are downloaded
import nltk

nltk.download('punkt')
nltk.download('stopwords')

# Set NLTK stopwords for Spanish
STOPWORDS = set(stopwords.words('spanish'))

# Initialize the Spanish stemmer
SPANISH_STEMMER = SnowballStemmer("spanish")
def extract_text_from_image(pdf_path):
    text = ""
    doc = fitz.open(pdf_path)

    for page_num in range(len(doc)):
        page = doc[page_num]
        text += page.get_text("text")  # Extrae texto normal
        if not text.strip():  # Si la página no tiene texto, intentar OCR
            images = convert_from_path(pdf_path, first_page=page_num+1, last_page=page_num+1)
            for img in images:
                text += pytesseract.image_to_string(img, lang="spa")  # Extraer texto con OCR
        
    return text.strip()

def extract_date_from_text(text: str) -> datetime.date:
    
   # Expresiones regulares
    patrones = [
        r"(?:EL DIA \w+\s+)?(\d{1,2}|\w+)\s+DE\s+([A-ZÁÉÍÓÚ]+)\s+DEL\s+DOS\s+MIL(?:\s+(\w+))?",  # Formato largo con "DEL DOS MIL"
        r"(\d{1,2})\s+DE\s+([A-ZÁÉÍÓÚ]+)\s+DE\s+(\d{4})",  # Formato corto con año en 4 dígitos
        r"(?:EL DIA \w+\s+)?(\d{1,2}|\w+)\s+DE\s+([A-ZÁÉÍÓÚ]+)\s+DE\s+DOS\s+MIL(?:\s+(\w+))?"  # Formato largo con "DE DOS MIL"
    ]

    # Buscar la primera coincidencia válida
    match = next((re.search(pat, text, re.IGNORECASE) for pat in patrones if re.search(pat, text, re.IGNORECASE)), None)

    # Si se encontró una coincidencia, imprimir los grupos
    if match:
        print("Fecha Exitosa")
    else:
        print(f"No se encontró una fecha válida en{text}")
        
    if match:
        day_text, month_text, year_text = match.groups()
    
        # Diccionario de conversión de nombres de meses a números
        meses = {
            "ENERO": 1, "FEBRERO": 2, "MARZO": 3, "ABRIL": 4, "MAYO": 5, "JUNIO": 6,
            "JULIO": 7, "AGOSTO": 8, "SEPTIEMBRE": 9, "OCTUBRE": 10, "NOVIEMBRE": 11, "DICIEMBRE": 12
        }
        
        # Diccionario de conversión de números escritos a enteros
        numeros_texto = { 
            "PRIMERO":1,"UNO": 1, "DOS": 2, "TRES": 3, "CUATRO": 4, "CINCO": 5, "SEIS": 6, "SIETE": 7,
            "OCHO": 8, "NUEVE": 9, "DIEZ": 10, "ONCE": 11, "DOCE": 12, "TRECE": 13,
            "CATORCE": 14, "QUINCE": 15, "DIECISÉIS": 16, "DIECISIETE": 17, "DIECIOCHO": 18,
            "DIECINUEVE": 19, "VEINTE": 20, "VEINTIUNO": 21, "VEINTIDÓS": 22, "VEINTITRÉS": 23,
            "VEINTICUATRO": 24, "VEINTICINCO": 25, "VEINTISÉIS": 26, "VEINTISIETE": 27,
            "VEINTIOCHO": 28, "VEINTINUEVE": 29, "TREINTA": 30, "TREINTA Y UNO": 31
        }

        # Convertir el mes a número
        month = meses.get(month_text.upper())
        
        # Determinar el día (puede ser número o texto)
        day = numeros_texto.get(day_text.upper(), None) if not day_text.isdigit() else int(day_text)

        # Determinar el año (puede ser vacío o en texto)
        if year_text.isdigit():
            year = int(year_text)
        else:
            year = 2000 + numeros_texto.get(year_text.upper(), 0) if year_text else 2000  # Si no hay año, asumir 2000

        if day and month and year:
            # Convertir a tipo datetime.date
            date_obj = datetime.date(year=year, month=month, day=day)
            print(f"Fecha extraída: {date_obj}")
            return date_obj
    
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
                raw_text = extract_text_from_image(str(pdf_file))
        
            
            # Si sigue sin texto, omitir el archivo
            if not raw_text.strip():
                print(f"No se pudo extraer texto de {pdf_file.name}. Saltando...")
                continue

            preprocessed_text = preprocess_text(raw_text)
            document_date = extract_date_from_text(raw_text)

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
                document_date = extract_date_from_text(pdf.original_content)
                if document_date:
                    pdf.document_date = document_date
        db.commit()
        print("Backfill complete!")
    except Exception as e:
        db.rollback()
        print(f"Error during backfill: {e}")
    finally:
        db.close()
