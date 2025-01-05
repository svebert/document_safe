import os
import sqlite3
import aspose.pdf as pdf
import uuid
import tempfile
from dotenv import load_dotenv, find_dotenv
import io

def get_pdf_files(db_path):
    """Holt sich alle PDF-Dateien aus der SQLite-Datenbank."""
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()

    # Abfrage für alle PDF-Dateien
    cursor.execute("SELECT attachment_filename FROM attachments WHERE attachment_filename LIKE '%.pdf'")
    pdf_files = cursor.fetchall()

    connection.close()

    return [file[0] for file in pdf_files]  # Liste der Dateinamen zurückgeben


def convert_pdf_to_xml(pdf_filename, pdf_base_path):
    """Konvertiert eine PDF-Datei in XML."""
    # PDF-Dokument laden
    xml_content = None
    try:
        document = pdf.Document(os.path.join(pdf_base_path, pdf_filename))
        random_filename = f"{uuid.uuid4()}.xml"
        temp_xml_file_path = os.path.join(tempfile.gettempdir(), random_filename)
        document.save(temp_xml_file_path, pdf.SaveFormat.MOBI_XML)
        with open(temp_xml_file_path, 'r', encoding='utf-8') as file:
            xml_content = file.read()

        os.remove(temp_xml_file_path)
    except RuntimeError as ex:
        print(ex)
        xml_content = str(ex)


    if xml_content is None:
        raise ValueError("failed to convert pdf to xml")
    return xml_content

def init_db(db_path):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()

    # Erstellen einer neuen Tabelle für die XML-Daten
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS xml_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            src_path TEXT,
            src_filename TEXT,
            xml_content TEXT
        )
    ''')
    connection.commit()
    connection.close()

def save_xml_to_database(xml_content, db_path, pdf_filename, pdf_base_path):
    """Speichert den Inhalt der XML-Datei in einer neuen SQLite-Datenbank."""

    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    # Einfügen des XML-Inhalts in die Datenbank
    cursor.execute("INSERT INTO xml_data (xml_content, src_filename, src_path) VALUES (?, ?, ?)",
                   (xml_content,pdf_filename,pdf_base_path))
    connection.commit()
    connection.close()

def main():

    load_dotenv()
    project_dir = os.path.dirname(find_dotenv())
    NORMALIZER_INPUT_DB_PATH = os.getenv('NORMALIZER_INPUT_DB_PATH')
    if not os.path.isabs(NORMALIZER_INPUT_DB_PATH):
        mail_db_path = os.path.join(project_dir, NORMALIZER_INPUT_DB_PATH)
    else:
        mail_db_path = NORMALIZER_INPUT_DB_PATH

    NORMALIZER_DB_PATH = os.getenv('NORMALIZER_DB_PATH')
    if not os.path.isabs(NORMALIZER_DB_PATH):
        output_db_path = os.path.join(project_dir, NORMALIZER_DB_PATH)
    else:
        output_db_path = NORMALIZER_DB_PATH

    pdf_base_path = os.getenv('NORMALIZER_INPUT_DATA_PATH')
    if not os.path.isabs(pdf_base_path):
        pdf_base_path = os.path.join(project_dir, pdf_base_path)

    # Schritt 1: PDF-Dateien abrufen
    pdf_files = get_pdf_files(mail_db_path)
    init_db(output_db_path)
    for pdf_file in pdf_files:
        print(f"Verarbeite: {pdf_file}")

        connection = sqlite3.connect(output_db_path)
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM xml_data WHERE src_filename = ? AND src_path = ?",
                       (pdf_file, pdf_base_path))
        exists = cursor.fetchone()[0] > 0
        connection.close()
        if not exists:
            # Schritt 2: PDF in XML konvertieren
            xml_content = convert_pdf_to_xml(pdf_file, pdf_base_path)

            # Schritt 3: XML in die neue Datenbank speichern
            save_xml_to_database(xml_content, output_db_path, pdf_file, pdf_base_path)
        else:
            print(f"have it already")

if __name__ == "__main__":
    main()
