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

    # Abfrage für alle PDF inklusive metadaten
    cursor.execute(
        '''
        SELECT 
            t1.attachment_filename as attachment_filename
            ,t2.email_subject as email_subject
            ,t2.email_date as email_date
            ,t2.email_body as email_body
            ,t2.email_header as email_header
            ,t2.email_sender as email_sender
            ,t2.email_id as email_id
            FROM
            attachments as t1
            LEFT JOIN emails as t2
            ON t1.email_id = t2.id
            WHERE attachment_filename LIKE '%.pdf'
        ''')

    pdf_files = cursor.fetchall()

    connection.close()

    return [
        {
            'attachment_filename': file[0],
            'email_subject': file[1],
            'email_date': file[2],
            'email_body': file[3],
            'email_header': file[4],
            'email_sender': file[5],
            'email_id': file[6],
        } for file in pdf_files]  # Liste der Dateinamen zurückgeben


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
    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS xml_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            src_path TEXT,
            src_filename TEXT,
            xml_content TEXT)
        '''
    )

    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS meta_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email_subject TEXT,
            email_date DATETIME,
            email_body TEXT,
            email_header TEXT,
            email_sender TEXT,
            email_id TEXT,
            xml_content_id INTEGER,
            FOREIGN KEY (xml_content_id) REFERENCES xml_data(id) ON DELETE CASCADE
            )
        '''
    )
    connection.commit()
    connection.close()

def save_meta_data_to_database(meta_data, db_path):
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    # Einfügen des XML-Inhalts in die Datenbank
    cursor.execute(
    """
            INSERT INTO meta_data 
            (email_subject, email_date, email_body, email_header, email_sender, email_id, xml_content_id) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
(meta_data['email_subject'], meta_data['email_date'], meta_data['email_body'], meta_data['email_header'],
                meta_data['email_sender'],meta_data['email_id'], meta_data['xml_content_id'])
    )
    connection.commit()
    connection.close()

def save_xml_to_database(xml_content, db_path, pdf_filename, pdf_base_path):
    """Speichert den Inhalt der XML-Datei in einer neuen SQLite-Datenbank."""

    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    # Einfügen des XML-Inhalts in die Datenbank
    cursor.execute("INSERT INTO xml_data (xml_content, src_filename, src_path) VALUES (?, ?, ?)",
                   (xml_content,pdf_filename,pdf_base_path))
    last_inserted_xml_data_id = cursor.lastrowid
    connection.commit()
    connection.close()
    return last_inserted_xml_data_id

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
    for element in pdf_files:
        pdf_file = element['attachment_filename']
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
            xml_content_id = save_xml_to_database(xml_content, output_db_path, pdf_file, pdf_base_path)
            element['xml_content_id'] = xml_content_id
            save_meta_data_to_database(element, output_db_path)
        else:
            print(f"have it already")

if __name__ == "__main__":
    main()
