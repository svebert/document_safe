import os
from datetime import datetime, timedelta
from typing import List, Optional
import base64
import sqlite3
import logging
from dotenv import load_dotenv, find_dotenv

from googleapiclient.discovery import build

from gmail_credentials_manager import GmailCredentialsManager

# Konfigurieren Sie das Logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Log in Konsole
    ]
)

class GmailDataLoader:
    def __init__(self, db_path: str = 'mails.db', attachment_folder: str = 'attachments'):
        load_dotenv()
        project_dir = os.path.dirname(find_dotenv())
        project_data_dir = os.path.join(project_dir, os.getenv('DOCUMENT_LOADER_DATA_PATH'))
        os.makedirs(project_data_dir, exist_ok=True)
        gmail_data_dir = os.path.join(project_data_dir, 'gmail')
        os.makedirs(gmail_data_dir, exist_ok=True)
        if not os.path.isabs(db_path):
            db_path = os.path.join(gmail_data_dir, 'mails.db')  # Pfad zur SQLite-Datenbank
        self.db_path = db_path
        if not os.path.isabs(attachment_folder):
            attachment_folder = os.path.join(gmail_data_dir, attachment_folder)
        os.makedirs(attachment_folder, exist_ok=True)
        self.attachment_folder = attachment_folder

        self._cred_tmp_folder = os.path.join(self.attachment_folder, 'tmp', 'credentials')
        self.email_addresses: List[str] = os.getenv('DOCUMENT_LOADER_GMAIL_EMAIL_LIST').split(",")
        self.init_db()

    # Datenbank initialisieren
    def init_db(self) -> None:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email_subject TEXT,
                email_date DATETIME,
                email_body TEXT,
                email_id TEXT UNIQUE  -- Eindeutige ID der E-Mail
            );
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS attachments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                attachment_filename TEXT UNIQUE,
                email_id INTEGER,  -- Verweis auf die E-Mail-ID
                FOREIGN KEY (email_id) REFERENCES emails(id) ON DELETE CASCADE
            );
        ''')
        conn.commit()
        conn.close()

    def load(self, load_all: bool=False):
        latest_date = self.get_latest_email_date()
        if latest_date is None or load_all:
            start_date = datetime.now() - timedelta(days=1000)  # Z.B. die
        else:
            start_date = latest_date  # Startdatum ist das Datum der neuesten )
        end_date: datetime = datetime.now()  # Enddatum (heute)
        for email in self.email_addresses:
            self._download_attachments(email, start_date, end_date)

    def get_latest_email_date(self) -> Optional[datetime]:
        """Holt das Datum der neuesten E-Mail aus der Datenbank."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT MAX(email_date) FROM emails")
        result = cursor.fetchone()

        conn.close()

        if result and result[0]:
            return datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S%z')  # Format anpassen
        return None

    def _download_attachments(self, email: str, start_date: datetime, end_date: datetime) -> None:
        cred_manager = GmailCredentialsManager(self._cred_tmp_folder)
        creds = cred_manager.get_credentials(email)

        service = build('gmail', 'v1', credentials=creds)

        valid_file_types = ['pdf', 'png', 'jpeg', 'jpg', 'doc', 'docx', 'odt', 'zip', 'tar.gz']
        file_type_query = ' OR '.join([f'filename:{file_type}' for file_type in valid_file_types])

        query = f'has:attachment after:{start_date.strftime("%Y/%m/%d")} before:{end_date.strftime("%Y/%m/%d")} ({file_type_query})'

        results = service.users().messages().list(userId='me', q=query, maxResults=10000).execute()
        messages: List[dict] = results.get('messages', [])

        if not messages:
            logging.info("Keine E-Mails mit Anhängen gefunden.")
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        for message in messages:
            msg = service.users().messages().get(userId='me', id=message['id'], format='full').execute()
            subject = next((header['value'] for header in msg['payload']['headers'] if header['name'] == 'Subject'),
                           'No Subject')
            email_date_str = next((header['value'] for header in msg['payload']['headers'] if header['name'] == 'Date'), 'No Date')
            cleaned_date_string = email_date_str.split(' (')[0].replace(' GMT', '')
            try:
                email_date = datetime.strptime(cleaned_date_string, '%d %b %Y %H:%M:%S %z')
            except ValueError:
                try:
                    try:
                        email_date = datetime.strptime(cleaned_date_string, '%a, %d %b %Y %H:%M:%S %z')
                    except ValueError:
                        email_date = datetime.strptime(cleaned_date_string,  '%d %b %y %H:%M:%S %z')
                except ValueError:
                    email_date = datetime.strptime(cleaned_date_string,  '%a, %d %b %Y %H:%M:%S')
            email_body = ""
            email_id = msg['id']

            cursor.execute("SELECT id FROM emails WHERE email_id=?", (email_id,))
            email_record = cursor.fetchone()
            if email_record:
                logging.debug(f"Email {email_id} already downloaded")
                continue

            # Speichern der E-Mail-Daten in der Datenbank
            cursor.execute(
                "INSERT OR IGNORE INTO emails (email_subject, email_date, email_body, email_id) VALUES (?, ?, ?, ?)",
                (subject, email_date, email_body.strip(), email_id))

            cursor.execute("SELECT id FROM emails WHERE email_id=?", (email_id,))
            email_record = cursor.fetchone()
            if not email_record:
                err_msg = f"Could not find the saved email record for {email_id}."
                logging.error(err_msg)
                raise ValueError(err_msg)

            email_record_id = email_record[0]  # Die ID der gespeicherten E-Mail

            if 'parts' in msg['payload']:
                for part in msg['payload']['parts']:
                    if part.get('mimeType') == 'text/plain' or part.get('mimeType') == 'text/html':
                        body_data = part.get('body', {}).get('data')
                        if body_data:
                            email_body += base64.urlsafe_b64decode(body_data.encode('UTF-8')).decode('utf-8')
                            cursor.execute("UPDATE emails SET email_body = ? WHERE id = ?",
                                           (email_body.strip(), email_record_id))
                    if part.get('filename'):
                        filename = part['filename']
                        extension = filename.split('.')[-1].lower()  # Extrahieren Sie die Dateiendung

                        if extension not in valid_file_types:
                            logging.info(f"{email_id}: '{filename}' invalid file type/extension")
                            continue

                        attachment_data = None
                        if part.get('body') and part['body'].get('attachmentId'):
                            attachment_id = part['body']['attachmentId']
                            attachment_data = service.users().messages().attachments().get(userId='me',
                                                                                           id=attachment_id,
                                                                                           messageId=msg['id']).execute().get('data')
                        if attachment_data is None:
                            logging.info(f"{email_id}: no attachment")
                            continue

                        file_data = base64.urlsafe_b64decode(attachment_data.encode('UTF-8'))
                        # Anhangsdateiname mit der E-Mail-ID als Präfix
                        filename = filename.replace("/","_").replace("\\","_")
                        prefixed_filename = f"{email_id}_{filename}"
                        file_path = os.path.join(self.attachment_folder, prefixed_filename)
                        with open(file_path, 'wb') as f:
                            f.write(file_data)
                            logging.info(f'Saved attachment: {file_path}')

                        cursor.execute("INSERT OR IGNORE INTO attachments (attachment_filename, email_id) VALUES (?, ?)",
                                       (prefixed_filename, email_record_id))

        conn.commit()
        conn.close()


if __name__ == '__main__':
    gml = GmailDataLoader()
    gml.load(load_all=True)