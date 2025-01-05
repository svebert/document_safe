import os
from dotenv import load_dotenv
from typing import List, Optional
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

class GmailCredentialsManager:
    def __init__(self, token_folder: str):
        self.token_folder = token_folder
        os.makedirs(token_folder, exist_ok=True)
        self.token_base_filename = 'token.json'
        self.creds: Optional[Credentials] = None

    def load_credentials(self, token_path: str) -> Optional[Credentials]:
        """Lädt die Anmeldeinformationen aus der JSON-Datei."""
        if os.path.exists(token_path):
            return Credentials.from_authorized_user_file(token_path)
        return None

    def save_credentials(self, creds: Credentials, token_path: str) -> None:
        """Speichert die Anmeldeinformationen in einer JSON-Datei."""
        with open(token_path, 'w') as token:
            token.write(creds.to_json())

    def get_credentials(self, email: str) -> Credentials:
        token_filename = f"{email}_{self.token_base_filename}"
        token_path = os.path.join(self.token_folder, token_filename)
        """Holt die Anmeldeinformationen, lädt sie oder fordert eine neue Authentifizierung an."""
        self.creds = self.load_credentials(token_path)  # Versuchen, die Anmeldeinformationen zu laden

        if self.creds is None or not self.creds.valid:
            # Wenn die Anmeldeinformationen ungültig sind oder nicht existieren, authentifizieren
            load_dotenv()
            GMAIL_CLIENT_ID: Optional[str] = os.getenv('DOCUMENT_LOADER_GMAIL_CLIENT_ID')
            GMAIL_CLIENT_KEY: Optional[str] = os.getenv('DOCUMENT_LOADER_GMAIL_CLIENT_KEY')
            flow = InstalledAppFlow.from_client_config(
                {
                    "installed": {
                        "client_id": GMAIL_CLIENT_ID,
                        "client_secret": GMAIL_CLIENT_KEY,
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token",
                    }
                },
                scopes=['https://www.googleapis.com/auth/gmail.readonly']
            )
            self.creds = flow.run_local_server(port=0)
            self.save_credentials(self.creds, token_path)  # Speichern der neuen Anmeldeinformationen

        elif not self.creds.valid and self.creds.expired and self.creds.refresh_token:
            # Token erneuern, wenn es abgelaufen ist
            self.creds.refresh(Request())  # Request muss importiert werden
            self.save_credentials(self.creds)  # Speichern der erneuerten Anmeldeinformationen

        return self.creds
