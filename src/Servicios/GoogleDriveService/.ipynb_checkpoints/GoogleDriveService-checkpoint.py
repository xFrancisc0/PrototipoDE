import os
from .IGoogleDriveService import IGoogleDriveService
from googleapiclient.discovery import build

class GoogleDriveService(IGoogleDriveService):
    def __init__(self):
        print("Path actual":, os.path.dirname(__file__))
        credentials_path = os.path.join(os.path.dirname(__file__), '/Credenciales/credenciales.json')
        print("Credentials path:", credentials_path)  # Imprime la ruta de las credenciales
        self.drive_service = build('drive', 'v3', credentials=credentials_path)

    def listar_archivos(self):
        results = self.drive_service.files().list(
        pageSize=10, fields="nextPageToken, files(id, name)").execute()
        items = results.get('files', [])

        if not items:
            print('No se encontraron archivos.')
        else:
            print('Archivos:')
            for item in items:
                print(u'{0} ({1})'.format(item['name'], item['id']))

        pass

    def subir_archivos(self, file_path):
        # Implementación para subir archivos a Google Drive
        pass
