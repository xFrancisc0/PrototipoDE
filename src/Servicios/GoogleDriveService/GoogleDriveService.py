import os
from IGoogleDriveService import IGoogleDriveService #Colocar punto en Jupyter, Quitar punto en pycharm al compilar GoogleDriveServiceTests.py
from google.oauth2 import service_account
from googleapiclient.discovery import build

class GoogleDriveService(IGoogleDriveService):
    def __init__(self):
        current_directory = os.path.dirname(__file__)
        credentials_path = os.path.join(current_directory, 'Credenciales', 'credenciales.json')

        if not os.path.exists(credentials_path):
            raise FileNotFoundError(f'File not found: {credentials_path}')

        credentials = service_account.Credentials.from_service_account_file(credentials_path,
                                                                            scopes=[
                                                                                'https://www.googleapis.com/auth/drive'])

        self.drive_service = build('drive', 'v3', credentials=credentials)

    # Consulta para obtener los contenedores
    def listar_contenedores(self):
        IdContenedorDataLake = "1nVbnD0pgYnAeym3lUoNna5cE7goN2U5N" #Contenedor padre publico para la visión, privado para la edición
        query = f"'{IdContenedorDataLake}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        ContenedoresJSONArray = self.drive_service.files().list(q=query).execute().get('files', [])
        print(ContenedoresJSONArray)
        return ContenedoresJSONArray
    
    def listar_archivos_en_contenedor(self, ContenedorJSON):
        # Consulta para obtener archivos de la carpeta
        IdContenedor = ContenedorJSON["id"]
        query = f"'{IdContenedor}' in parents and trashed=false"
        results = self.drive_service.files().list(q=query).execute()
        ArchivosJSONArray = results.get('files', [])
        print(ArchivosJSONArray)
        return ArchivosJSONArray

    def eliminar_archivos_de_contenedor(self, ContenedorJSON):
        ArchivosJSONArray = self.listar_archivos_en_contenedor(ContenedorJSON)
        for ArchivoJSON in ArchivosJSONArray:
            try:
                self.drive_service.files().delete(fileId=ArchivoJSON["id"]).execute()
                print(f"Archivo con ID {ArchivoJSON['id']} eliminado correctamente.")
            except Exception as e:
                print(f"Error al eliminar archivo con ID {ArchivoJSON['id']}: {e}")

    def subir_archivo_a_contenedor(self, IdContenedor, NombreArchivo):
            # Crear un archivo de texto local
            with open(NombreArchivo, 'w') as f:
                f.write(NombreArchivo)

            # Metadata del archivo que vamos a subir
            MetadataArchivo = {
                'name': NombreArchivo,
                'parents': [IdContenedor],
                'mimeType': 'text/plain'
            }

            try:
                # Subir el archivo al Drive
                media = self.drive_service.files().create(body=MetadataArchivo, media_body=NombreArchivo).execute()
                print(f"Archivo '{NombreArchivo}' subido correctamente. ID: {media['id']}")
            except Exception as e:
                print(f"Error al subir archivo '{NombreArchivo}': {e}")
            finally:
                if os.path.exists(NombreArchivo):
                    os.remove(NombreArchivo)
                    print(f"Archivo local '{NombreArchivo}' eliminado.")


    def listarPermisosArchivo(self, idArchivo):
        permissions = self.drive_service.permissions().list(fileId=idArchivo).execute()
        print(permissions)