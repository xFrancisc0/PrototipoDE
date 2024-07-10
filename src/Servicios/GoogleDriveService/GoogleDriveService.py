import os
import io
from .IGoogleDriveService import IGoogleDriveService #Colocar punto en Jupyter, Quitar punto en pycharm al compilar GoogleDriveServiceTests.py
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload
from pathlib import Path
import json


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

    # Consulta para obtener los contenedores obj
    def listar_contenedores_JSONArray(self, IdContenedorDataLake):
        query = f"'{IdContenedorDataLake}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        ContenedoresJSONArray = self.drive_service.files().list(q=query).execute().get('files', [])
        print("Contenedores encontrados: ", ContenedoresJSONArray)
        return ContenedoresJSONArray
    
    def listar_archivos_en_contenedorJSON(self, ContenedorJSON): #IdContenedor es JSON
        # Consulta para obtener archivos de la carpeta
        IdContenedor = ContenedorJSON["id"]
        query = f"'{IdContenedor}' in parents and trashed=false"
        results = self.drive_service.files().list(q=query).execute()
        ArchivosJSONArray = results.get('files', [])
        print("Archivos encontrados: ", ArchivosJSONArray)
        return ArchivosJSONArray
    
    def listar_archivos_en_contenedorSTR(self, IdContenedor): #IdContenedor es STR
        # Consulta para obtener archivos de la carpeta
        query = f"'{IdContenedor}' in parents and trashed=false"
        results = self.drive_service.files().list(q=query).execute()
        ArchivosJSONArray = results.get('files', [])
        print("Archivos encontrados: ", ArchivosJSONArray)
        return ArchivosJSONArray

    def eliminar_archivos_de_contenedor(self, ContenedorJSON):
        ArchivosJSONArray = self.listar_archivos_en_contenedorJSONArray(ContenedorJSON)
        for ArchivoJSON in ArchivosJSONArray:
            try:
                self.drive_service.files().delete(fileId=ArchivoJSON["id"]).execute()
                print(f"Archivo con ID {ArchivoJSON['id']} eliminado correctamente.")
            except Exception as e:
                print(f"Error al eliminar archivo con ID {ArchivoJSON['id']}: {e}")

    def subir_archivo_a_contenedor(self, IdContenedor, NombreArchivo, DataArchivo):
        print("Subiendo data a Google Drive")
        
        # Asegurarse de que DataArchivo sea una cadena JSON
        if isinstance(DataArchivo, list):
            DataArchivo = json.dumps(DataArchivo)  # Convertir la lista a una cadena JSON
        elif not isinstance(DataArchivo, str):
            raise ValueError("DataArchivo debe ser una cadena de texto o una lista")

        # Función para obtener o crear una carpeta en Google Drive
        def get_or_create_folder(parent_id, folder_name):
            # Buscar la carpeta
            query = f"'{parent_id}' in parents and name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            results = self.drive_service.files().list(q=query).execute()
            folders = results.get('files', [])
            
            if len(folders) > 0:
                return folders[0]['id']
            else:
                # Crear una nueva carpeta
                folder_metadata = {
                    'name': folder_name,
                    'mimeType': 'application/vnd.google-apps.folder',
                    'parents': [parent_id]
                }
                folder = self.drive_service.files().create(body=folder_metadata, fields='id').execute()
                return folder['id']

        # Obtener los directorios de Google Drive desde la ruta del archivo
        path = Path(NombreArchivo)
        directories = path.parts[:-1]  # Todas las partes menos el archivo en sí
        parent_id = IdContenedor  # Comienza con el contenedor raíz

        # Crear los directorios necesarios en gdrive
        for folder_name in directories:
            parent_id = get_or_create_folder(parent_id, folder_name)
        
        # Metadata del archivo que vamos a subir
        MetadataArchivo = {
            'name': path.name,  # Nombre del archivo
            'parents': [parent_id],  # ID del directorio padre en Google Drive
            'mimeType': 'application/json'  # MIME type para archivo JSON
        }
        # Definir la ruta del archivo
        archivo_path = Path(f'/TemporalBlobs/{NombreArchivo}')
        
        # Asegurarse de que el archivo existe
        if archivo_path.exists():
            # Abrir el archivo local para sobreescribir (overwrite)
            with archivo_path.open('w') as f:
                f.write(DataArchivo)  # Nuevos datos a escribir en el archivo
        else:
            print(f"El archivo {archivo_path} no existe.")
            
        try:
            # Crear el objeto MediaFileUpload
            media = MediaFileUpload(archivo_path, mimetype='application/json')

            # Buscar el archivo de google drive para sobreescribir (overwrite)
            query = f"'{IdContenedor}' in parents and name='{Path(NombreArchivo).name}' and trashed=false"
            existing_files = self.drive_service.files().list(q=query).execute().get('files', [])
    
            if existing_files:
                # Si el archivo ya existe, eliminarlo
                file_id = existing_files[0]['id']
                self.drive_service.files().delete(fileId=file_id).execute()
                print(f"Archivo {NombreArchivo} ya existente en google cloud con ID {file_id} eliminado (overwrite).")
            
            # Subir el archivo al Drive
            archivo = self.drive_service.files().create(body=MetadataArchivo, media_body=media).execute()
            print(f"Archivo '{NombreArchivo}' subido correctamente. ID: {archivo['id']}")
        except Exception as e:
            print(f"Error al subir archivo '{NombreArchivo}': {e}")
        finally:
            pass
            # Eliminar el archivo local si existe
            #if os.path.exists(temp_file_path):
                #os.remove(temp_file_path)

    
    def obtener_blob_archivo_gdrive(self, archivo_id):
        request = self.drive_service.files().get_media(fileId=archivo_id)
        file_stream = io.BytesIO()
        downloader = MediaIoBaseDownload(file_stream, request)

        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f"Descargando {int(status.progress() * 100)}%.")

        file_stream.seek(0)  # Volver al inicio del archivo
        return file_stream

    def descargar_archivo_porid(self, archivo_id):
        blob = self.obtener_blob_archivo_gdrive(archivo_id)
        data = blob.read()
        return data


    def listar_permisos_archivo(self, idArchivo):
        permissions = self.drive_service.permissions().list(fileId=idArchivo).execute()
        print(permissions)