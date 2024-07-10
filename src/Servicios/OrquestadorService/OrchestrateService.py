from ..GoogleDriveService.GoogleDriveService import GoogleDriveService

#Servicio orquestador.
#Utiliza Inyección de dependencias para facilitar pruebas unitarias, promover mantebilidad del codigo, flexibilidad y extensibilidad
class OrchestrateService:
    def __init__(self, google_drive_service: GoogleDriveService):
        self.google_drive_service = google_drive_service

    def test_listar_archivos(self):
        print("\nProbando listar contenedores...")
        contenedores_datalake = self.google_drive_service.listar_contenedores()   #Prueba exitosa

        print("\nProbando listar archivos...")                                    #Prueba exitosa
        for contenedorJSON in contenedores_datalake:
            self.google_drive_service.listar_archivos_en_contenedor(contenedorJSON)

        print("\nProbando eliminar todos los archivos de todos los contenedores...")                                    #Prueba exitosa
        for contenedorJSON in contenedores_datalake:
            self.google_drive_service.eliminar_archivos_de_contenedor(contenedorJSON)
            self.verificar_permisos_archivo(contenedorJSON["id"])
            self.test_subir_archivo(contenedorJSON["id"])

    def verificar_permisos_archivo(self, idArchivo):
        print("\nProbando permisos archivo...")
        self.google_drive_service.listarPermisosArchivo(idArchivo)  # Prueba exitosa

    def test_subir_archivo(self, idContenedor):  #Prueba exitosa
        print("\nProbando subir_archivos...")
        # Simular subida de archivo, puedes ajustar según tus necesidades
        NombreArchivo = "Archivo.txt"
        self.google_drive_service.SubirArchivo(idContenedor, NombreArchivo)  # Prueba exitosa