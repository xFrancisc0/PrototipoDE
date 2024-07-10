import unittest
from GoogleDriveService import GoogleDriveService

class GoogleDriveServiceTests(unittest.TestCase):

    def setUp(self):
        # Configuración inicial que se ejecuta antes de cada prueba, permite testear si se encuentran bien las credenciales
        self.google_drive_service = GoogleDriveService()  #Prueba exitosa

    def tearDown(self):
        # Limpieza después de cada prueba, si es necesaria
        pass

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

    def verificar_permisos_archivo(self, idarchivo):
        print("\nProbando permisos archivo...")
        self.google_drive_service.listarPermisosArchivo(idarchivo)  # Prueba exitosa

    def test_subir_archivo(self, idcontenedor):  #Prueba exitosa
        print("\nProbando subir_archivos...")
        # Simular subida de archivo, puedes ajustar según tus necesidades
        NombreArchivo = "Archivo.txt"
        self.google_drive_service.subir_archivo_a_contenedor(idcontenedor, NombreArchivo)  # Prueba exitosa

if __name__ == '__main__':
    unittest.main()