# Interfaz de la clase GoogleDriveService
class IGoogleDriveService:
    def ListarArchivos(self):
        raise NotImplementedError()

    def SubirArchivos(self, file_path):
        raise NotImplementedError()