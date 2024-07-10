from ..GoogleDriveService.GoogleDriveService import GoogleDriveService

#Servicio orquestador.
#Utiliza Inyección de dependencias para facilitar pruebas unitarias, promover mantebilidad del codigo, flexibilidad y extensibilidad
class OrchestrateService:
    def __init__(self, google_drive_service: GoogleDriveService):
        self.google_drive_service = google_drive_service


