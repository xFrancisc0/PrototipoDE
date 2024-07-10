from ..GoogleDriveService.GoogleDriveService import GoogleDriveService
from ..AdvanaService.AdvanaService import AdvanaService

#Servicio orquestador.
#Utiliza Inyección de dependencias para facilitar pruebas unitarias, promover mantebilidad del codigo, flexibilidad y extensibilidad
#La implementacion de los metodos aun esta pendiente
class OrchestrateService:
    def __init__(self, google_drive_service: GoogleDriveService, advana_service: AdvanaService):
        self.google_drive_service = google_drive_service
        self.advana_service = advana_service

    def Metodo_dummy():
        pass