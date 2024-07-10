import unittest
from AdvanaService import AdvanaService

class AdvanaServiceTests(unittest.TestCase):

    def setUp(self):
        # Configuración inicial que se ejecuta antes de cada prueba, permite testear si se encuentran bien las credenciales
        self.advana_service = AdvanaService()  #Prueba exitosa

    def tearDown(self):
        # Limpieza después de cada prueba, si es necesaria
        pass

    def test_enviar_peticion_http(self):
        print("\nProbando enviar peticion http...")
        JSONObj = {"name": "TDD (Pruebas) EnviarPeticionHTTP() Advana Fmateu Aun en desarrollo",
                   "mail": "francisco.mateu.araneda@gmail.com Aun en desarrollo",
                   "github_url": "https://github.com/xFrancisc0/PrototipoDE Aun en desarrollo"
                  }
        URLRequest = "https://advana-challenge-check-api-cr-k4hdbggvoq-uc.a.run.app/data-engineer"
        self.advana_service.EnviarPeticionHTTP(JSONObj, URLRequest, "POST", "REST")

if __name__ == '__main__':
    unittest.main()