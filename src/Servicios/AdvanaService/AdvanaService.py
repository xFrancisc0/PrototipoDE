import requests
import json
from IAdvanaService import IAdvanaService #Colocar punto en Jupyter, Quitar punto en pycharm al compilar AdvanaServiceTests.py

class AdvanaService(IAdvanaService):
    def __init__(self):
        pass

    # Metodo para enviar peticion HTTP al destino de Advana.
    def EnviarPeticionHTTP(self, JSONObj, URLRequest, Metodo, TipoWebService):
        if(TipoWebService == "REST"):
            if(Metodo == "POST"):
                headers = {
                    'Content-Type': 'application/json'
                }

                try:
                    response = requests.post(URLRequest, headers=headers, json=JSONObj)
                    response.raise_for_status()  # Lanza una excepción para errores HTTP
                    print(f"Solicitud POST a {URLRequest} exitosa")
                    return response.json()  # Devuelve la respuesta como JSON si la hay
                except requests.exceptions.RequestException as e:
                    print(f"Error al hacer la solicitud POST a {URLRequest}: {e}")
                    return None
            else:
                print(f"El método '{Metodo}' no está soportado por este método.")
                return None