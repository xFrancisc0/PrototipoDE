import requests
import zipfile
import io
import json
from pyspark.sql import SparkSession

def ObtenerDataPorHTTPRequest(url, metodo, TipoArchivo, boolarchivocomprimido):
    if metodo == "GET":
        response = requests.get(url)
        if response.status_code == 200:
            if TipoArchivo == "JSON":
                print("Obteniendo JSON POR HTTP Request")
                if boolarchivocomprimido:
                    # Si el archivo está comprimido
                    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                        json_file_name = z.namelist()[0]
                        with z.open(json_file_name) as f:
                            content = f.read().decode('utf-8')
                            # Imprimir los primeros 1000 caracteres para inspeccionar el contenido    
                            try:
                                # Si el contenido es una lista de objetos JSON
                                json_data = []
                                for line in content.split('\n'):
                                    if line.strip():  # Solo procesar líneas no vacías
                                        try:
                                            json_obj = json.loads(line)
                                            json_data.append(json_obj)
                                        except json.JSONDecodeError as e:
                                            print(f'Error de decodificación JSON en línea: {line}')
                                            print(f'Error: {e}')
                                if len(json_data) == 1:
                                    json_data = json_data[0]
                                print('Archivo JSON descomprimido y cargado exitosamente.')
                                return json_data
                            except json.JSONDecodeError as e:
                                print(f'Error de decodificación JSON: {e}')
                                return None
                else:
                    json_data = response.json()
                    print('Archivo descargado exitosamente.')
                    return json_data
        else:
            print(f'Error al descargar el archivo: {response.status_code}')
            return None
    else:
        raise ValueError("Método HTTP no soportado, solo 'GET' está permitido.")

def get_spark_session():
    import os 
    import sys
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = SparkSession.builder \
    .appName("TransformacionJsonConSQL") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
    return spark

# Instancia global de SparkSession
spark = get_spark_session()