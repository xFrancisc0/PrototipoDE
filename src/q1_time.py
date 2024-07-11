import pandas as pd
import json
from typing import List, Tuple
from datetime import datetime

def convert_complex_to_string(data):
    if isinstance(data, dict) or isinstance(data, list):
        return json.dumps(data)
    return data
    
def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    with open(file_path, 'r') as f:
        data = json.load(f)  # Cargar el archivo JSON

    print(type(data))
    df = pd.DataFrame(data)  # Crear un DataFrame a partir de los datos JSON
    print(df.info())
    