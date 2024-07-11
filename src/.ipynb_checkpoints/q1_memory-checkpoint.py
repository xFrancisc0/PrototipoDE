import pandas as pd
import pandasql as psql
import json
from typing import List, Tuple
from datetime import datetime
from pathlib import Path
from memory_profiler import profile

def convert_complex_to_string(data):
    if isinstance(data, dict) or isinstance(data, list):
        return json.dumps(data)
    return data

@profile
def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    with open(Path(file_path), 'r') as f:
        data = json.load(f)  # Cargar el archivo JSON

    df = pd.DataFrame(data)  # Crear un DataFrame a partir de los datos JSON
    print(df)
    
    # Extraer la columna 'username' de 'user' en una nueva columna
    df['username'] = df['user'].apply(lambda x: x['username'] if isinstance(x, dict) and 'username' in x else None)
    # Seleccionar las columnas deseadas y ordenar por 'retweetCount' en orden descendente
    result = df[['date', 'username', 'retweetCount']].sort_values(by='retweetCount', ascending=False).head(10)

    return result