import pandas as pd
import json
from typing import List, Tuple
from datetime import datetime
from memory_profiler import profile

@profile
def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    with open(file_path, 'r') as f:
        data = json.load(f)  # Cargar el archivo JSON
    
    df = pd.DataFrame(data)  # Crear un DataFrame a partir de los datos JSON
    
    # Convertir la columna de fecha a datetime y extraer solo la fecha
    df['date'] = pd.to_datetime(df['date']).dt.date
    
    # Contar el número de tweets por fecha
    tweet_counts = df['date'].value_counts()
    
    # Obtener las 10 fechas con más tweets
    top_dates = tweet_counts.nlargest(10).index
    
    result = []
    for date in top_dates:
        users_on_date = df[df['date'] == date]['username']
        most_frequent_user = users_on_date.value_counts().idxmax()
        result.append((date, most_frequent_user))
    
    return result