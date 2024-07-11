import pandas as pd
import json
from typing import List, Tuple
from pathlib import Path
from memory_profiler import profile

@profile
def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    with open(file_path, 'r') as f:
        data = json.load(f)  # Cargar el archivo JSON
    
    df = pd.DataFrame(data)  # Crear un DataFrame a partir de los datos JSON
    
    df['mentionedUsers'] = df['mentionedUsers'].apply(lambda x: [user['username'] for user in x] if x else []) #El campo sera una lista de usuarios
    df['mentionedUsersCount'] = df['mentionedUsers'].apply(len) #Se cuentan los usuarios mencionados de cada lista y se crea el campo mentioned users
    top_10_users = df.nlargest(10, 'mentionedUsersCount') #limit 10
    result = [(row['user']['username'], row['mentionedUsersCount']) for _, row in top_10_users.iterrows()]

    return result