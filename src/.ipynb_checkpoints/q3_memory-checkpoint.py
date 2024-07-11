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
    
    # Contar las menciones de cada usuario
    mentions = df['text'].str.findall(r'@\w+').explode()
    mentions = mentions.str[1:]  # Quitar el '@'
    mention_counts = mentions.value_counts().head(10)
    
    result = list(mention_counts.items())
    
    return result