import pandas as pd
import json
from typing import List, Tuple
import re
from memory_profiler import profile

@profile
def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    with open(file_path, 'r') as f:
        data = json.load(f)  # Cargar el archivo JSON
    
    df = pd.DataFrame(data)  # Crear un DataFrame a partir de los datos JSON
    
    # Contar las menciones de cada usuario
    mentions = df['text'].apply(lambda text: re.findall(r'@\w+', text))
    all_mentions = [mention[1:] for sublist in mentions for mention in sublist]  # Quitar el '@'
    mention_counts = pd.Series(all_mentions).value_counts().head(10)
    
    result = list(mention_counts.items())
    
    return result