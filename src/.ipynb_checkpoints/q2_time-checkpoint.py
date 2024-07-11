import pandas as pd
import json
from typing import List, Tuple

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    with open(file_path, 'r') as f:
        data = json.load(f)  # Cargar el archivo JSON
    
    df = pd.DataFrame(data)  # Crear un DataFrame a partir de los datos JSON
    
    # Unir todos los textos de los tweets en una sola cadena
    all_texts = ' '.join(df['text'])
    
    # Extraer los emojis del texto
    emojis = [char for char in all_texts if char in emoji.UNICODE_EMOJI_ENGLISH]
    
    # Contar el número de veces que aparece cada emoji
    emoji_counts = pd.Series(emojis).value_counts().head(10)
    
    result = list(emoji_counts.items())
    
    return result