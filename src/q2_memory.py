import pandas as pd
from typing import Counter, List, Tuple
from pathlib import Path
from memory_profiler import profile
import json
import re

def extract_emojis(text):
        # Regex para encontrar emojis en un texto
    emoji_pattern = re.compile(
        "["  
        "\U0001F600-\U0001F64F"  # Emojis de caritas
        "\U0001F300-\U0001F5FF"  # Emojis de símbolos y objetos
        "\U0001F680-\U0001F6FF"  # Emojis de transporte y mapas
        "\U0001F700-\U0001F77F"  # Emojis de símbolos de alchemia
        "\U0001F780-\U0001F7FF"  # Emojis de geometría
        "\U0001F800-\U0001F8FF"  # Emojis de símbolos adicionales
        "\U0001F900-\U0001F9FF"  # Emojis de más símbolos
        "\U0001FA00-\U0001FA6F"  # Emojis de símbolos diversos
        "\U0001FA70-\U0001FAFF"  # Emojis de símbolos y pictogramas adicionales
        "\U00002700-\U000027BF"  # Emojis de varios símbolos
        "\U000024C2-\U0001F251"  # Emojis adicionales
        "]+",
        flags=re.UNICODE
    )
    return emoji_pattern.findall(text)

def extraer_emojis_de_columnas(fila):
    emojis = []
    columnas = [
        fila.get('content', ''),
        fila.get('renderedContent', ''),
        fila.get('quotedTweet', {}).get('content', '') if fila.get('quotedTweet') else '',   #Puede ser nulo
        fila.get('quotedTweet', {}).get('renderedContent', '') if fila.get('quotedTweet') else '', #Puede ser nulo
        fila.get('user', {}).get('description', ''),
        fila.get('user', {}).get('displayname', ''),
        fila.get('user', {}).get('rawDescription', ''),
    ]
    
    for columna in columnas:
        emojis.extend(extract_emojis(columna))
    
    return emojis

@profile
def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    with open(file_path, 'r') as f:
        data = json.load(f)  # Cargar el archivo JSON
    
    df = pd.DataFrame(data)  # Crear un DataFrame a partir de los datos JSON
    
    all_emojis = []
    
    # Aplicar la función a cada fila del DataFrame
    for _, fila in df.iterrows():
        all_emojis.extend(extraer_emojis_de_columnas(fila))
    
    # Contar emojis y obtener los 10 más comunes
    emoji_counts = Counter(all_emojis)
    top_10_emojis = emoji_counts.most_common(10)
    
    return top_10_emojis