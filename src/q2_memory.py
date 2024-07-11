import pandas as pd
import json
from typing import List, Tuple
from pathlib import Path
from memory_profiler import profile

@profile
def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    with open(file_path, 'r') as f:
        data = json.load(f)  # Cargar el archivo JSON
    
    df = pd.DataFrame(data)  # Crear un DataFrame a partir de los datos JSON
    
    query = """
    SELECT emoji, COUNT(*) as emoji_count
    FROM (
        SELECT unnest(emojis) as emoji
        FROM df
    ) as subquery
    GROUP BY emoji
    ORDER BY emoji_count DESC
    LIMIT 10
    """
    result = psql.sqldf(query, locals())
    print(result)