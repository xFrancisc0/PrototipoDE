import pandas as pd
import json
from typing import List, Tuple

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    with open(file_path, 'r') as f:
        data = json.load(f)  # Cargar el archivo JSON
    
    df = pd.DataFrame(data)  # Crear un DataFrame a partir de los datos JSON
    df['mentionedUsers'] = df['mentionedUsers'].apply(lambda x: [user['username'] for user in x] if x else [])
    mentions_df = df.explode('mentionedUsers')
    
    query = """
    SELECT mentionedUsers as user, COUNT(*) as mention_count
    FROM mentions_df
    GROUP BY user
    ORDER BY mention_count DESC
    LIMIT 10
    """
    result = psql.sqldf(query, locals())
    return list(result.itertuples(index=False, name=None))