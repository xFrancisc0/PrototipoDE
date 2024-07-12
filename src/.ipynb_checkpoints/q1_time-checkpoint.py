from typing import List, Tuple
from datetime import datetime
from Utilidades.GlobalUtility import spark
    
def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    print("Q1: Iniciando")
    df = spark.read.json(file_path)
    df.show(truncate=False)

    df.createOrReplaceTempView("dataset")

    result = spark.sql("""
        SELECT date, 
               user.username AS username
        FROM dataset
        ORDER BY retweetCount DESC
        LIMIT 10
    """)

    spark.catalog.dropTempView("dataset")

    return [ (row.date, row.username) for row in result.collect() ]