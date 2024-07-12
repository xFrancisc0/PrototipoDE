from Utilidades.GlobalUtility import spark
from pyspark.sql.functions import udf, size, col
from pyspark.sql.types import StringType, ArrayType
from typing import List, Tuple

def extract_usernames(users_list):
    return [user['username'] for user in users_list] if users_list else []

# Definir la UDF
def extract_usernames_udf():
    return udf(extract_usernames, ArrayType(StringType()))

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    # Leer el archivo JSON en un DataFrame de PySpark
    df = spark.read.json(file_path)
    print("Q3: Mostrar df inicial")
    df.show(truncate=False)

    #print("Seleccionar df_content")
    df_content = df.select("user.username", "mentionedUsers")
    df_content = df.select(col("user.username").alias("username"), col("mentionedUsers").alias("mentionedUsers"))
    #df_content.show()

    #print("Q3: Eliminar de df_content los mentionedUsers NULL")
    df_transformed_filtrarusuariosquehacenmenciones = df_content.filter(col("mentionedUsers").isNotNull())
    #df_transformed_filtrarusuariosquehacenmenciones.show()

    #print("Q3: Guardar numero de mencionados por usuario en otra columna")
    df_transformed_crearcamponumeromenciones = df_transformed_filtrarusuariosquehacenmenciones.withColumn("numeroMenciones", size(col("mentionedUsers")))
    #df_transformed_crearcamponumeromenciones.show()

    #print("Q3: Eliminar columna mentionesUsers para solo dejar usuario y numeroMenciones")
    df_transformed_eliminarcolumnaMentionedUsers = df_transformed_crearcamponumeromenciones.drop("mentionedUsers")
    #df_transformed_eliminarcolumnaMentionedUsers.show()

    #print("Q3: Ordenar por numeroMenciones desc")
    df_transformed_ordenarpornumeromencionesdesc = df_transformed_eliminarcolumnaMentionedUsers.orderBy("numeroMenciones", ascending=False)
    #df_transformed_ordenarpornumeromencionesdesc.show()

    #print("Q3: Seleccionar solo los primeros 10")
    df_transformed_seleccionarprimerosdiez = df_transformed_ordenarpornumeromencionesdesc.limit(10)
    #df_transformed_seleccionarprimerosdiez.show()

    return [ (row.username, row.numeroMenciones) for row in df_transformed_seleccionarprimerosdiez.collect() ]