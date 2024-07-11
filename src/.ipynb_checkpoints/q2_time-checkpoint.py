from Utilidades.GlobalUtility import spark
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType, StringType, ArrayType
import re
from typing import List, Tuple

"""
Recordatorio spark

#Seleccionar ciertas columnas
df = df.select("columna1", "columna2", "columna3") 

#Renombrar columnas
df = df.select(col("A").alias("AX"), col("B").alias("BX"))

#Crear columnas
from pyspark.sql.functions import col, lit
df = df.withColumn("nueva_columna", lit("valor_fijo"))  #Con valor fijo
df = df.withColumn("nueva_columna", col("columna_existente") * 2)  #Crear desde otra columna

#Eliminar columna
df = df.drop("columna_a_eliminar")

#Filtrar 
df = df.filter(col("columna") > 5)
df = df.filter(col("columna").isNotNull() & (col("columna") != ""))

#Ordenar
df = df.orderBy("columna", ascending=False)

#Obtener primeros 10 elementos
df.limit(10).show()

#Eliminar duplicados por columna
#df = df.dropDuplicates(["columna"])

#Funciones customizadas para DF (UDF)

def funcion(texto):
    return texto.upper()

ejemplo_udf_func = udf(funcion, StringType())  #Creacion de udf el cual retorna Strings

# Aplicar el UDF en nueva_columna y crear una nueva columna texto_mayusculas con el resultado del UDF hacia una col
df = df.withColumn("texto_mayusculas", ejemplo_udf_func(col("nombre_columna")))
#Si hay error con el UDF, lo que hay que hacer es hacer una prueba de funcion en funcion, ejemplo: print(funcion("texto_a_probar"))

"""    
def contar_emojis(texto):
    # Verifica si el texto es None, en cuyo caso retorna 0
    if texto is None:
        return 0
    
    try:
        # Expresión regular para detectar emojis
        emoji_pattern = re.compile(
            "[\U0001F600-\U0001F64F"  # emoticonos
            "\U0001F300-\U0001F5FF"  # símbolos y pictogramas
            "\U0001F680-\U0001F6FF"  # transportes y mapas
            "\U0001F700-\U0001F77F"  # símbolos alquímicos
            "\U0001F780-\U0001F7FF"  # geometría
            "\U0001F800-\U0001F8FF"  # símbolos de la calculadora
            "\U0001F900-\U0001F9FF"  # emojis adicionales
            "\U0001FA00-\U0001FA6F"  # símbolos y pictogramas diversos
            "\U0001FA70-\U0001FAFF"  # diversos
            "\U00002702-\U000027B0"  # símbolos adicionales
            "\U000024C2-\U0001F251"  # más emojis
            "]+",
            flags=re.UNICODE
        )
        # Encuentra todos los emojis en el texto
        emojis = emoji_pattern.findall(texto)
        return len(emojis)
    except Exception as e:
        # Imprime el error y retorna 0 si ocurre una excepción
        print("Hubo un error al contar emojis")
        print("El texto que lo provoco fue:", texto)
        print(f"El error es: {e}.")
        return 0

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    # Leer el archivo JSON en un DataFrame de PySpark
    df = spark.read.json(file_path)
    print("Mostrar df inicial")
    df.show()

    print("Seleccionar df_content")
    df_content = df.select("content", "renderedContent", "quotedTweet.content", "quotedTweet.renderedContent","user.description", "user.displayname", "user.rawDescription")
    df_content.show()

    print("Renombrar columnas df_content")
    df_content = df.select(
    col("content").alias("content"), 
    col("renderedContent").alias("renderedContent"), 
    col("quotedTweet.content").alias("quotedTweet_content"), 
    col("quotedTweet.renderedContent").alias("quotedTweet_renderedContent"),
    col("user.description").alias("user_description"), 
    col("user.displayname").alias("user_displayname"), 
    col("user.rawDescription").alias("user_rawDescription"))
    df_content.show(truncate=False)

    print("hola")
    print("df_transformed_contaremojis")
    contar_emojis_udf = udf(contar_emojis, IntegerType()) #Se crea un udf para ocuparlo en df_content. Este permitira contar los emojis de los campos a priori seleccionados.
    

    try:
        df_transformed_contaremojis = df_content.withColumn("content_emoji_count", contar_emojis_udf(col("content")))
        df_transformed_contaremojis = df_transformed_contaremojis.withColumn("renderedContent_emoji_count", contar_emojis_udf(col("renderedContent")))
        df_transformed_contaremojis = df_transformed_contaremojis.withColumn("quotedTweet_content_emoji_count", contar_emojis_udf(col("quotedTweet_content"))) 
        df_transformed_contaremojis = df_transformed_contaremojis.withColumn("quotedTweet_renderedContent_emoji_count", contar_emojis_udf(col("quotedTweet_renderedContent")))
        df_transformed_contaremojis = df_transformed_contaremojis.withColumn("user_description_emoji_count", contar_emojis_udf(col("user_description"))) 
        df_transformed_contaremojis = df_transformed_contaremojis.withColumn("user_displayname_emoji_count", contar_emojis_udf(col("user_displayname"))) 
        df_transformed_contaremojis = df_transformed_contaremojis.withColumn("user_rawDescription_emoji_count", contar_emojis_udf(col("user_rawDescription")))
        df_transformed_contaremojis.show(truncate=False)
    except Exception as e:
        # Imprime el error y retorna 0 si ocurre una excepción
        print(f"Error en contar_emojis: {e}")
    
    
    return True