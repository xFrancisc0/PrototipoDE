from Utilidades.GlobalUtility import spark
from pyspark.sql.functions import col, explode, split, lit, regexp_extract, expr, count as spark_count
from pyspark.sql.types import IntegerType
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

#Acceder a una lista anidada de objetos
data = [
    ({"A": {"B": {"C": {"D": [1,2,3,4]}}}},),
    ({"A": {"B": {"C": {"D": [5,6,7,8]}}}},)
]

df = spark.createDataFrame(data, ["json_column"])

# Acceder a la lista D dentro de json_column
df_d = df.withColumn("D", col("json_column").getField("A")
                                .getField("B")
                                .getField("C")
                                .getField("D"))

                                
#Funciones customizadas para DF (UDF)

def funcion(texto):
    return texto.upper()

ejemplo_udf_func = udf(funcion, StringType())  #Creacion de udf el cual retorna Strings

# Aplicar el UDF en nueva_columna y crear una nueva columna texto_mayusculas con el resultado del UDF hacia una col
df = df.withColumn("texto_mayusculas", ejemplo_udf_func(col("nombre_columna")))
#Si hay error con el UDF, lo que hay que hacer es hacer una prueba de funcion en funcion, ejemplo: print(funcion("texto_a_probar"))
## Si se requiere aplicar udf junto con pyspark o incluso con pandas, se necesitara el paquete "pyarrow"

"""    

# Patrón de expresión regular para capturar emojis
emoji_pattern = "[\U0001F600-\U0001F64F" \
                "\U0001F300-\U0001F5FF" \
                "\U0001F680-\U0001F6FF" \
                "\U0001F700-\U0001F77F" \
                "\U0001F780-\U0001F7FF" \
                "\U0001F800-\U0001F8FF" \
                "\U0001F900-\U0001F9FF" \
                "\U0001FA00-\U0001FA6F" \
                "\U0001FA70-\U0001FAFF" \
                "\U00002700-\U000027BF" \
                "\U000024C2-\U0001F251" \
                "]+"

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    print("Q2: Iniciando")
    # Leer el archivo JSON en un DataFrame de PySpark
    df = spark.read.json(file_path)


    # Seleccionar las columnas de interés
    df_content = df.select(
        col("content"), 
        col("renderedContent"), 
        col("quotedTweet.content").alias("quotedTweet_content"), 
        col("quotedTweet.renderedContent").alias("quotedTweet_renderedContent"),
        col("user.description").alias("user_description"), 
        col("user.displayname").alias("user_displayname"), 
        col("user.rawDescription").alias("user_rawDescription")
    )

    # Rellenar valores nulos con cadenas vacías
    df_transformed_fillna = df_content.fillna("", subset=df_content.columns)

    # Extraer emojis y contar ocurrencias
    columns_to_explode = [
        "content",
        "renderedContent",
        "quotedTweet_content",
        "quotedTweet_renderedContent",
        "user_description",
        "user_displayname",
        "user_rawDescription"
    ]

    # Extraer emojis y contar ocurrencias
    emoji_dfs = []
    for column_name in columns_to_explode:
        df_emoji = (
            df_transformed_fillna
            .withColumn(column_name, regexp_extract(col(column_name), emoji_pattern, 0))  # Extrae emojis de la columna
            .select(explode(split(col(column_name), "")).alias("emoji"))  # Divide la cadena de emojis en emojis individuales
            .filter(col("emoji") != "")  # Elimina las filas con cadenas vacías en la columna de emojis
            .withColumn("numeroCoincidencias", lit(1))  # Crea una columna de conteo con valor 1
            .groupBy("emoji")  # Agrupa por emoji
            .agg(spark_count("numeroCoincidencias").cast(IntegerType()).alias("numeroCoincidencias"))  # Cuenta las ocurrencias
        )

        emoji_dfs.append(df_emoji)

    # Combinar todos los DataFrames de emojis en uno solo
    df_combined = emoji_dfs[0]
    for df in emoji_dfs[1:]:
        df_combined = df_combined.union(df)

    # Agrupar por emoji y contar las ocurrencias totales
    df_aggregated = df_combined.groupBy("emoji") \
                              .agg(spark_count("numeroCoincidencias").cast(IntegerType()).alias("numeroCoincidencias"))
    
    # Convertir el DataFrame final a una lista de tuplas (emoji, conteo)
    result = [(row.emoji, row.numeroCoincidencias) for row in df_aggregated.collect()]

    # Mostrar resultados de emojis y sus conteos
    for emoji, count in result:
        print(f"{emoji} | {count}")

    return result