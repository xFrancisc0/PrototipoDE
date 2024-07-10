## Descripción General
Esto es un prototipo de proyecto de data engineering.

## Paso 1: Requerimientos
 Se requiere instalar Python 3.12.4. Para efectos de comodidad, este prototipo fue desarrollado con pyenv, un gestor de versiones de Python (Similar a nvm de NodeJS).
 Se debe ejecutar en directorio raiz: pip install -r requirements.txt

## Paso 2: "Datalake" prototipo a usar para almacenamiento de data no estructurada (Blobs).
 Para este prototipo se ocupara Google Drive como Datalake prototipo (Tengo 110TB).
 Se creó una cuenta de servicio en GCP con correo "prototipo-latam-service-accoun@proyecto-prototipo-latam.iam.gserviceaccount.com" (con permisos de lectura y escritura), la cual se compartio al directorio público de solo lectura del Datalake "https://drive.google.com/drive/folders/1nVbnD0pgYnAeym3lUoNna5cE7goN2U5N?usp=sharing". Recomiendo ampliamente verlo para ver su composicion.
 La composicion del Datalake es por zonas: bronze (Data recien extraida), gold (Data procesada).
 Omitiré la zona silver por el tamaño del proyecto (pequeño)
 Se creó credentials.json en base a esta cuenta de servicio para poder acceder a este Datalake con permisos de lectura y escritura. Son credenciales para la insersion, modificacion y eliminacion, no conlleva riesgo subirlo como .zip (Se requiere exportarlo en credentials.json en src/Servicios/GoogleDriveService/Credenciales/credentials.json en entorno local)
 A modo de ejercicio, la adjunción de los blob al Datalake serán en modo overwrite (No append).
 A modo de ejercicio, la adjuncion de los blob no será mediante Delta Lake, es decir el Datalake no tendrá versionamiento ya que no se ocupará spark y esto no está contemplado en el alcance de este prototipo (Procesamiento Spark en Databricks por ejemplo)
 PD: En un entorno real no usaria Google Drive, usaria o Azure Blob Storage + Gen 1 (Azure Data Lake Storage Gen2) o AWS Buckets S3 o GCP Bucket, entre otros. Crearía jobs en un workspace con un cluster dedicado, estos jobs los crearía como .py y luego importaría e instanciaría un objeto en los jobs asociados a la nube elegida para poder interactuar con el Datalake. Ocuparía pyspark para crear un dataframe de spark, realizaría filtros, reemplazos, esquematizaciones, entre otras transformaciones al Big Data en ese dataframe y posteriormente guardaría los blob como Delta Lake para tener mayor control sobre el Big Data. Las credenciales las administraria en alguna tecnología Vault.

## Paso 3: Jupyter Notebook
 Ejecutar jupyter lab y abrir challenge.ipynb
 (Si no abre, pip install --upgrade pip
    pip install --upgrade setuptools
    pip install --upgrade urllib3)

## Paso 4: Test unitarios
Para pruebas unitarias, se pueden compilar los *Tests.py que estan dentro de cada componente.
A modo de mejora, se podrían implementar pruebas automatizadas con CI.
En entorno real, las pruebas automatizadas podrían ser de varios tipos, con Mock, por numero de registros, filtros,
Respuestas HTTP, FTP, entre otros. Se hacen las pruebas automatizadas en CI y en CD se hace deploy al servidor de interes.
Ya sea QA (o UAT) o Prod.
Por ejemplo en vez de dejar aislado "EnviarPeticionHTTP" en AdvanaService pude haberlo dejado en un servicio global. Es cierto.
Sin embargo, lo implementé así para ilustrar de mejor manera el diagrama de componentes y de clases (Modelo C4)

## Paso 5: Consideraciones
5.1.- Los archivos se estan trabajando todos en .json. En un entorno real, ocuparía .parquet en las zonas silver y gold.
5.2.- En un entorno real usaria Azure Synapse Analytics o AWS Athena o GCP Big Query para leer como tablas externas los parquet. No subiria ni ejecutaria un HTTPRequest a Advana.
5.3.- Dado que solo se estan trabjaando los archivos en .json (5.1.-), habrán ocasiones donde en el código aparecerá "application/json". Esto sería parametrizado completamente en entorno productivo.
5.4.- Se ocupó inyección de dependencias (Patron de diseño creacional) para inyectar los servicios en un orquestador. Esto lo hice para volver el código mucho mas amigable.
5.5.- Hay métodos que tienen muchas instrucciones. En un entorno real, haría refactorización con clean code e intentar basarme en los principios SOLID.
5.6.- Dado a que hay solo hay uno y debe haber solo un orquestador en todo el sistema, pude haberlo vuelto Singleton (Patron de diseño creacional).
5.7.- Como fue explicado a priori, el Datalake que se esta ocupando es google drive. El motivo es que tengo en mi cuenta 110TB y me es mas factible aprovechar este espacio de almacenamiento a ocupar el free tier de algun cloud.
5.8.- En un proyecto real utilizaría pyspark en un cluster de Spark en Databricks, como este es un prototipo, se ocupará pandas.

