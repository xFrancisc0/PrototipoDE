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
    
Para pruebas unitarias: python -m unittest discover -s . -p "*Test.py"