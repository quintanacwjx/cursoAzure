# Curso Azure
Este es un archivo de prueba
## Subtitulo ejemplo 

Explicacion de etapas 
- Etapa 1
- Etapa 2
- Etapa 3

Codigo para leer un archivo JSON	

``` phyton
from pyspark.sql.functions import explode
df = spark.read.load('abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/clientes_correos.json', format='json')
display(df.limit(10))
df.show(5)
df.printSchema()
df1 = df.select(explode(df.data)) ## abre el array
df1.printSchema()
df1.show(5)
df2 = df1.select(df1["col"].getItem(0).alias("rowclientid"),df1["col"].getItem(1).alias("correo")) ## se obtiene cada columna del archivo
df2.show(5)
```
