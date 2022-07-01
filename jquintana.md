# José Quintana - Examen Final
## Dado el siguiente modelo analitico.
 
El departamento de Marketing, desea enviar una campana personalizada a cada uno de los clientes, promocionando el producto que mas veces han solicitado. 
Por lo tanto el requerimiento ingresa al departamento de Ingeniería Analitica solicitado que se genere una tabla, con las siguientes columnas:

- Codigo del cliente (rowidcliente)
- Nombre del producto mas comprado
- Fecha de la ultima compra
- Correo del cliente

Las tablas del modelo analitico proveen los datos transaccionales. 
Sin embargo los correos electrónicos han sido proporcionados por medio de un archivo CSV el cual se encuentra adjunto.

## Lineamientos generales:

- El nombre de usuario debe ser la letra inicial del primer nombre y su apellido Ejm: Agustin Martinez (amartinez)
- Cada pipeline de ADF debe anteponer el nombre de usuario seguido de pipeline Ejm: amartinez_pipeline
- Los archivos ingestados en ADL2 deben colocarse en RAW en una carpeta con el nombre de usuario
- Los notebooks de Spark deben ser nombrados  {usuario}_notebook
- La tabla de resultados debe almacenarse en un POOL SQL, en el esquema default con el esquema "tbl_{usuario}"

Finalmente, cada participante debera elaborar un archivo Markdown en Github con acceso publico. 
Por su puesto en el documento debe obviarse datos sensibles como claves etc.
El documento Markdown debera contener el proceso tecnico que el participante ha seguido con un orden logico 
y el link del proyecto Githab debera ser registrado como entregable de esta tarea.

# Desarrollo del Exámen
## :point_right: Crear Servicio Vinculado 
Se crea un servicio vinculado en este caso ya tenias creado por lo que lo reutilizamos, previamente habiamos instalado un Integration Runtime en la máquina o server que tenga acceso a la base de datos y configuramos con el Key de Azure.
![image](https://user-images.githubusercontent.com/108035896/175313416-40166d78-a1c9-459b-9e3d-6afbaf3726fb.png)

## :point_right: Crear Data Source de Origen (en este caso lo reutilizamos) SourceDataset_xer
Se crea el Data Origen con un parametro que es el nombre de la tabla
![image](https://user-images.githubusercontent.com/108035896/175315801-f068f904-6ae8-4bb1-bfc7-59315e97a2d1.png)
![image](https://user-images.githubusercontent.com/108035896/175316037-c58f1063-1954-45ec-b710-08b8078e5df0.png)

## :point_right: Crear un Data Source Destino: DestinationDataset_examen
Se crea un Data source con 2 parametros para enviar el nombre de la tabla y el usuario
- **Ruta acceso:** synapse/workspaces/synapsecapacitacion/warehouse/raw/@{dataset().vUsuario}
- **Nombre Tabla:** @{dataset().vTabla}.parquet
![image](https://user-images.githubusercontent.com/108035896/175183432-5c75bde7-a37d-4b14-b725-756049d594e5.png)


## :point_right: Crear Pipeline y nombrarlo según lo solicitado
![image](https://user-images.githubusercontent.com/108035896/175182084-2fbf91af-9f69-4524-b5a9-7d4504d542fb.png)

# Copiar tablas 
## :point_right: Usamos un lookup para buscar el nombre de las tablas
- Cliente
- Factura
- facturaproducto
- producto

Usamos el siguiente query de SQL
```sql
select 
table_name as tabla
from information_schema. tables
where
table_schema='dwh' 
```
![image](https://user-images.githubusercontent.com/108035896/175184074-b139c0e1-84e6-4f3f-bcae-b8e544bbde2b.png)

## :point_right: Usamos un Foreach para copiar cada tabla 
Como elemento de la lista usamos los nombnres de la tablas que se obtienen del lookup: @activity('Tablas').output.value
![image](https://user-images.githubusercontent.com/108035896/175184623-c1b2022a-ffca-492f-8871-6dd0445eb619.png)

Creamos la variable para el nombre del usuario
![image](https://user-images.githubusercontent.com/108035896/175184765-643155d3-538f-4c9b-b4fa-15d93280dca3.png)

## :point_right: Actividad Copiar Tabla
Dentro del For Each cramos una activida para copiar la tabla usando el Datasource creado y le pasamos los 2 parametros
![image](https://user-images.githubusercontent.com/108035896/175184970-fc95944d-744a-4d78-86b8-e894015ee55b.png)


## :point_right: Resultado de la copia de tablas
![image](https://user-images.githubusercontent.com/108035896/175198809-636af7a2-b383-4f17-9e02-79ebd96bcc7b.png)

## :point_right: Creamos un Notebook jquintana_notebook_cargar_csv para copiar los datos del scv a un archivo parquet 
```phyton
## Leer archivo csv
dfMails = spark.read.load('abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/clientes_correos.csv', format='csv'
## If header exists uncomment line below
##, header=True
)
display(dfMails.limit(10))
dfMails.printSchema()

## ArchivoParquet
vPathResultado = 'abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/jquintana/mails.parquet'
dfMails.repartition(1).write.mode("overwrite").parquet(vPathResultado)
```
![image](https://user-images.githubusercontent.com/108035896/175202512-6c37366f-8796-4d78-82fb-7cf0968a0879.png)

## :point_right: Creamos un Notebook jquintana_notebook_cargar_csv para obtener los datos de las tablas y crear la tabla resultado
![image](https://user-images.githubusercontent.com/108035896/175213995-fdc25eea-9ba8-4361-8320-298742d6d8a0.png)

Los pasos realizados son:
**1. Obtenemos los datos de los archivos parquet**
```python
## Obtenemos los datos de los archivos parquet     
## Cliente
vPathCliente = 'abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/jquintana/cliente.parquet'
dfCliente = spark.read.load(vPathCliente, format='parquet')
## Factura
vPathFactura = 'abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/jquintana/factura.parquet'
dfFactura = spark.read.load(vPathFactura, format='parquet')
## Producto x Factura
vPathFacProducto = 'abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/jquintana/facturaproducto.parquet'
dfFacProducto = spark.read.load(vPathFacProducto, format='parquet')
## Producto
vPathProducto = 'abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/jquintana/producto.parquet'
dfProducto = spark.read.load(vPathProducto, format='parquet')
## Mail
vPathMail = 'abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/jquintana/mails.parquet'
dfMail = spark.read.load(vPathMail, format='parquet')
```
**2. Creamos tablas temporales**
```python
##Tablas Temporales
dfCliente.createOrReplaceTempView("tbl_cliente")
dfFactura.createOrReplaceTempView("tbl_factura")
dfFacProducto.createOrReplaceTempView("tbl_facturaproducto")
dfProducto.createOrReplaceTempView("tbl_producto")
dfMail.createOrReplaceTempView("tbl_mail")
```
**3. Obtenemos el total de cada producto por cliente y creamos tabla temporal**
```python
##SQL Cantidad de producto x cliente
vSql = """
SELECT c.rowidcliente,m._c1 correo,p.producto,max(d.fecha) fecha_ult_compra, count(p.rowidproducto) cantidad_Producto,sum(d.valorventaproducto) ValorTotProducto
FROM tbl_cliente c 
INNER JOIN tbl_factura f on c.rowidcliente = f.rowidcliente
INNER JOIN tbl_facturaproducto d on d.rowidfactura = f.rowidfactura
INNER JOIN tbl_producto P on p.rowidproducto = d.rowidproducto
INNER JOIN tbl_mail m on m._c0 = c.rowidcliente
GROUP BY c.rowidcliente,p.producto,m._c1
"""
dfTotProdxCliente = spark.sql(vSql)
##display(dfTotProdxCliente)
## tala temporal
dfTotProdxCliente.createOrReplaceTempView("tbl_TotProdxCliente")
```
**4. Obtenemos la cantidad máxima de producto por cliente**
```python
##SQL  máxima de producto por cliente
vSql2 = """
SELECT txp.rowidcliente, max(txp.cantidad_Producto) Maxcantidad_Producto, max(txp.ValorTotProducto) MaxValorTotProducto,max(fecha_ult_compra) Maxfecha_ult_compra
FROM tbl_TotProdxCliente txp
GROUP BY txp.rowidcliente
"""

dfTotProdxCliente = spark.sql(vSql2)
dfTotProdxCliente.createOrReplaceTempView("tbl_MaxProdxCliente")
## display(dfResultado)
```
**5. Obtenemos el resultado solciitado y creamos la tabla en el POOL SQL**
```python
##SQL Creamos la tabla con el resultado solicitado uniendo las tablas anteriores
vSql3 = """
SELECT txp.rowidcliente,txp.producto,txp.correo,txp.fecha_ult_compra, txp.cantidad_Producto ,txp.ValorTotProducto
FROM tbl_TotProdxCliente txp 
inner join tbl_MaxProdxCliente mxp on mxp.rowidcliente = txp.rowidcliente 
and mxp.Maxcantidad_Producto = txp.cantidad_Producto 
and txp.ValorTotProducto = mxp.MaxValorTotProducto
and txp.fecha_ult_compra = mxp.Maxfecha_ult_compra
"""
"""
## Creamos tabla en Pool SQL
dfResultado = spark.sql(vSql3)
dfResultado.write.mode("overwrite").saveAsTable("default.tbl_jquintana")

## ArchivoParquet
vPathResultado = 'abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/jquintana/tbl_jquintana.parquet'
dfResultado.repartition(1).write.mode("overwrite").parquet(vPathResultado)
```
## :point_right: Incluimos en el Pipeline los notebooks 
Se añaden al pipeline los notebooks creados y se asigna el Grupo Spark para procesarlos
![image](https://user-images.githubusercontent.com/108035896/175215993-f06a8caf-261b-4359-9cca-007f45118a11.png)


## :point_right: Verificamos el resultado consultando la tabla
```sql
SELECT TOP (100) [rowidcliente]
,[producto]
,[correo]
,[fecha_ult_compra]
 FROM [default].[dbo].[tbl_jquintana]
 ```
 ![image](https://user-images.githubusercontent.com/108035896/175214767-a8229980-6e46-4660-870d-abf235419228.png)

También se valida el archivo parquet creado
![image](https://user-images.githubusercontent.com/108035896/175215396-9389a9ef-d716-422e-85c6-bc6b7aca089a.png)

## :point_right: Pipeline Completo
Se muestra el Pipeline final con todo el flujo 
![image](https://user-images.githubusercontent.com/108035896/175220061-5596bf96-f603-4667-96fd-51574320a7d3.png)


## :point_right: Ejecución del Pipeline satisfactoriamente
![image](https://user-images.githubusercontent.com/108035896/175219914-dd1e0127-788b-4329-bc98-b212265f7d7f.png)

