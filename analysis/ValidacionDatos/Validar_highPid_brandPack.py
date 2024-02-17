# Databricks notebook source
# MAGIC %md
# MAGIC ####Proceso de High Pids con Brand-Pack distintos

# COMMAND ----------

#Leemos el path
df_products = spark.read.format('parquet').load('/mnt/consumezone/Argentina/Datascience/SkuPrioritization/promotor/Features/products')

#Filtramos por productos CZA
df_products_t = df_products.filter(df_products.business == 'CZA')

#Dejamos solo las filas que tengan una combinacion distinta de HIGH_pid y brand_pack
# Un mismo high_pid debe pertenecer a un solo brand pack 
df_products_t = df_products_t.select('HIGH_pid','brand_pack').distinct()

#Agrupamos el dataframe anterior por high_pid, contamos la cantidad de filas, y filtramos los que tegan en count > 1
# Con count > 1 podemos ver los high_pid que tienen mas de un brand pack distinto
df_products_t = df_products_t.groupBy('HIGH_pid').count()
df_products_t = df_products_t.filter('count > 1')


#Cambiamos nombres a las columnas
#df_dim_prod_t = df_dim_prod_t.withColumnRenamed('HIGH_pid','HIGH_pid_2')

#Hacemos join con products para obtener todos lo high pid que tiene brand-pack distintos, con el codigo pid
df_products_t = df_products_t.join(df_products, on = 'HIGH_pid')
df_products_t = df_products_t.select('SKU_ID','HIGH_pid','product_desc','brand_family','brand_pack').orderBy('brand_family')



# COMMAND ----------

# MAGIC %md
# MAGIC ####Envio de mail

# COMMAND ----------

# DBTITLE 0,Envio de mail
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from io import StringIO, BytesIO

password = dbutils.secrets.get(scope = "KeyVault", key = "LASOfficeServiceUserPassword")
username = dbutils.secrets.get(scope = "KeyVault", key = "LASOfficeServiceUserUser")

#Funcion envio de mail
def SendEmail(sender, recipient, subject, message, csv):
  
    # Creamos el mail con su contenido
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = recipient
    mensaje_mail = MIMEText(message, 'html')
  
    # Adjuntamos csv
    archivo_csv = MIMEApplication(csv,encoding='utf-8',sep=";")
    archivo_csv.add_header("content-disposition", "attachment", filename="Productos con brand-pack distintos.csv")
    msg.attach(mensaje_mail)
    msg.attach(archivo_csv)
  
    # Configuramos el servidor SMTP y enviamos el mail
    server = smtplib.SMTP ('smtp.office365.com', 587) # check server and port with your provider
    server.ehlo()
    server.starttls()
    server.login(username, password) # insert secret name
  
    # Enviamos mail y cerramos la conexion con el servidor
    server.sendmail(emisor, destinatario.split(','), msg.as_string() + "\n")
    server.close()

# COMMAND ----------

# Guardamos en una variable la cantidad de filas del dataframe resultante
cantidadFilas = df_products_t.count()

# Si se encuentras filas se manda el mail
if cantidadFilas > 0:
  
    # Se transforma el dataframe a html y csv para poder enviarlo en el mail
    df_html = df_products_t.toPandas().to_html()
    df_csv  = df_products_t.toPandas().to_csv()
  
    subject       = "High Pids con Brand-Pack distintos"
    sender       =  username
    recipient = "octapica@quilmes.com.ar, mmarzora@quilmes.com.ar, mamalnic@quilmes.com.ar, gaacosta@quilmes.com.ar"
    message      = df_html
    csv          = df_csv
    SendEmail(sender, recipient, subject, message, csv)
