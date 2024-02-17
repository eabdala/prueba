# Databricks notebook source
# MAGIC %md
# MAGIC ####Importar librerias

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC ####Lectura de paths
# MAGIC

# COMMAND ----------

# DBTITLE 0,Lectura de paths
#Path upsellModel
path_upsell = '/mnt/consumezoneprod/Argentina/Commercial/Arena/Upsellmodel'
df_upsell   =  spark.read.format('parquet').load(path_upsell)

#Path productSlim
path_prodslim = '/mnt/consumezoneprod/Argentina/Commercial/Arena/Productslim'
df_prodslim   = spark.read.format('parquet').load(path_prodslim) 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Duplicados ProducSlim

# COMMAND ----------

duplicates_productSlim = df_prodslim.groupBy('pid').count().alias('count').filter(col('count') > 1)
duplicates_productSlim.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Envio de mail

# COMMAND ----------

# DBTITLE 0,Envio de mail
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


def SendEmail(recipient, subject, message, attached_1):
  
    password = dbutils.secrets.get(scope = "KeyVault", key = "LASOfficeServiceUserPassword")
    username = dbutils.secrets.get(scope = "KeyVault", key = "LASOfficeServiceUserUser")

    sender = username

    # Creamos el mail con su contenido
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = recipient

    message_html = MIMEText(message, 'html')
    msg.attach(message_html)

    # Adjuntamos csv
    file_attached_1 = MIMEApplication(attached_1,encoding='utf-8',sep=";")
    file_attached_1.add_header("content-disposition", "attachment", filename="Duplicados_productSlim.csv")
    msg.attach(file_attached_1)
  
  
    # Configuramos el servidor SMTP
    server = smtplib.SMTP ('smtp.office365.com', 587) # check server and port with your provider
    server.ehlo()
    server.starttls()
    server.login(username, password) # insert secret name
  
    # Enviamos el mail y cerramos la conexion  con el servidor
    server.sendmail(sender, recipient.split(','), msg.as_string() + "\n")
    server.close()
  

# COMMAND ----------

cantidadFilas  = duplicates_productSlim.count()

# Si se encuetran datos se envia el mail
if (cantidadFilas > 0):
    # csv para enviar en el mail
    csv = duplicates_productSlim.toPandas().to_csv()
  
    # Parametros del mail
    subject    =  "Check ProducSlim"
    recipient  =  "octapica@quilmes.com.ar, mmarzora@quilmes.com.ar, mamalnic@quilmes.com.ar, gaacosta@quilmes.com.ar"
    message    =  "Duplicados en ProductSlim: " + str(cantidadFilas) + "<br>"
    SendEmail (recipient, subject, message, csv)
