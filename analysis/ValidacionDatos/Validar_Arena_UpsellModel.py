# Databricks notebook source
ls -ltr /dbfs/mnt/consumezoneprod/Argentina/Commercial/Arena/Upsellmodel

# COMMAND ----------

# MAGIC %md
# MAGIC ####Libraries
# MAGIC

# COMMAND ----------

# DBTITLE 0,Importamos librerias 
from matplotlib import pyplot as plt
from pyspark.sql.functions import *
from datetime import *

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read paths

# COMMAND ----------

# DBTITLE 0,Leemos el path
#Leemos del path
path_upsell_cz_prod = '/mnt/consumezoneprod/Argentina/Commercial/Arena/Upsellmodel'
df_upsell_cz_prod = spark.read.format('parquet').load(path_upsell_cz_prod) 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Set Variables 

# COMMAND ----------

#Fecha de hoy
current_date = date.today()

#Fecha de ayer
yesterday_date = current_date - timedelta(days=1)

#Cant. de filas filtrando la columna recommendation_ts por el dia anterior
cant_filas_lastDay = df_upsell_cz_prod.filter(to_date(col("LAST_UPDATE")) == current_date).count()

#Cant. de poc_id distintos filtrando la columna recommendation_ts por el dia anterior
cant_pocid_lastDay = df_upsell_cz_prod.filter(to_date(col("LAST_UPDATE")) == current_date).select(countDistinct("poc_code")).collect()[0][0]


# COMMAND ----------

# MAGIC %md
# MAGIC ####Send mail

# COMMAND ----------

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def SendEmail(recipient, subject, message):

    password = dbutils.secrets.get(scope = "KeyVault", key = "LASOfficeServiceUserPassword")
    username = dbutils.secrets.get(scope = "KeyVault", key = "LASOfficeServiceUserUser")
 
    #Configuramos el servidor SMTP
    server = smtplib.SMTP ('smtp.office365.com', 587) # check server and port with your provider
    server.ehlo()
    server.starttls()
    server.login(username, password) # insert secret name
    sender =  username
  
    #Creamos el mail con su contenido
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = recipient
    msg.attach(MIMEText(message, 'html'))
  
    #enviamos el mail
    server.sendmail(sender, recipient.split(','), msg.as_string() + "\n")
    server.close()

# COMMAND ----------

#Se crean las variables que van en el envio del mail 
#Si la cantidad de filas es igual a 0, se envia el mail
if(cant_filas_lastDay == 0):
    asunto       = "Arena UpsellModel - No se encontraron datos para la ultima fecha " + str(yesterday_date)
    destinatario = "mmarzora@quilmes.com.ar, mamalnic@quilmes.com.ar, mibassan@quilmes.com.ar"
    mensaje      = "Fecha: " + str(yesterday_date)  + "<br> Cantidad filas: " + str(cant_filas_lastDay)  + "<br> Cantidad poc_id distintos: " + str(cant_pocid_lastDay) 
    SendEmail(destinatario, asunto, mensaje)
