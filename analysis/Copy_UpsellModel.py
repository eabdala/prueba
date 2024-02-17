# Databricks notebook source
# ls -ltr /dbfs/mnt/consumezone/Argentina/Datascience/SkuPrioritization/Arena/UpsellModel/

# COMMAND ----------

# MAGIC %md
# MAGIC ####Widgets

# COMMAND ----------

import pandas as pd

dbutils.widgets.text("dias", "14")
dias = int(getArgument("dias"))
print(dias)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Librerias importadas

# COMMAND ----------

from datetime import date, datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ####Lectura de paths

# COMMAND ----------

# Leemos del path
path_upsell = "/mnt/consumezoneprod/Argentina/Commercial/Arena/Upsellmodel"
df_upsell = spark.read.format("parquet").load(path_upsell)

path_hpid = "/mnt/consumezone/Argentina/Datascience/SkuPrioritization/promotor/Features/hpid_info"
df_hpid = spark.read.format("parquet").load(path_hpid)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Procesamiento de los datos

# COMMAND ----------

current_date = date.today()

# Filtramos upsell por la fecha actual
df_upsell = df_upsell.filter(
    df_upsell.CREATED_DATE >= current_date - timedelta(days=dias)
)

# Hacemos join hpid_info, para obtener la columna business
df_join = df_upsell.join(df_hpid, on="HIGH_pid", how="left").select(
    "poc_code",
    "HIGH_pid",
    "rating",
    "quantity",
    "recommendation_id_type",
    "type",
    "recommendation_ts",
    "model_type",
    "supplemental",
    "pid",
    "group_name",
    "group_description",
    "LAST_UPDATE",
    "CREATED_DATE",
    "business",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Esrituta en el DL

# COMMAND ----------

# Escribimos los datos
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df_join.write.mode("overwrite").partitionBy("CREATED_DATE", "business").format(
    "parquet"
).save("/mnt/consumezone/Argentina/Datascience/SkuPrioritization/Arena/UpsellModel")

# COMMAND ----------
