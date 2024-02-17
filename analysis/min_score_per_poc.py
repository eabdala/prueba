# Databricks notebook source
# ls -ltr /dbfs/mnt/consumezone/Argentina/Datascience/SkuPrioritization/Score/min_rating_per_poc

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# COMMAND ----------

df_upsellModel = spark.read.format('parquet').load('/mnt/consumezoneprod/Argentina/Commercial/Arena/Upsellmodel')
#df_upsellModel.groupBy('poc_code','LAST_UPDATE').min('rating').show()

window = Window.partitionBy('poc_code','LAST_UPDATE').orderBy("rating")


df_upsellModel = df_upsellModel.filter(df_upsellModel.LAST_UPDATE >= '2023-05-12')
df_upsellModel = df_upsellModel.withColumn('row_number',row_number().over(window))
df_upsellModel = df_upsellModel.filter(df_upsellModel.row_number == 1).select('poc_code','HIGH_pid','rating','recommendation_ts','LAST_UPDATE')
df_upsellModel.display()


# COMMAND ----------

spark.conf.set('spark.sql.sources.partitionOverwriteMode','dynamic')
df_upsellModel.write.mode('overwrite').partitionBy('LAST_UPDATE').format('parquet').save('/mnt/consumezone/Argentina/Datascience/SkuPrioritization/Score/min_rating_per_poc')
