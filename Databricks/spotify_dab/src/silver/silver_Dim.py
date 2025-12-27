# Databricks notebook source
# MAGIC %md
# MAGIC ##### DimUser

# COMMAND ----------

# MAGIC %md
# MAGIC ##### AutoLoader

# COMMAND ----------

# MAGIC
# MAGIC %run /Workspace/Users/ravimohan878@gmail.com/spotify_dab/utils/transformation.py

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys

# project_path = os.path.join(os.getcwd(), '..','..')
# sys.path.append(project_path)
# from utils.transformation import reusable

# COMMAND ----------

df_user = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@vnspotify.dfs.core.windows.net/DimUser/checkpoint")\
                .option("schemaEvolutionMode","addNewColumns")\
                .load("abfss://bronze@vnspotify.dfs.core.windows.net/DimUser")


# COMMAND ----------

df_user= df_user.withColumn("user_name",upper(col("user_name")))


# COMMAND ----------

df_user_obj = reusable()
df_user = df_user_obj.dropColumns(df_user,['_rescued_data'])
df_user = df_user.drop_duplicates(['user_id'])


# COMMAND ----------

df_user.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@vnspotify.dfs.core.windows.net/DimUser/checkpoint")\
    .trigger(once=True)\
        .option("path","abfss://silver@vnspotify.dfs.core.windows.net/DimUser/data")\
.toTable("spotify_catalog.silver.DimUser")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimArtist

# COMMAND ----------

df_artist = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@vnspotify.dfs.core.windows.net/DimArtist/checkpoint")\
                .option("schemaEvolutionMode","addNewColumns")\
                .load("abfss://bronze@vnspotify.dfs.core.windows.net/DimArtist")


# COMMAND ----------

df_artist_obj = reusable()
df_artist = df_artist_obj.dropColumns(df_artist,['_rescued_data'])
df_artist = df_artist.drop_duplicates(['artist_id'])


# COMMAND ----------

df_artist.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@vnspotify.dfs.core.windows.net/DimArtist/checkpoint")\
    .trigger(once=True)\
        .option("path","abfss://silver@vnspotify.dfs.core.windows.net/DimArtist/data")\
    .toTable("spotify_catalog.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### DimTrack

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@vnspotify.dfs.core.windows.net/DimTrack/checkpoint")\
                .option("schemaEvolutionMode","addNewColumns")\
                .load("abfss://bronze@vnspotify.dfs.core.windows.net/DimTrack")


# COMMAND ----------

#classify track based on duration
df_track = df_track.withColumn("durationFlag",when(col("duration_sec")<150,"low")\
    .when(col("duration_sec")<300,"medium")\
        .otherwise("Medium"))

#track name modification for better SEO

df_track = df_track.withColumn("track_name",regexp_replace(col("track_name"),"- "," "))


# COMMAND ----------

df_track_obj = reusable()
df_track = df_track_obj.dropColumns(df_track,['_rescued_data'])
df_track = df_track.drop_duplicates(['track_id'])


# COMMAND ----------

df_track.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@vnspotify.dfs.core.windows.net/DimTrack/checkpoint")\
    .trigger(once=True)\
        .option("path","abfss://silver@vnspotify.dfs.core.windows.net/DimTrack/data")\
    .toTable("spotify_catalog.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DimDate

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@vnspotify.dfs.core.windows.net/DimDate/checkpoint")\
                .option("schemaEvolutionMode","addNewColumns")\
                .load("abfss://bronze@vnspotify.dfs.core.windows.net/DimDate")


# COMMAND ----------

df_date_obj = reusable()
df_date = df_date_obj.dropColumns(df_date,['_rescued_data'])

df_date.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@vnspotify.dfs.core.windows.net/DimDate/checkpoint")\
    .trigger(once=True)\
        .option("path","abfss://silver@vnspotify.dfs.core.windows.net/DimDate/data")\
    .toTable("spotify_catalog.silver.DimDate")


# COMMAND ----------

# MAGIC %md
# MAGIC ## FactStream

# COMMAND ----------

df_fact = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@vnspotify.dfs.core.windows.net/FactStream/checkpoint")\
                .option("schemaEvolutionMode","addNewColumns")\
                .load("abfss://bronze@vnspotify.dfs.core.windows.net/FactStream")


# COMMAND ----------

df_fact_obj = reusable()
df_fact = df_fact_obj.dropColumns(df_fact,['_rescued_data'])

# COMMAND ----------



df_fact.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@vnspotify.dfs.core.windows.net/FactStream/checkpoint")\
    .trigger(once=True)\
        .option("path","abfss://silver@vnspotify.dfs.core.windows.net/FactStream/data")\
    .toTable("spotify_catalog.silver.FactStream")


# COMMAND ----------

