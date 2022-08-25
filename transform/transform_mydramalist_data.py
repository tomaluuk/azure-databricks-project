# Databricks notebook source
# Set up access configuration through service principal
service_credential = dbutils.secrets.get(scope="iscase-dbscope",key="ADLS_DATABRICKS_KEY")
application_id = dbutils.secrets.get(scope="iscase-dbscope",key="iscase-aad-appid")
directory_id = dbutils.secrets.get(scope="iscase-dbscope",key="iscase-aad-dirid")

spark.conf.set("fs.azure.account.auth.type.iscasedata.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.iscasedata.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.iscasedata.dfs.core.windows.net", application_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.iscasedata.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.iscasedata.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS mydramalist

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP SCHEMA IF EXISTS mydramalist CASCADE

# COMMAND ----------

# Read mydramalist data into a Spark DataFrame and drop nulls
df = spark.sql("SELECT * FROM psa.mydramalist WHERE name IS NOT NULL")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Create a movie identifier column 
df = df.withColumn(
    "movie_id", 
    F.md5(
        F.concat(
            F.coalesce(
                df.country_origin,
                F.lit("")
            ), 
            df.name, 
            F.coalesce(
                df.duration_in_minutes, 
                F.lit(0)
            ).cast("string")
        )
    )
)

# COMMAND ----------

df_movies = df.select(
    F.col("movie_id"),
    F.col("name"), 
    F.col("country_origin"), 
    F.col("duration_in_minutes"), 
    F.col("mydramalist_url"),
    F.col("nb_episodes"),
    F.col("nb_ratings"),
    F.col("nb_reviews"),
    F.col("nb_watchers"),
    F.col("ranking"),
    F.col("ratings"),
    F.col("synopsis"),
)

# COMMAND ----------

df_movies.write.mode("overwrite").format("delta").saveAsTable("mydramalist.movies")
