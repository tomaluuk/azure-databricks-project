# Databricks notebook source
# MAGIC %md
# MAGIC # Transform mydramalist data into relational format

# COMMAND ----------

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

# MAGIC %md
# MAGIC ## Create a reproducible identifier for movies, and fetch movie data 

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

# MAGIC %md
# MAGIC ### Write movies data to delta table

# COMMAND ----------

df_movies.write.mode("overwrite").format("delta").saveAsTable("mydramalist.movies")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch movie staff from dedicated string arrays 

# COMMAND ----------

df_directors = df.select(
    F.col("movie_id"),
    F.explode("director").alias("member")
)

df_directors = df_directors \
    .withColumn("staff_id", F.md5("member")) \
    .withColumn("staff_role", F.lit("director"))

# COMMAND ----------

df_screenwriters = df.select(
    F.col("movie_id"),
    F.explode("screenwriter").alias("member")
)
 
df_screenwriters = df_screenwriters \
    .withColumn("staff_id", F.md5("member")) \
    .withColumn("staff_role", F.lit("screenwriter"))

# COMMAND ----------

df_main_roles = df.select(
    F.col("movie_id"),
    F.explode("main_roles").alias("member")
)
 
df_main_roles = df_main_roles \
    .withColumn("staff_id", F.md5("member")) \
    .withColumn("staff_role", F.lit("main role"))

# COMMAND ----------

df_guest_roles = df.select(
    F.col("movie_id"),
    F.explode("guest_roles").alias("member")
)
 
df_guest_roles = df_guest_roles \
    .withColumn("staff_id", F.md5("member")) \
    .withColumn("staff_role", F.lit("guest role"))

# COMMAND ----------

# Concatenate staff DataFrames
df_staff = df_directors \
    .union(df_screenwriters) \
    .union(df_main_roles) \
    .union(df_guest_roles) \
    .dropna()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Write staff data into a delta table

# COMMAND ----------

df_staff.write.mode("overwrite").format("delta").saveAsTable("mydramalist.staff")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch tags data

# COMMAND ----------

df_tags = df.select(
    F.col("movie_id"),
    F.explode("tags").alias("tag")
)

# COMMAND ----------

df_tags = df_tags.withColumn(
    "tag_id",
    F.md5("tag")
).dropna()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write tags data to delta table

# COMMAND ----------

df_tags.write.mode("overwrite").format("delta").saveAsTable("mydramalist.tags")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch genres data

# COMMAND ----------

df_genres = df.select(
    F.col("movie_id"),
    F.explode("genres").alias("genre")
)

# COMMAND ----------

df_genres = df_genres.withColumn(
    "genre_id",
    F.md5("genre")
).dropna()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write genres data to a delta table

# COMMAND ----------

df_genres.write.mode("overwrite").format("delta").saveAsTable("mydramalist.genres")
