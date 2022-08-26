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

file = "abfss://dramas@iscasedata.dfs.core.windows.net/stage/mydramalist/top_5000_mydramalist.json"

# COMMAND ----------

df = spark.read \
    .option("multiline","true") \
    .option("inferSchema", "true") \
    .json(file)

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("psa.mydramalist")
