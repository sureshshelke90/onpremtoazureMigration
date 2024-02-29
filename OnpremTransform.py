# Databricks notebook source
# MAGIC %md
# MAGIC #Brone Layer Mount

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type" : "CustomAccessToken",
    "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}
dbutils.fs.mount(
    source = "abfss://bronze@onpremadf.dfs.core.windows.net/",
    mount_point = "/mnt/onprem/bronze",
    extra_configs = configs 
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Silver Layer Mount

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type" : "CustomAccessToken",
    "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}
dbutils.fs.mount(
    source = "abfss://silver@onpremadf.dfs.core.windows.net/",
    mount_point = "/mnt/onprem/silver",
    extra_configs = configs 
)

# COMMAND ----------

dbutils.fs.unmount("/mnt/onprem/gold")

# COMMAND ----------

# MAGIC %md
# MAGIC #Gold Layer Mount

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type" : "CustomAccessToken",
    "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}
dbutils.fs.mount(
    source = "abfss://gold@onpremadf.dfs.core.windows.net/",
    mount_point = "/mnt/onprem/gold",
    extra_configs = configs 
)

# COMMAND ----------

dbutils.fs.ls("/mnt/onprem/gold/")

# COMMAND ----------


