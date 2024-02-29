# Databricks notebook source
dbutils.fs.ls("mnt/onprem/bronze/Person")

# COMMAND ----------

dbutils.fs.ls("mnt/onprem/bronze/Person/Person")

# COMMAND ----------

df= spark.read.format("parquet").load("dbfs:/mnt/onprem/bronze/Person/Person/Person.parquet")

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp,date_format
from pyspark.sql.types import TimestampType
df = df.withColumn("ModifiedDate",date_format(from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))


# COMMAND ----------

# MAGIC %md
# MAGIC # Doning transformation for all the Table

# COMMAND ----------

table_name=[]
for i in dbutils.fs.ls('/mnt/onprem/bronze/Person/'):
    table_name.append(i.name.split('/')[0])

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp,date_format
from pyspark.sql.types import TimestampType
for i in table_name:
    path ='/mnt/onprem/bronze/Person/' + i + '/' + i + '.parquet'
    df= spark.read.format("parquet").load(path)
    column = df.columns
    for col in column:
        if "Date" in col or "date" in col:
            df = df.withColumn("ModifiedDate",date_format(from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))
    output_path='/mnt/onprem/silver/' + i + '/'
    df.write.format('delta').mode("overwrite").save(output_path)



# COMMAND ----------

display(df)
