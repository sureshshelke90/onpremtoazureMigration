# Databricks notebook source
dbutils.fs.ls('/mnt/onprem/silver/Person')

# COMMAND ----------

dbutils.fs.ls('mnt/onprem/gold/')

# COMMAND ----------

input_path='/mnt/onprem/silver/Person/'

# COMMAND ----------

df = spark.read.format("delta").load(input_path)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col,regexp_replace

column_names= df.columns

for old_col_name in column_names:
    new_col_name = "".join(["_" + char if char.isupper() and not old_col_name[ i- 1].isupper() else char for i,char in enumerate(old_col_name)]).lstrip("_")
    df = df.withColumnRenamed(old_col_name,new_col_name)
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC #Doing transformation for all tables (Changing column names)

# COMMAND ----------

table_name=[]
for i in dbutils.fs.ls('/mnt/onprem/silver/'):
    table_name.append(i.name.split('/')[0])


# COMMAND ----------

table_name

# COMMAND ----------

for name in table_name:
    path = '/mnt/onprem/silver/' + name
    print(print)
    df = spark.read.format('delta').load(path)    
    column_names= df.columns
    
    for old_col_name in column_names:
        new_col_name = "".join(["_" + char if char.isupper() and not old_col_name[ i- 1].isupper() else char for i,char in enumerate(old_col_name)]).lstrip("_")
        df = df.withColumnRenamed(old_col_name,new_col_name)
    output_path='/mnt/onprem/gold/Person/' + name +'/'
    df.write.format('delta').mode("overwrite").save(output_path)



# COMMAND ----------

display(df)
