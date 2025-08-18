# Databricks notebook source
customer_path = 'dbfs:/Volumes/workspace/customer_schema/customer_volume/Customer_Updated.csv'
products_path = 'dbfs:/Volumes/workspace/products_schema/products_volume/Products_Updated.csv'
transaction_path='dbfs:/Volumes/workspace/transaction_schema/transaction_volume/Transaction_Updated.csv'

# COMMAND ----------

#csv options
infer_schema = True
first_row_is_header = True
delimeter =','

# COMMAND ----------

df = spark.read.format('csv')\
        .option('header',first_row_is_header)\
        .option('inferschema',infer_schema)\
        .option('delimiter',delimeter)\
        .load(customer_path)
display(df)

# COMMAND ----------

#Create a temp view - table

temp_table_name = 'customer_delta_table'

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- test
# MAGIC SELECT * FROM customer_delta_table;

# COMMAND ----------

#permanent paqrquet file
parquet_table_name = 'customer_delta_table_persist'
df.write.mode('overwrite').format('delta').saveAsTable(parquet_table_name)