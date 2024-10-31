# Databricks notebook source
# MAGIC %md **Read the actual file from the Storage Location with PII columns**

# COMMAND ----------

## Creating dataframe with sample data. Here SSN is sensitive data.
columns = ["Customer_id","SSN"]
data = [("001","0001-01-021"), ("002", "0001-02-011"), ("003", "0001-03-222")]

df_actual = spark.createDataFrame(data,columns)
display(df_actual)
        

# COMMAND ----------

# MAGIC %md **We can use Salting with Hashing to generate a pseudonymized key. 
# MAGIC The best approach would be storing the salt value as SECRET** 

# COMMAND ----------

salt = 'EXTRA'
spark.conf.set("da.salt", salt)

# COMMAND ----------

# MAGIC %md **Create a FUNCTION or UDF to generate the pseudonymized key**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or REPLACE FUNCTION salted_hash_col (ssn STRING) RETURNS STRING
# MAGIC RETURN sha2(concat(ssn,"${da.salt}"),256)

# COMMAND ----------

# MAGIC %md **Invoke the function with required PII column**

# COMMAND ----------

df_salted = df_actual.selectExpr("Customer_id","salted_hash_col(SSN) as SSN")
display(df_salted)

# COMMAND ----------

# MAGIC %md Note: **Save the dataframe as file at cloud container and use it for your Testing**
