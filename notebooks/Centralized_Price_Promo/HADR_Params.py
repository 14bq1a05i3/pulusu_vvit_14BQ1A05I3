# Databricks notebook source
dbricksWorkspaceURL = dbutils.widgets.get("workspaceURL")
dbricksClusterId = dbutils.widgets.get("clusterId")
dbricksSecretName = dbutils.widgets.get("dbricksSecretName")
keyvaultURL = dbutils.widgets.get("keyvaultURL")
sqlDBSecretName = dbutils.widgets.get("secretName")

# COMMAND ----------

df1 = spark.createDataFrame([(dbricksWorkspaceURL, dbricksClusterId, dbricksSecretName, keyvaultURL, sqlDBSecretName)], ['dbricksWorkspaceURL', 'dbricksClusterId', 'dbricksSecretName', 'keyvaultURL', 'sqlDBSecretName'])
df1.createOrReplaceTempView("df1")

# COMMAND ----------

# MAGIC %sql SELECT * FROM df1

# COMMAND ----------

df1.repartition(1).write.format('csv').mode("overwrite").option("delimiter", "|").save('/mnt/centralized-price-promo/HADR_Config/')

# COMMAND ----------

# dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().toString()

# COMMAND ----------

# dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobId()

# COMMAND ----------

# dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()

# COMMAND ----------

# %sql SELECT * FROM delhaizeCppVersion

# COMMAND ----------

### Add logging
