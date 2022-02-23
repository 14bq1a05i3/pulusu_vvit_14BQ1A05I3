# Databricks notebook source
# MAGIC %sql SELECT distinct LAST_UPDATE_TIMESTAMP FROM itemmaster ORDER BY LAST_UPDATE_TIMESTAMP DESC

# COMMAND ----------

# %sql UPDATE itemmaster SET sale_price=null, sale_quantity=null WHERE (SCRTX_DET_FREQ_SHOP_TYPE = 1 and SCRTX_DET_FREQ_SHOP_VAL=0) OR (SCRTX_DET_FREQ_SHOP_TYPE = 0 and SCRTX_DET_FREQ_SHOP_VAL=0) OR (SCRTX_DET_FREQ_SHOP_TYPE is null and SCRTX_DET_FREQ_SHOP_VAL is null)

# COMMAND ----------

# %sql RESTORE TABLE default.itemmaster to VERSION as of 124

# COMMAND ----------

# MAGIC %sql SELECT * FROM itemmaster where LAST_UPDATE_TIMESTAMP >= '2022-02-19' AND sale_price is not null and SCRTX_DET_FREQ_SHOP_TYPE=0

# COMMAND ----------

# MAGIC %sql SELECT * FROM itemmaster where LAST_UPDATE_TIMESTAMP >= '2022-02-19' AND sale_price is null and SCRTX_DET_FREQ_SHOP_TYPE=1

# COMMAND ----------

# MAGIC %sql SELECT * FROM itemmaster where LAST_UPDATE_TIMESTAMP >= '2022-02-19' AND sale_price is not null and SCRTX_DET_FREQ_SHOP_TYPE=1 and SCRTX_DET_FREQ_SHOP_VAL=0

# COMMAND ----------

# MAGIC %sql SELECT * FROM itemmaster where LAST_UPDATE_TIMESTAMP >= '2022-02-19' AND sale_price is not null and SCRTX_DET_FREQ_SHOP_VAL=0
