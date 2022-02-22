# Databricks notebook source
from pyspark.sql.types import * 
from pyspark.sql import *
import json
from pyspark.sql.functions  import *
from pytz import timezone
import datetime
import logging 
from functools import reduce
from delta.tables import *

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/Mount_Point_Creation

# COMMAND ----------

mount_point = "/mnt/centralized-price-promo"
source= "abfss://centralized-price-promo@rs06ue2dmasadata02.dfs.core.windows.net/"
clientId="2cbef55f-e5b2-403d-a3c0-430d6f5e83d4"
keyVaultName="MerchandisingApp-Key-Vault-DEV"
try:
  mounting(mount_point, source, clientId, keyVaultName)
except Exception as ex:
  # send error message to ADF and send email notification
  print(ex)

# COMMAND ----------

# raw_df = spark.read.format("csv").load("/mnt/ahold-centralized-price-promo/UnitTesting/Test")
infer_schema = "true"
first_row_is_header = "true"

# The applied options are for CSV files. For other file types, these will be ignored.
raw_df = spark.read.format("parquet").option("header", "true").option("inferSchema", "true").load("/mnt/centralized-price-promo/POSdaily/Outbound/CDM/ItemMaster_Output")
temp_table_name = "itemTemp1"
raw_df.createOrReplaceTempView(temp_table_name)
display(raw_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from itemTemp1 where SMA_DEST_STORE in ("0591", "0656", "0674", "0997", "1167", "1313", "1319", "1417", "1451", "2219", "2559", "8221", "8373", "8374")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select PRD2STORE_BANNER_ID, SMA_DEST_STORE, SMA_GTIN_NUM, SMA_ITEM_DESC, SMA_TAX_1, SMA_TAX_2, SMA_TAX_3, SMA_TAX_4, SMA_TAX_5, SMA_TAX_6, SMA_TAX_7, SMA_TAX_8, SMA_ITEM_STATUS, BTL_DPST_AMT from itemTemp1 where SMA_DEST_STORE in ("0591", "0656", "0674", "0997", "1167", "1313", "1319", "1417", "1451", "2219", "2559")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select SMA_DEST_STORE, SMA_GTIN_NUM, SMA_LEGACY_ITEM, SMA_PRIME_UPC, EFF_TS, SMA_ITEM_DESC, SMA_SUB_DEPT, SMA_MULT_UNIT_RETL, SMA_RETL_MULT_UNIT, SALE_PRICE, SALE_QUANTITY, SMA_LINK_HDR_COUPON, PERF_DETL_SUB_TYPE, SMA_LINK_START_DATE, SMA_LINK_END_DATE, SMA_ITEM_SIZE, SMA_ITEM_UOM, SMA_RETL_MULT_UOM, SMA_SBW_IND, HOW_TO_SELL, AVG_WGT, SMA_VEND_COUPON_FAM1, SMA_VEND_COUPON_FAM2, BTL_DPST_AMT, SMA_TAX_1, SMA_TAX_2, SMA_TAX_3, SMA_TAX_4, SMA_TAX_5, SMA_TAX_6, SMA_TAX_7, SMA_TAX_8, SMA_RECALL_FLAG, SMA_WIC_IND, SMA_RESTR_CODE, SMA_FSA_IND, SMA_FOOD_STAMP_IND, SMA_LINK_UPC, SMA_TARE_PCT, SMA_FIXED_TARE_WGT, SMA_CONTRIB_QTY, SMA_ITEM_STATUS, SMA_STATUS_DATE, 
# MAGIC LAST_UPDATE_TIMESTAMP from itemTemp1 where SMA_DEST_STORE in ("0591", "0656", "0674", "0997", "1167", "1313", "1319", "1417", "1451", "2219", "2559")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select SMA_DEST_STORE, SMA_GTIN_NUM, SMA_LEGACY_ITEM, SMA_PRIME_UPC, EFF_TS, SMA_ITEM_DESC, SMA_SUB_DEPT, SMA_MULT_UNIT_RETL, SMA_RETL_MULT_UNIT, SALE_PRICE, SALE_QUANTITY, SMA_LINK_HDR_COUPON, PERF_DETL_SUB_TYPE, SMA_LINK_START_DATE, SMA_LINK_END_DATE, SMA_ITEM_SIZE, SMA_ITEM_UOM, SMA_RETL_MULT_UOM, SMA_SBW_IND, HOW_TO_SELL, AVG_WGT, SMA_VEND_COUPON_FAM1, SMA_VEND_COUPON_FAM2, BTL_DPST_AMT, SMA_TAX_1, SMA_TAX_2, SMA_TAX_3, SMA_TAX_4, SMA_TAX_5, SMA_TAX_6, SMA_TAX_7, SMA_TAX_8, SMA_RECALL_FLAG, SMA_WIC_IND, SMA_RESTR_CODE, SMA_FSA_IND, SMA_FOOD_STAMP_IND, SMA_LINK_UPC, SMA_TARE_PCT, SMA_FIXED_TARE_WGT, SMA_CONTRIB_QTY, SMA_ITEM_STATUS, SMA_STATUS_DATE from itemTemp1 where SMA_DEST_STORE in ("8221", "8373", "8374")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select PRD2STORE_BANNER_ID, SMA_DEST_STORE, SMA_GTIN_NUM, SMA_LEGACY_ITEM, SMA_ITEM_DESC, SMA_TAX_1, SMA_TAX_2, SMA_TAX_3, SMA_TAX_4, SMA_TAX_5, SMA_TAX_6, SMA_TAX_7, SMA_TAX_8, SMA_ITEM_STATUS, BTL_DPST_AMT from itemTemp1 where SMA_DEST_STORE = '8221' and SMA_GTIN_NUM = '818849020010'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from itemTemp1 where SMA_DEST_STORE in ("1167")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select SMA_DEST_STORE, SMA_GTIN_NUM, SMA_LEGACY_ITEM, SMA_PRIME_UPC, TPRX001_RTL_PRC_EFF_DT, EFF_TS, SMA_ITEM_DESC, SMA_SUB_DEPT, SMA_MULT_UNIT_RETL, SMA_RETL_MULT_UNIT, SALE_PRICE, SALE_QUANTITY, SMA_LINK_HDR_COUPON, PERF_DETL_SUB_TYPE, SMA_LINK_START_DATE, SMA_LINK_END_DATE, SMA_ITEM_SIZE, SMA_ITEM_UOM, SMA_RETL_MULT_UOM, SMA_SBW_IND, HOW_TO_SELL, AVG_WGT, SMA_VEND_COUPON_FAM1, SMA_VEND_COUPON_FAM2, BTL_DPST_AMT, SMA_TAX_1, SMA_TAX_2, SMA_TAX_3, SMA_TAX_4, SMA_TAX_5, SMA_TAX_6, SMA_TAX_7, SMA_TAX_8, SMA_RECALL_FLAG, SMA_WIC_IND, SMA_RESTR_CODE, SMA_FSA_IND, SMA_FOOD_STAMP_IND, SMA_LINK_UPC, SMA_TARE_PCT, SMA_FIXED_TARE_WGT, SMA_CONTRIB_QTY, SMA_ITEM_STATUS, SMA_STATUS_DATE, 
# MAGIC LAST_UPDATE_TIMESTAMP from itemTemp1 where SMA_DEST_STORE in ("1167")