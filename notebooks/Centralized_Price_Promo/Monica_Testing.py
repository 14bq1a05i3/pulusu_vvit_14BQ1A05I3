# Databricks notebook source
from pyspark.sql.types import * 
import quinn
import json
from pyspark.sql.functions  import *
from pytz import timezone
import datetime
import logging 
from functools import reduce
from delta.tables import *
from pyspark.sql.functions import regexp_extract
import jsonpickle
from json import JSONEncoder
import re

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

mount_point = "/mnt/ahold-centralized-price-promo"
source= "abfss://ahold-centralized-price-promo@rs06ue2dmasadata02.dfs.core.windows.net/"
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
raw_df = spark.read.format("parquet").option("header", "true").option("inferSchema", "true").load("/mnt/centralized-price-promo/Item/Outbound/CDM/ItemMaster_Output")
temp_table_name = "itemTemp1"
raw_df.createOrReplaceTempView(temp_table_name)
display(raw_df)

# COMMAND ----------

#Defining the schema for Coupon Table
itemRaw_schema = StructType([
                    StructField("DB-DEST-STORE",StringType(),False),
                    StructField("DB-STORE",IntegerType(),False),
                    StructField("DB-BATCH-SERIAL-NBR",StringType(),False),
                    StructField("DB-EFF-DOW",IntegerType(),False),
                    StructField("DB-BATCH-SUB-DEPT",IntegerType(),False),
                    StructField("DB-CHG-TYPE",IntegerType(),False),
                    StructField("DB-RETL-VENDOR",IntegerType(),False),
                    StructField("DB-GTIN-NUM",LongType(),False),
                    StructField("DB-ITM-EFF-DATE",StringType(),False),
                    StructField("DB-LEGACY-ITEM",LongType(),False),
                    StructField("DB-VEND-NUM",IntegerType(),False),
                    StructField("DB-PRIME-UPC",StringType(),False),
                    StructField("DB-COMM",IntegerType(),False),
                    StructField("DB-SUB-COMM",IntegerType(),False),
                    StructField("DB-ITEM-DESC",StringType(),False),
                    StructField("DB-UPC-DESC",StringType(),True),
                    StructField("DB-GLUTEN-FREE",StringType(),True),
                    StructField("DB-ITEM-SIZE",FloatType(),False),
                    StructField("DB-ITEM-MVMT",StringType(),True),
                    StructField("DB-ITEM-POG-COMM",StringType(),True),
                    StructField("DB-ITEM-BRAND",StringType(),True),
                    StructField("DB-ITEM-TYPE",StringType(),True),
                    StructField("DB-VEND-COUPON-FAM1",IntegerType(),False),
                    StructField("DB-VEND-COUPON-FAM2",IntegerType(),False),
                    StructField("DB-FSA-IND",IntegerType(),False),
                    StructField("DB-NAT-ORG-IND",StringType(),True),
                    StructField("DB-TAX-CATEG",StringType(),True),
                    StructField("DB-PRIV-LABEL",StringType(),True),
                    StructField("DB-TAG-SZ",StringType(),True),
                    StructField("DB-UNIT-PRICE-CODE",StringType(),True),
                    StructField("DB-NON-PRICED-ITEM",StringType(),True),
                    StructField("DB-GAS-IND",StringType(),True),
                    StructField("DB-ITEM-STATUS",StringType(),True),
                    StructField("DB-RECV-TYPE",StringType(),True),
                    StructField("DB-SOURCE-METHOD",StringType(),True),
                    StructField("DB-SOURCE-WHSE",StringType(),True),
                    StructField("DB-QTY-KEY-OPTIONS",StringType(),True),
                    StructField("DB-MANUAL-PRICE-ENTRY",StringType(),True),
                    StructField("DB-FOOD-STAMP-IND",StringType(),True),
                    StructField("DB-WIC-IND",StringType(),True),
                    StructField("DB-TARE-PCT",FloatType(),False),
                    StructField("DB-FIXED-TARE-WGT",FloatType(),False),
                    StructField("DB-TARE-UOM",StringType(),True),
                    StructField("DB-POINTS-ELIGIBLE",StringType(),True),
                    StructField("DB-RECALL-FLAG",StringType(),True),
                    StructField("DB-TAX-CHG-IND",StringType(),True),
                    StructField("DB-TAX-1",StringType(),True),
                    StructField("DB-TAX-2",StringType(),True),
                    StructField("DB-TAX-3",StringType(),True),
                    StructField("DB-TAX-4",StringType(),True),
                    StructField("DB-TAX-5",StringType(),True),
                    StructField("DB-TAX-6",StringType(),True),
                    StructField("DB-TAX-7",StringType(),True),
                    StructField("DB-TAX-8",StringType(),True),
                    StructField("DB-SBW-IND",StringType(),True),
                    StructField("DB-CONTRIB-QTY",FloatType(),False),
                    StructField("DB-FILLER1",StringType(),True),
                    StructField("DB-DISCOUNTABLE-IND",StringType(),True),
                    StructField("DB-WIC-SHLF-MIN",IntegerType(),False),
                    StructField("DB-WIC-ALT",StringType(),True),
                    StructField("DB-CPN-MLTPY-IND",StringType(),True),
                    StructField("DB-HIP-IND",StringType(),True),
                    StructField("DB-MEAS-OF-EACH",DoubleType(),False),
                    StructField("DB-MEAS-OF-PRICE",IntegerType(),False),
                    StructField("DB-UOM-OF-PRICE",StringType(),True),
                    StructField("DB-CONTRIB-QTY-UOM",StringType(),True),
                    StructField("DB-SHELF-TAG-REQ",StringType(),True),
                    StructField("DB-SEGMENT",IntegerType(),False),
                    StructField("DB-VENDOR-PACK",IntegerType(),False),
                    StructField("DB-ACTIVATION-CODE",StringType(),True),
                    StructField("DB-LINK-UPC",StringType(),True),
                    StructField("DB-POS-REQUIRED",StringType(),True),
                    StructField("DB-TAG-REQUIRED",StringType(),True),
                    StructField("DB-FEE-TYPE",StringType(),True),
                    StructField("DB-BOTTLE-DEPOSIT-IND",StringType(),True),
                    StructField("DB-SUB-DEPT",StringType(),True),
                    StructField("DB-RESTR-CODE",StringType(),True),
                    StructField("DB-FUEL-PROD-CATEG",StringType(),True),
                    StructField("DB-ITEM-UOM",StringType(),True),
                    StructField("DB-NEW-ITMUPC",StringType(),True),
                    StructField("DB-SMR-EFF-DATE",StringType(),False),
                    StructField("DB-SELL-RETL",FloatType(),False),
                    StructField("DB-RETL-UOM",StringType(),True),
                    StructField("DB-RETL-MULT-UNIT",FloatType(),False),
                    StructField("DB-MULT-UNIT-RETL",FloatType(),False),
                    StructField("DB-RETL-MULT-UOM",StringType(),True),
                    StructField("DB-RETL-CHG-TYPE",StringType(),True),
                    StructField("DB-PRICE-CHG-ID",LongType(),False),
                    StructField("DB-RETL-PRC-ENTRY-CHG",StringType(),True),
                    StructField("DB-FILLER2",StringType(),True),
                    StructField("DB-WPG",IntegerType(),False),
                    StructField("DB-STORE-ZONE",StringType(),True),
                    StructField("DB-STORE-DIV",StringType(),True),
                    StructField("DB-STORE-STATE",StringType(),True),
                    StructField("DB-DATA-TYPE",StringType(),True),
                    StructField("DB-MKT-AREA",StringType(),True),
                    StructField("DB-FILLER3",StringType(),True),
                    StructField("DB-LINK-HDR-LOCATION",LongType(),False),
                    StructField("DB-POS-SYSTEM",StringType(),True),
                    StructField("DB-LINK-RCRD-TYPE",IntegerType(),False),
                    StructField("DB-LINK-HDR-MAINT-TYPE",IntegerType(),False),
                    StructField("DB-LINK-APPLY-DATE",StringType(),False),
                    StructField("DB-LINK-APPLY-TIME",IntegerType(),False),
                    StructField("DB-LINK-HDR-COUPON",LongType(),False),
                    StructField("DB-DEMAND-TEC-PROMO-ID",StringType(),False),
                    StructField("DB-LINK-START-DATE",StringType(),False),
                    StructField("DB-LINK-END-DATE",StringType(),False),
                    StructField("DB-LINK-ITEM-NBR",LongType(),False),
                    StructField("DB-PROMO-LINK-UPC",LongType(),False),
                    StructField("DB-LINK-TYPE",StringType(),True),
                    StructField("DB-LINK-FAMCD-PROMOCD",IntegerType(),False),
                    StructField("DB-LINK-CHG-TYPE",IntegerType(),False),
                    StructField("DB-LINK-OOPS-ADWK",IntegerType(),False),
                    StructField("DB-LINK-OOPS-FILE-ID",StringType(),True),
                    StructField("DB-ORIG-LIN",StringType(),True),
                    StructField("DB-ORIG-VENDOR",StringType(),True),
                    StructField("DB-ORIG-CHG-TYPE",IntegerType(),False),
                    StructField("DB-EMERGY-UPD-IND",StringType(),True),
                    StructField("DB-STATUS-DATE",StringType(),True),
                    StructField("DB-TRANSMIT-DATE",StringType(),True),
                    StructField("DB-TIBCO-DATE",StringType(),True),
                    StructField("DB-IFPS-CODE",IntegerType(),False),
                    StructField("DB-MEAS-OF-EACH-WIP",StringType(),False),
                    StructField("DB-UPC-OVERRIDE-GRP-NUM",IntegerType(),False),
                    StructField("DB-TENDER-RESTRICTION-CODE",IntegerType(),False),
                    StructField("DB-REFUND-RECEIPT-EXCLUSION",IntegerType(),False),
                    StructField("DB-IN-STORE-TAG-PRINT",StringType(),True),
                    StructField("DB-FEE-ITEM-TYPE",IntegerType(),False),
                    StructField("DB-KOSHER-FLAG",StringType(),True),
                    StructField("DB-GUIDING-STARS",IntegerType(),False),
                    StructField("DB-COMP-ID",StringType(),True),
                    StructField("DB-COMP-PRICE",StringType(),True),
                    StructField("DB-COMP-OBSERV-DATE",StringType(),True),
                    StructField("DB-FILLER4",StringType(),True)
])

# COMMAND ----------

Item_renaming = { "DB-STORE": "SMA_STORE",
                  "DB-DEST-STORE": "SMA_DEST_STORE",
                  "DB-GTIN-NUM": "SMA_GTIN_NUM",
                  "DB-SUB-DEPT" : "SMA_SUB_DEPT",                 
                  "DB-COMM":"SMA_COMM",        
                  "DB-DATA-TYPE":"SMA_DATA_TYPE",
                  "DB-DEMAND-TEC-PROMO-ID":"SMA_DEMANDTEC_PROMO_ID",
                  "DB-BOTTLE-DEPOSIT-IND":"SMA_BOTTLE_DEPOSIT_IND",
                  "DB-SUB-COMM":"SMA_SUB_COMM", 
                  "DB-SEGMENT":"SMA_SEGMENT", 
                  "DB-ITEM-DESC" : "SMA_ITEM_DESC",
                  "DB-ITEM-MVMT":"SMA_ITEM_MVMT",
                  "DB-ITEM-POG-COMM":"SMA_ITEM_POG_COMM",
                  "DB-UPC-DESC":"SMA_UPC_DESC",
                  "DB-RETL-MULT-UOM":"SMA_RETL_MULT_UOM",                 
                  "DB-SBW-IND": "SMA_SBW_IND",
                  "DB-PRIME-UPC": "SMA_PRIME_UPC",                 
                  "DB-ITEM-SIZE" : "SMA_ITEM_SIZE",                 
                  "DB-ITEM-UOM":"SMA_ITEM_UOM",                 
                  "DB-ITEM-BRAND":"SMA_ITEM_BRAND",
                  "DB-ITEM-TYPE":"SMA_ITEM_TYPE",                 
                  "DB-VEND-COUPON-FAM1": "SMA_VEND_COUPON_FAM1",
                  "DB-VEND-COUPON-FAM2": "SMA_VEND_COUPON_FAM2",
                  "DB-FSA-IND" : "SMA_FSA_IND",                 
                  "DB-NAT-ORG-IND":"SMA_NAT_ORG_IND",
                  "DB-IN-STORE-TAG-PRINT":"SMA_IN_STORE_TAG_PRINT",
                  "DB-NEW-ITMUPC":"SMA_NEW_ITMUPC",
                  "DB-PRIV-LABEL":"SMA_PRIV_LABEL",                 
                  "DB-TAG-SZ":"SMA_TAG_SZ",
                  "DB-FUEL-PROD-CATEG":"SMA_FUEL_PROD_CATEG",                 
                  "DB-CONTRIB-QTY":"SMA_CONTRIB_QTY",   
                  "DB-CONTRIB-QTY-UOM":"SMA_CONTRIB_QTY_UOM",
                  "DB-GAS-IND" : "SMA_GAS_IND",                 
                  "DB-ITEM-STATUS":"SMA_ITEM_STATUS",                 
                  "DB-VEND-NUM":"SMA_VEND_NUM",                 
                  "DB-SOURCE-METHOD":"SMA_SOURCE_METHOD",
                  "DB-SOURCE-WHSE":"SMA_SOURCE_WHSE",
                  "DB-RECV-TYPE":"SMA_RECV_TYPE",                 
                  "DB-MEAS-OF-EACH":"SMA_MEAS_OF_EACH",
                  "DB-MEAS-OF-EACH-WIP":"SMA_MEAS_OF_EACH_WIP",
                  "DB-MEAS-OF-PRICE":"SMA_MEAS_OF_PRICE",    
                  "DB-MKT-AREA":"SMA_MKT_AREA",
                  "DB-UOM-OF-PRICE": "SMA_UOM_OF_PRICE",                 
                  "DB-VENDOR-PACK":"SMA_VENDOR_PACK",
                  "DB-SHELF-TAG-REQ":"SMA_SHELF_TAG_REQ",                 
                  "DB-QTY-KEY-OPTIONS":"SMA_QTY_KEY_OPTIONS",                 
                  "DB-MANUAL-PRICE-ENTRY":"SMA_MANUAL_PRICE_ENTRY",
                  "DB-FOOD-STAMP-IND":"SMA_FOOD_STAMP_IND",                 
                  "DB-HIP-IND":"SMA_HIP_IND",                 
                  "DB-WIC-IND":"SMA_WIC_IND",
                  "DB-TARE-PCT": "SMA_TARE_PCT",
                  "DB-FIXED-TARE-WGT":"SMA_FIXED_TARE_WGT",
                  "DB-TARE-UOM":"SMA_TARE_UOM",                  
                  "DB-POINTS-ELIGIBLE":"SMA_POINTS_ELIGIBLE",                 
                  "DB-RECALL-FLAG":"SMA_RECALL_FLAG",
                  "DB-TAX-CHG-IND":"SMA_TAX_CHG_IND",
                  "DB-RESTR-CODE":"SMA_RESTR_CODE",                 
                  "DB-WIC-SHLF-MIN":"SMA_WIC_SHLF_MIN",                 
                  "DB-WIC-ALT":"SMA_WIC_ALT",                 
                  "DB-TAX-1":"SMA_TAX_1",
                  "DB-TAX-2":"SMA_TAX_2",
                  "DB-TAX-3":"SMA_TAX_3",
                  "DB-TAX-4":"SMA_TAX_4",
                  "DB-TAX-5":"SMA_TAX_5",
                  "DB-TAX-6":"SMA_TAX_6",
                  "DB-TAX-7":"SMA_TAX_7",
                  "DB-TAX-8":"SMA_TAX_8",            
                  "DB-CPN-MLTPY-IND":"SMA_CPN_MLTPY_IND",
                  "DB-DISCOUNTABLE-IND":"SMA_DISCOUNTABLE_IND",                 
                  "DB-MULT-UNIT-RETL":"SMA_MULT_UNIT_RETL",
                  "DB-RETL-MULT-UNIT" : "SMA_RETL_MULT_UNIT",
                  "DB-LINK-UPC":"SMA_LINK_UPC",
                  "DB-ACTIVATION-CODE":"SMA_ACTIVATION_CODE",                                  
                  "DB-GLUTEN-FREE":"SMA_GLUTEN_FREE", 
                  "DB-NON-PRICED-ITEM":"SMA_NON_PRICED_ITEM",
                  "DB-ORIG-CHG-TYPE":"SMA_ORIG_CHG_TYPE",
                  "DB-ORIG-LIN":"SMA_ORIG_LIN",
                  "DB-ORIG-VENDOR":"SMA_ORIG_VENDOR",
                  "DB-IFPS-CODE":"SMA_IFPS_CODE",
                  "DB-BATCH-SERIAL-NBR":"SMA_BATCH_SERIAL_NBR",
                  "DB-BATCH-SUB-DEPT":"SMA_BATCH_SUB_DEPT",
                  "DB-EFF-DOW":"SMA_EFF_DOW",
                  "DB-EMERGY-UPD-IND":"SMA_EMERGY_UPDATE_IND",
                  "DB-CHG-TYPE":"SMA_CHG_TYPE",
                  "DB-RETL-VENDOR":"SMA_RETL_VENDOR",
                  "DB-POS-REQUIRED":"SMA_POS_REQUIRED",
                  "DB-TAG-REQUIRED":"SMA_TAG_REQUIRED",
                  "DB-UPC-OVERRIDE-GRP-NUM":"SMA_UPC_OVERRIDE_GRP_NUM",                 
                  "DB-TENDER-RESTRICTION-CODE":"SMA_TENDER_RESTRICTION_CODE",
                  "DB-REFUND-RECEIPT-EXCLUSION":"SMA_REFUND_RECEIPT_EXCLUSION",                 
                  "DB-FEE-ITEM-TYPE":"SMA_FEE_ITEM_TYPE",
                  "DB-FEE-TYPE":"SMA_FEE_TYPE",
                  "DB-KOSHER-FLAG":"SMA_KOSHER_FLAG",       
                  "DB-LEGACY-ITEM":"SMA_LEGACY_ITEM",
                  "DB-GUIDING-STARS":"SMA_GUIDING_STARS",                 
                  "DB-COMP-ID":"SMA_COMP_ID",
                  "DB-COMP-OBSERV-DATE":"SMA_COMP_OBSERV_DATE",
                  "DB-COMP-PRICE":"SMA_COMP_PRICE",
                  "DB-LINK-HDR-COUPON":"SMA_LINK_HDR_COUPON",
                  "DB-LINK-HDR-LOCATION":"SMA_LINK_HDR_LOCATION",
                  "DB-LINK-START-DATE":"SMA_LINK_START_DATE",
                  "DB-LINK-RCRD-TYPE":"SMA_LINK_RCRD_TYPE",
                  "DB-LINK-END-DATE":"SMA_LINK_END_DATE",
                  "DB-LINK-HDR-MAINT-TYPE":"SMA_LINK_HDR_MAINT_TYPE",
                  "DB-LINK-ITEM-NBR":"SMA_LINK_ITEM_NBR",
                  "DB-LINK-OOPS-ADWK":"SMA_LINK_OOPS_ADWK",
                  "DB-LINK-OOPS-FILE-ID":"SMA_LINK_OOPS_FILE_ID",
                  "DB-LINK-CHG-TYPE":"SMA_LINK_CHG_TYPE",
                  "DB-LINK-TYPE":"SMA_LINK_TYPE",
                  "DB-LINK-FAMCD-PROMOCD":"SMA_LINK_FAMCD_PROMOCD",
                  "DB-ITM-EFF-DATE":"SMA_ITM_EFF_DATE",
                  "DB-PRICE-CHG-ID": "SMA_PRICE_CHG_ID",
                  "DB-POS-SYSTEM":"SMA_POS_SYSTEM",
                  "DB-SELL-RETL": "SMA_SELL_RETL",
                  "DB-RETL-UOM": "SMA_RETL_UOM",
                  "DB-RETL-PRC-ENTRY-CHG": "SMA_RETL_PRC_ENTRY_CHG",
                  "DB-RETL-CHG-TYPE":"SMA_RETL_CHG_TYPE",
                  "DB-LINK-APPLY-DATE":"SMA_LINK_APPLY_DATE",
                  "DB-LINK-APPLY-TIME":"SMA_LINK_APPLY_TIME",
                  "DB-STATUS-DATE":"SMA_STATUS_DATE",
                  "DB-PROMO-LINK-UPC":"SMA_PROMO_LINK_UPC",
                  "DB-SMR-EFF-DATE":"SMA_SMR_EFF_DATE",
                  "DB-STORE-DIV":"SMA_STORE_DIV",
                  "DB-STORE-STATE":"SMA_STORE_STATE",
                  "DB-STORE-ZONE":"SMA_STORE_ZONE",
                  "DB-TAX-CATEG":"SMA_TAX_CATEG",
                  "DB-TIBCO-DATE":"SMA_TIBCO_DATE",
                  "DB-TRANSMIT-DATE":"SMA_TRANSMIT_DATE",
                  "DB-UNIT-PRICE-CODE":"SMA_UNIT_PRICE_CODE",
                  "DB-WPG":"SMA_WPG"
 }

# COMMAND ----------

def itemflat_promotable(s):
    return Item_renaming[s]
def change_col_name(s):
    return s in Item_renaming

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Full Load

# COMMAND ----------

# raw_df = spark.read.format("csv").load("/mnt/ahold-centralized-price-promo/UnitTesting/Test")
infer_schema = "true"
first_row_is_header = "true"

# The applied options are for CSV files. For other file types, these will be ignored.
fullLoad = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("sep", '|').schema(itemRaw_schema).load("/mnt/ahold-centralized-price-promo/Fullitem/Inbound/RDS/2021/06/22/UnzippedSink/TEST.NQ.GM.SMA.DB.ITEM.LOAD.ZIP/TEST.NQ.GM.SMA.DB.ITEM.LOAD.txt")
fullLoad = quinn.with_some_columns_renamed(itemflat_promotable, change_col_name)(fullLoad)
temp_table_name = "fullLoad"
fullLoad.createOrReplaceTempView(temp_table_name)
display(fullLoad)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Daily Load

# COMMAND ----------

# raw_df = spark.read.format("csv").load("/mnt/ahold-centralized-price-promo/UnitTesting/Test")
infer_schema = "true"
first_row_is_header = "true"

# The applied options are for CSV files. For other file types, these will be ignored.
dailyLoad = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("sep", '|').schema(itemRaw_schema).load("/mnt/ahold-centralized-price-promo/Item/Inbound/RDS/2021/06/22/UnzippedSink/TEST.NQ.CI.SMA.DB.ITEM.MAINT.ZIP/TEST.NQ.CI.SMA.DB.ITEM.MAINT.txt")
dailyLoad = quinn.with_some_columns_renamed(itemflat_promotable, change_col_name)(dailyLoad)
temp_table_name = "dailyLoad"
dailyLoad.createOrReplaceTempView(temp_table_name)
display(dailyLoad)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select SMA_STORE,SMA_GTIN_NUM,SMA_LEGACY_ITEM,SMA_CHG_TYPE,SMA_LINK_APPLY_DATE,SMA_LINK_START_DATE, SMA_LINK_HDR_COUPON,SMA_LINK_END_DATE from fullLoad where sma_store= 0758 and sma_gtin_num= 1504

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC --Select item_number, store_number ,SID_COPYCARD, SOURCE_PROMOTIONID from weeklyad_delta where item_number like '%154861' 
# MAGIC --and store_number like '762'
# MAGIC 
# MAGIC --Select item_number, store_number ,SID_COPYCARD, SOURCE_PROMOTIONID from weeklyad_delta where item_number like '%257155' and store_number like '100'
# MAGIC 
# MAGIC --Select item_number, store_number ,SID_COPYCARD, SOURCE_PROMOTIONID from weeklyad_delta where item_number like '%4061' and store_number like '762'
# MAGIC 
# MAGIC select 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from (select sma_store, sma_gtin_num, count(*) as count from fullLoad group by sma_store, sma_gtin_num) where sma_store=758 and sma_gtin_num=1504

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from fullLoad where sma_store=758 and sma_gtin_num in (1504,1562,3056,3116,3117,3121,3178,3179,3207,3239,3293,3294,3445,3450,3492,3496,3605)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from dailyLoad where sma_store=758 and sma_gtin_num in (1504,1562,3056,3116,3117,3121,3178,3179,3207,3239,3293,3294,3445,3450,3492,3496,3605)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from itemMasterAhold

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from CouponAholdTable order by location 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from itemMasterAhold where SMA_DEST_STORE= '0758'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct sma_store from itemMasterAhold 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select SMA_STORE, SMA_GTIN_NUM, SMA_LEGACY_ITEM, SMA_PRIME_UPC, EFF_TS, SMA_ITEM_DESC, SMA_SUB_DEPT, SMA_MULT_UNIT_RETL, SMA_RETL_MULT_UNIT, SALE_PRICE, SALE_QUANTITY, SMA_LINK_HDR_COUPON, PERF_DETL_SUB_TYPE, SMA_LINK_START_DATE, SMA_LINK_END_DATE, SMA_ITEM_SIZE, SMA_ITEM_UOM, SMA_RETL_MULT_UOM, SMA_SBW_IND, HOW_TO_SELL, AVG_WGT, SMA_VEND_COUPON_FAM1, SMA_VEND_COUPON_FAM2, BTL_DPST_AMT, SMA_TAX_1, SMA_TAX_2, SMA_TAX_3, SMA_TAX_4, SMA_TAX_5, SMA_TAX_6, SMA_TAX_7, SMA_TAX_8, SMA_RECALL_FLAG, SMA_WIC_IND, SMA_RESTR_CODE, SMA_FSA_IND, SMA_FOOD_STAMP_IND, SMA_LINK_UPC, SMA_TARE_PCT, SMA_FIXED_TARE_WGT, SMA_CONTRIB_QTY, SMA_ITEM_STATUS, SMA_STATUS_DATE, LAST_UPDATE_TIMESTAMP from itemMasterAhold where SMA_DEST_STORE in ('0758', '6447') order by SMA_GTIN_NUM asc

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from CouponAholdTable

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select * from itemMasterAhold where SMA_DEST_STORE= '0758' and SMA_GTIN_NUM in ('7566919675')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select distinct LAST_UPDATE_TIMESTAMP from itemMasterAhold

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select * from ItemMainAhold where SMA_DEST_STORE= '0758' and SMA_GTIN_NUM in ('00001065304077 ')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select * from PromotionMain where SMA_STORE= '0758' and SMA_PROMO_LINK_UPC in ('3056')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select * from PromotionLink where SMA_STORE= '0758' and SMA_PROMO_LINK_UPC in ('3056')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select * from itemMasterAhold where SMA_DEST_STORE= '0758' 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select SMA_STORE, SMA_GTIN_NUM, SMA_LEGACY_ITEM, SMA_PRIME_UPC, EFF_TS, SMA_ITEM_DESC, SMA_SUB_DEPT, SMA_MULT_UNIT_RETL, SMA_RETL_MULT_UNIT, SALE_PRICE, SALE_QUANTITY, SMA_LINK_HDR_COUPON, PERF_DETL_SUB_TYPE, SMA_LINK_START_DATE, SMA_LINK_END_DATE, SMA_ITEM_SIZE, SMA_ITEM_UOM, SMA_RETL_MULT_UOM, SMA_SBW_IND, HOW_TO_SELL, AVG_WGT, SMA_VEND_COUPON_FAM1, SMA_VEND_COUPON_FAM2, BTL_DPST_AMT, SMA_TAX_1, SMA_TAX_2, SMA_TAX_3, SMA_TAX_4, SMA_TAX_5, SMA_TAX_6, SMA_TAX_7, SMA_TAX_8, SMA_RECALL_FLAG, SMA_WIC_IND, SMA_RESTR_CODE, SMA_FSA_IND, SMA_FOOD_STAMP_IND, SMA_LINK_UPC, SMA_TARE_PCT, SMA_FIXED_TARE_WGT, SMA_CONTRIB_QTY, SMA_ITEM_STATUS, SMA_STATUS_DATE, LAST_UPDATE_TIMESTAMP from itemMasterAhold where SMA_DEST_STORE= '0020' and SMA_GTIN_NUM= '360001333' order by SMA_GTIN_NUM asc

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC show tables

# COMMAND ----------

# spark.sql("""DELETE FROM ItemMainAhold;""")
# spark.sql("""DROP TABLE IF EXISTS ItemMainAhold;""")
# dbutils.fs.rm('/mnt/ahold-centralized-price-promo/Item/Outbound/DeltaTables/ItemMain',recurse=True)
# spark.sql("""DELETE FROM PriceChange;""")
# spark.sql("""DROP TABLE IF EXISTS PriceChange;""")
# dbutils.fs.rm('/mnt/ahold-centralized-price-promo/Item/Outbound/DeltaTables/PriceChange',recurse=True)
# spark.sql("""DELETE FROM PromotionLink;""")
# spark.sql("""DROP TABLE IF EXISTS PromotionLink;""")
# dbutils.fs.rm('/mnt/ahold-centralized-price-promo/Item/Outbound/DeltaTables/Test/PromotionLink',recurse=True)
# spark.sql("""DELETE FROM PromotionMain;""")
# spark.sql("""DROP TABLE IF EXISTS PromotionMain;""")
# dbutils.fs.rm('/mnt/ahold-centralized-price-promo/Item/Outbound/DeltaTables/Test/PromoMain',recurse=True)
# spark.sql("""DELETE FROM itemMasterAhold;""")
# spark.sql("""DROP TABLE IF EXISTS itemMasterAhold;""")
# dbutils.fs.rm('/mnt/centralized-price-promo/Ahold/Outbound/SDM/Item_Master_delta',recurse=True)
# spark.sql("""DELETE FROM Vendor_delta;""")
# spark.sql("""DROP TABLE IF EXISTS Vendor_delta;""")
# dbutils.fs.rm('/mnt/ahold-centralized-price-promo/Vendor/Outbound/SDM/Vendor_delta',recurse=True)
# spark.sql("""DELETE FROM Store_delta;""")
# spark.sql("""DROP TABLE IF EXISTS Store_delta;""")
# dbutils.fs.rm('/mnt/ahold-centralized-price-promo/Store/Outbound/SDM/Store_delta',recurse=True)
# spark.sql("""DELETE FROM CouponAholdTable;""")
# spark.sql("""DROP TABLE IF EXISTS CouponAholdTable;""")
# dbutils.fs.rm('/mnt/ahold-centralized-price-promo/Coupon/Outbound/SDM/Coupon_Delta',recurse=True)

# spark.sql("""DELETE FROM tax_holiday;""")
# spark.sql("""DROP TABLE IF EXISTS tax_holiday;""")
# dbutils.fs.rm('/mnt/ahold-centralized-price-promo/TAX/Outbound/SDM/taxHoliday_delta',recurse=True)

# COMMAND ----------

spark.sql("""DELETE FROM itemMasterAhold;""")
spark.sql("""DROP TABLE IF EXISTS itemMasterAhold;""")
dbutils.fs.rm('/mnt/centralized-price-promo/Ahold/Outbound/SDM/Item_Master_delta',recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC desc detail delta.`/mnt/centralized-price-promo/Delhaize/Outbound/SDM/Item_Master_delta`

# COMMAND ----------

/mnt/ahold-centralized-price-promo/Item/Outbound/DeltaTables/PromoMain

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select SMA_STORE, SMA_GTIN_NUM, SMA_LEGACY_ITEM,SMA_ITEM_DESC, SMA_TAX_1, SMA_TAX_2, SMA_TAX_3, SMA_TAX_4, SMA_TAX_5, SMA_TAX_6, SMA_TAX_7, SMA_TAX_8 LAST_UPDATE_TIMESTAMP from itemMasterAhold where SMA_DEST_STORE in ('6447') and SMA_LEGACY_ITEM = '000900000122'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select SMA_STORE, SMA_GTIN_NUM, SMA_LEGACY_ITEM, SMA_PRIME_UPC, SMA_ITEM_DESC, SMA_SUB_DEPT, SMA_MULT_UNIT_RETL, SMA_RETL_MULT_UNIT, SALE_PRICE, SALE_QUANTITY, SMA_LINK_HDR_COUPON, PERF_DETL_SUB_TYPE, SMA_LINK_START_DATE, SMA_LINK_END_DATE, SMA_RETL_MULT_UOM, SMA_VEND_COUPON_FAM1, SMA_VEND_COUPON_FAM2,SMA_WIC_IND,SMA_TAX_1, SMA_TAX_2, SMA_TAX_3, SMA_TAX_4, SMA_TAX_5, SMA_TAX_6, SMA_TAX_7, SMA_TAX_8, LAST_UPDATE_TIMESTAMP from itemMasterAhold  where SMA_DEST_STORE = '0020' and SMA_GTIN_NUM in ('920082418', '69685912800', '63711811153', '7507200126', '4110000701')