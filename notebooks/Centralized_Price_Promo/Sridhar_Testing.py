# Databricks notebook source
from pyspark.sql.types import * 
import json
from pyspark.sql.functions  import *
from pytz import timezone
import datetime
import logging 
from functools import reduce
from delta.tables import *
from pyspark.sql.functions import regexp_extract
from json import JSONEncoder
import re

# COMMAND ----------

df_item_master  = df_item_master.filter((col('SMA_GTIN_NUM') == '00007680850112' and col('sma_store') == '0491'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SET TIME ZONE 'America/Halifax';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_timestamp();

# COMMAND ----------

# MAGIC %sql
# MAGIC SET TIME ZONE 'Zulu';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_timestamp();

# COMMAND ----------

Date = datetime.datetime.now(timezone("Zulu")).strftime("%Y-%m-%d")
Date

# COMMAND ----------

# raw_df = spark.read.format("csv").load("/mnt/ahold-centralized-price-promo/UnitTesting/Test")
infer_schema = "true"
first_row_is_header = "true"

# The applied options are for CSV files. For other file types, these will be ignored.
raw_df = spark.read.format("parquet").option("header", "true").option("inferSchema", "true").load("/mnt/centralized-price-promo/Coupon/Outbound/CDM")
temp_table_name = "itemTemp1"
raw_df.createOrReplaceTempView(temp_table_name)
display(raw_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct(last_update_timestamp) from itemTemp1 where date(last_update_timestamp) >= current_date()-5 order by last_update_timestamp desc

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from item_Main where date(LAST_UPDATE_TIMESTAMP) = current_date() and rtx_store=0001 and last_update_id='411b8a56-632b-4f03-8557-4c268e43ee9b'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select rtx_upc, rtx_store, SCRTX_DET_UNT_QTY, SCRTX_DET_RTL_PRC, SCRTX_HDR_DESC, last_update_id, last_update_timestamp from item_Main where date(LAST_UPDATE_TIMESTAMP) = current_date() and rtx_store=0001 and rtx_upc in ('31110', '40520', '40520', '43940', '9300000220', '10900000150', '31180', '36030', '36180', '40240', '42240', '42250', '42400', '44090')

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC 
# MAGIC select SMA_GTIN_NUM, sma_store, SMA_RETL_MULT_UNIT, SMA_MULT_UNIT_RETL, SCRTX_HDR_DESC, SCRTX_DET_FREQ_SHOP_VAL, SCRTX_DET_FREQ_SHOP_TYPE from ItemMainEffTemp where sma_store='0001' and sma_gtin_num in ('31110', '40520', '40520', '43940', '9300000220', '10900000150', '31180', '36030', '36180', '40240', '42240', '42250', '42400', '44090')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select SMA_GTIN_NUM, sma_store, SMA_RETL_MULT_UNIT, SMA_MULT_UNIT_RETL, SALE_PRICE, SALE_QUANTITY, PRICE_MLT_Quantity, SCRTX_DET_FREQ_SHOP_TYPE, PRICE_UNIT_PRICE, SCRTX_DET_FREQ_SHOP_VAL from itemMaster where sma_store='0001' and sma_gtin_num in ('31110', '40520', '40520', '43940', '9300000220', '10900000150', '31180', '36030', '36180', '40240', '42240', '42250', '42400', '44090')

# COMMAND ----------

# spark.sql("""DELETE FROM ItemMainAhold;""")
# spark.sql("""DROP TABLE IF EXISTS ItemMainAhold;""")
# spark.sql("""DELETE FROM PriceChange;""")
# spark.sql("""DROP TABLE IF EXISTS PriceChange;""")
# spark.sql("""DELETE FROM PromotionLink;""")
# spark.sql("""DROP TABLE IF EXISTS PromotionLink;""")
# spark.sql("""DELETE FROM PromotionMain;""")
# spark.sql("""DROP TABLE IF EXISTS PromotionMain;""")
# spark.sql("""DELETE FROM CouponAholdTable;""")
# spark.sql("""DROP TABLE IF EXISTS CouponAholdTable;""")
# spark.sql("""DELETE FROM itemMasterAhold;""")
# spark.sql("""DROP TABLE IF EXISTS itemMasterAhold;""")

# spark.sql("""DELETE FROM Vendor_delta;""")
# spark.sql("""DROP TABLE IF EXISTS Vendor_delta;""")

# spark.sql("""DELETE FROM Store_delta;""")
# spark.sql("""DROP TABLE IF EXISTS Store_delta;""")

# spark.sql("""DELETE FROM promoLinkTemp;""")
# spark.sql("""DROP TABLE IF EXISTS promoLinkTemp;""")

# dbutils.fs.rm('/mnt/ahold-centralized-price-promo/Item/Outbound/SDM/ArchivalRecords/promoLinkTemp',recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC desc history PriceChange

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE PriceChange TO VERSION AS OF 269

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select sma_store, last_update_timestamp, count(*) as count from itemMaster where sma_store=1167 group by sma_store, last_update_timestamp

# COMMAND ----------

from datetime import datetime, timedelta
date_time_str = '2021/06/30'
# date_time_str = '2021/11/16'

date_time_obj = datetime.strptime(date_time_str, '%Y/%m/%d')

d = date_time_obj - timedelta(days=1)

print ("The type of the date is now",  type(date_time_obj))
print ("The date is", date_time_obj.date().isoformat())
print(d.date().isoformat())

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from itemMaster where SCRTX_DET_FREQ_SHOP_TYPE  = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from itemMaster where DATE(last_update_timestamp) >= current_date()-1 and sma_store=1167

# COMMAND ----------



# spark.sql("""DELETE FROM itemMaster;""")
# spark.sql("""DROP TABLE IF EXISTS itemMaster;""")

# spark.sql("""DELETE FROM productDeltaTable;""")
# spark.sql("""DROP TABLE IF EXISTS productDeltaTable;""")
# spark.sql("""DELETE FROM productDeltaTable;""")
# spark.sql("""DROP TABLE IF EXISTS productDeltaTable;""")
# spark.sql("""DELETE FROM ItemMainEffTemp;""")
# spark.sql("""DROP TABLE IF EXISTS ItemMainEffTemp;""")
# spark.sql("""DELETE FROM productTemp;""")
# spark.sql("""DROP TABLE IF EXISTS productTemp;""")
# spark.sql("""DELETE FROM Item_Main;""")
# spark.sql("""DROP TABLE IF EXISTS Item_Main;""")
# spark.sql("""DELETE FROM CouponDelhaizeTable;""")
# spark.sql("""DROP TABLE IF EXISTS CouponDelhaizeTable;""")
# spark.sql("""DELETE FROM bottleDeposit;""")
# spark.sql("""DROP TABLE IF EXISTS bottleDeposit;""")
# spark.sql("""DELETE FROM promoLink;""")
# spark.sql("""DROP TABLE IF EXISTS promoLink;""")
# spark.sql("""DELETE FROM AlternateUPC_delta;""")
# spark.sql("""DROP TABLE IF EXISTS AlternateUPC_delta;""")
# spark.sql("""DELETE FROM Inventory_delta;""")
# spark.sql("""DROP TABLE IF EXISTS Inventory_delta;""")
# spark.sql("""DELETE FROM DApriceTemp;""")
# spark.sql("""DROP TABLE IF EXISTS DApriceTemp;""")
# spark.sql("""DELETE FROM DAPriceDelta;""")
# spark.sql("""DROP TABLE IF EXISTS DAPriceDelta;""")
# spark.sql("""DELETE FROM ProductToStoreDelta;""")
# spark.sql("""DROP TABLE IF EXISTS ProductToStoreDelta;""")
# spark.sql("""DELETE FROM prodToStoreTemp;""")
# spark.sql("""DROP TABLE IF EXISTS prodToStoreTemp;""")
# spark.sql("""DELETE FROM Store_delta_Delhaize;""")
# spark.sql("""DROP TABLE IF EXISTS Store_delta_Delhaize;""")
# spark.sql("""DELETE FROM StoreTemp;""")
# spark.sql("""DROP TABLE IF EXISTS StoreTemp;""")

# dbutils.fs.rm('/mnt/centralized-price-promo/Item/Outbound/CDM/ItemMaster_Output',recurse=True)

# COMMAND ----------

spark.sql(""" CREATE TABLE IF NOT EXISTS itemMaster (
                AVG_WGT STRING,
                BTL_DPST_AMT STRING,
                HOW_TO_SELL STRING,
                PERF_DETL_SUB_TYPE STRING,
                SALE_PRICE STRING,
                SALE_QUANTITY STRING,
                SMA_ACTIVATION_CODE STRING,
                SMA_BATCH_SERIAL_NBR INTEGER,
                SMA_BATCH_SUB_DEPT LONG,
                SMA_BOTTLE_DEPOSIT_IND STRING,
                SMA_CHG_TYPE STRING,
                SMA_COMM INTEGER,
                SMA_COMP_ID STRING,
                SMA_COMP_OBSERV_DATE STRING,
                SMA_COMP_PRICE STRING,
                SMA_CONTRIB_QTY STRING,
                SMA_CONTRIB_QTY_UOM STRING,
                SMA_CPN_MLTPY_IND STRING,
                SMA_DATA_TYPE STRING,
                SMA_DEMANDTEC_PROMO_ID STRING,
                SMA_DEST_STORE STRING,
                SMA_DISCOUNTABLE_IND STRING,
                SMA_EFF_DOW STRING,
                SMA_EMERGY_UPDATE_IND STRING,
                SMA_FEE_ITEM_TYPE STRING,
                SMA_FEE_TYPE STRING,
                SMA_FIXED_TARE_WGT STRING,
                SMA_FOOD_STAMP_IND STRING,
                SMA_FSA_IND STRING,
                SMA_FUEL_PROD_CATEG STRING,
                SMA_GAS_IND STRING,
                SMA_GLUTEN_FREE STRING,
                SMA_GTIN_NUM STRING,
                SMA_GUIDING_STARS INTEGER,
                SMA_HIP_IND STRING,
                SMA_HLTHY_IDEAS STRING,
                SMA_IFPS_CODE STRING,
                SMA_IN_STORE_TAG_PRINT STRING,
                SMA_ITEM_BRAND STRING,
                SMA_ITEM_DESC STRING,
                SMA_ITEM_MVMT STRING,
                SMA_ITEM_POG_COMM STRING,
                SMA_ITEM_SIZE STRING,
                SMA_ITEM_STATUS STRING,
                SMA_ITEM_TYPE STRING,
                SMA_ITEM_UOM STRING,
                SMA_KOSHER_FLAG STRING,
                SMA_LEGACY_ITEM STRING,
                SMA_LINK_APPLY_DATE STRING,
                SMA_LINK_APPLY_TIME STRING,
                SMA_LINK_CHG_TYPE STRING,
                SMA_LINK_END_DATE STRING,
                SMA_LINK_FAMCD_PROMOCD STRING,
                SMA_LINK_HDR_COUPON STRING,
                SMA_LINK_HDR_LOCATION STRING,
                SMA_LINK_HDR_MAINT_TYPE STRING,
                SMA_LINK_ITEM_NBR STRING,
                SMA_LINK_OOPS_ADWK STRING,
                SMA_LINK_OOPS_FILE_ID STRING,
                SMA_LINK_RCRD_TYPE STRING,
                SMA_LINK_START_DATE STRING,
                SMA_LINK_SYS_DIGIT STRING,
                SMA_LINK_TYPE STRING,
                SMA_LINK_UPC STRING,
                SMA_PL_UPC_REDEF STRING,
                SMA_MANUAL_PRICE_ENTRY STRING,
                SMA_MEAS_OF_EACH STRING,
                SMA_MEAS_OF_EACH_WIP STRING,
                SMA_MEAS_OF_PRICE STRING,
                SMA_MKT_AREA STRING,
                SMA_MULT_UNIT_RETL FLOAT,
                SMA_NAT_ORG_IND STRING,
                SMA_NEW_ITMUPC STRING,
                SMA_NON_PRICED_ITEM STRING,
                SMA_ORIG_CHG_TYPE STRING,
                SMA_ORIG_LIN STRING,
                SMA_ORIG_VENDOR STRING,
                SMA_POINTS_ELIGIBLE STRING,
                SMA_POS_REQUIRED STRING,
                SMA_POS_SYSTEM STRING,
                SMA_PRICE_CHG_ID STRING,
                SMA_PRIME_UPC STRING,
                SMA_PRIV_LABEL STRING,
                SMA_QTY_KEY_OPTIONS STRING,
                SMA_RECALL_FLAG STRING,
                SMA_RECV_TYPE STRING,
                SMA_REFUND_RECEIPT_EXCLUSION STRING,
                SMA_RESTR_CODE STRING,
                SMA_RETL_CHG_TYPE STRING,
                SMA_RETL_MULT_UNIT STRING,
                SMA_RETL_MULT_UOM STRING,
                SMA_RETL_PRC_ENTRY_CHG STRING,
                SMA_RETL_UOM STRING,
                SMA_RETL_VENDOR STRING,
                SMA_SBW_IND STRING,
                SMA_SEGMENT STRING,
                SMA_SELL_RETL STRING,
                SMA_SHELF_TAG_REQ STRING,
                SMA_SMR_EFF_DATE STRING,
                SMA_SOURCE_METHOD STRING,
                SMA_SOURCE_WHSE STRING,
                SMA_STATUS_DATE STRING,
                SMA_STORE STRING,
                SMA_STORE_DIV STRING,
                SMA_STORE_STATE STRING,
                SMA_STORE_ZONE STRING,
                SMA_SUB_COMM INTEGER,
                SMA_SUB_DEPT STRING,
                SMA_TAG_REQUIRED STRING,
                SMA_TAG_SZ STRING,
                SMA_TARE_PCT STRING,
                SMA_TARE_UOM INTEGER,
                SMA_TAX_1 STRING,
                SMA_TAX_2 STRING,
                SMA_TAX_3 STRING,
                SMA_TAX_4 STRING,
                SMA_TAX_5 STRING,
                SMA_TAX_6 STRING,
                SMA_TAX_7 STRING,
                SMA_TAX_8 STRING,
                SMA_TAX_CATEG STRING,
                SMA_TAX_CHG_IND STRING,
                SMA_TENDER_RESTRICTION_CODE STRING,
                SMA_TIBCO_DATE STRING,
                SMA_TRANSMIT_DATE STRING,
                SMA_UNIT_PRICE_CODE STRING,
                SMA_UOM_OF_PRICE STRING,
                SMA_UPC_DESC STRING,
                SMA_UPC_OVERRIDE_GRP_NUM STRING,
                SMA_VEND_COUPON_FAM1 INTEGER,
                SMA_VEND_COUPON_FAM2 INTEGER,
                SMA_VEND_NUM STRING,
                SMA_VENDOR_PACK INTEGER,
                SMA_WIC_ALT STRING,
                SMA_WIC_IND STRING,
                SMA_WIC_SHLF_MIN STRING,
                SMA_WPG STRING,
                SMA_ITM_EFF_DATE STRING,
                DSS_PRODUCT_DESC STRING,
                DSS_PRIMARY_ID STRING,
                DSS_PRIMARY_DESC STRING,
                DSS_SUB_CATEGORY_DESC STRING,
                DSS_CATEGORY_ID STRING,
                DSS_CATEGORY_DESC STRING,
                DSS_SUPER_CATEGORY_DESC STRING,
                DSS_ALL_CATEGORY_ID STRING,
                DSS_ALL_CATEGORY_DESC STRING,
                DSS_MDSE_PROGRAM_ID STRING,
                DSS_MDSE_PROGRAM_DESC STRING,
                DSS_PRICE_MASTER_ID STRING,
                DSS_PRICE_MASTER_DESC STRING,
                DSS_BUYER_ID STRING,
                DSS_BUYER_DESC STRING,
                DSS_PRICE_SENS_ID STRING,
                DSS_PRICE_SENS_SHORT_DESC STRING,
                DSS_PRICE_SENS_LONG_DESC STRING,
                DSS_MANCODE_ID STRING,
                DSS_MANCODE_DESC STRING,
                DSS_PRIVATE_BRAND_ID STRING,
                DSS_PRIVATE_BRAND_DESC STRING,
                DSS_PRODUCT_STATUS_ID STRING,
                DSS_PRODUCT_STATUS_DESC STRING,
                DSS_PRODUCT_UOM_DESC STRING,
                DSS_PRODUCT_PACK_QTY STRING,
                DSS_DIRECTOR_ID STRING,
                DSS_DIRECTOR_DESC STRING,
                DSS_DIRECTOR_GROUP_DESC STRING,
                DSS_MAJOR_CATEGORY_ID STRING,
                DSS_MAJOR_CATEGORY_DESC STRING,
                DSS_PLANOGRAM_ID STRING,
                DSS_PLANOGRAM_DESC STRING,
                DSS_HEIGHT STRING,
                DSS_WIDTH STRING,
                DSS_DEPTH STRING,
                DSS_BRAND_ID STRING,
                DSS_BRAND_DESC STRING,
                DSS_LIFO_POOL_ID STRING,
                DSS_GROUP_TYPE STRING,
                DSS_DATE_UPDATED STRING,
                DSS_EQUIVALENT_SIZE_ID STRING,
                DSS_EQUIVALENT_SIZE_DESC STRING,
                DSS_EQUIV_SIZE STRING,
                DSS_SCAN_TYPE_ID STRING,
                DSS_SCAN_TYPE_DESC STRING,
                DSS_STORE_HANDLING_CODE STRING,
                DSS_SHC_DESC STRING,
                DSS_GS_RATING_DESC STRING,
                DSS_GS_DATE_RATED STRING,
                DSS_PRIVATE_LABEL_ID STRING,
                DSS_PRIVATE_LABEL_DESC STRING,
                DSS_PRODUCT_DIMESION_L_BYTE STRING,
                DSS_PRDT_NAME STRING,
                DSS_MFR_NAME STRING,
                DSS_BRAND_NAME STRING,
                DSS_KEYWORD STRING,
                DSS_META_DESC STRING,
                DSS_META_KEYWORD STRING,
                DSS_SRVG_SZ_DSC STRING,
                DSS_SRVG_SZ_UOM_DSC STRING,
                DSS_SRVG_PER_CTNR STRING,
                DSS_NEW_PRDT_FLG STRING,
                DSS_OVRLN_DSC STRING,
                DSS_ALT_SRVG_SZ_DSC STRING,
                DSS_ALT_SRVG_SZ_UOM STRING,
                DSS_AVG_UNIT_WT STRING,
                DSS_PRIVATE_BRAND_CD STRING,
                DSS_MENU_LBL_FLG STRING,
                PRD2STORE_BANNER_ID STRING,
                PRD2STORE_PROMO_CODE STRING,
                PRD2STORE_SPLN_AD_CD STRING,
                PRD2STORE_TAG_TYP_CD STRING,
                PRD2STORE_WINE_VALUE_FLAG STRING,
                PRD2STORE_BUY_QTY STRING,
                PRD2STORE_LMT_QTY STRING,
                PRD2STORE_SALE_STRT_DT STRING,
                PRD2STORE_SALE_END_DT STRING,
                PRD2STORE_AGE_FLAG STRING,
                PRD2STORE_AGE_CODE STRING,
                PRD2STORE_SWAP_SAVE_UPC STRING,
                SCRTX_DET_FREQ_SHOP_TYPE INTEGER,
                SCRTX_DET_FREQ_SHOP_VAL FLOAT,
                SCRTX_DET_QTY_RQRD_FG INTEGER,
                SCRTX_DET_MIX_MATCH_CD INTEGER,
                SCRTX_DET_CENTRAL_ITEM INTEGER,
                SCRTX_DET_SLS_RESTRICT_GRP INTEGER,
                RTX_TYPE INTEGER,
                SCRTX_DET_PLU_BTCH_NBR INTEGER,
                SCRTX_DET_OP_CODE INTEGER,
                SCRTX_DET_RCPT_DESCR STRING,
                SCRTX_DET_NON_MDSE_ID INTEGER,
                SCRTX_DET_NG_ENTRY_FG INTEGER,
                SCRTX_DET_STR_CPN_FG INTEGER,
                SCRTX_DET_VEN_CPN_FG INTEGER,
                SCRTX_DET_MAN_PRC_FG INTEGER,
                SCRTX_DET_INTRNL_ID INTEGER,
                SCRTX_DET_DEA_GRP INTEGER,
                SCRTX_DET_COMP_TYPE INTEGER,
                SCRTX_DET_COMP_PRC STRING,
                SCRTX_DET_COMP_QTY INTEGER,
                SCRTX_DET_BLK_GRP INTEGER,
                SCRTX_DET_RSTRCSALE_BRCD_FG INTEGER,
                SCRTX_DET_NON_RX_HEALTH_FG INTEGER,
                SCRTX_DET_RX_FG INTEGER,
                SCRTX_DET_WIC_CVV_FG INTEGER,
                PRICE_UNIT_PRICE FLOAT,
                PRICE_UOM_CD STRING,
                PRICE_MLT_QUANTITY STRING,
                PRICE_PROMO_RTL_PRC FLOAT,
                PRICE_PROMO_UNIT_PRICE FLOAT,
                PRICE_PROMOTIONAL_QUANTITY STRING,
                PRICE_START_DATE STRING,
                PRICE_END_DATE STRING,
                TPRX001_STORE_SID_NBR STRING,
                TPRX001_ITEM_NBR STRING,
                TPRX001_ITEM_SRC_CD STRING,
                TPRX001_CPN_SRC_CD STRING,
                TPRX001_RTL_PRC_EFF_DT STRING,
                TPRX001_ITEM_PROMO_FLG STRING,
                TPRX001_PROMO_TYP_CD STRING,
                TPRX001_AD_TYP_CD STRING,
                TPRX001_PROMO_DSC STRING,
                TPRX001_MIX_MTCH_FLG STRING,
                TPRX001_PRC_STRAT_CD STRING,
                TPRX001_LOYAL_CRD_FLG STRING,
                TPRX001_SCAN_AUTH_FLG STRING,
                TPRX001_MDSE_AUTH_FLG STRING,
                TPRX001_SBT_FLG STRING,
                TPRX001_SBT_VEND_ID STRING,
                TPRX001_SBT_VEND_NET_CST DECIMAL(8,3),
                TPRX001_SCAN_DAUTH_DT STRING,
                TPRX001_SCAN_PRVWK_RTL_MLT STRING,
                TPRX001_SCAN_PRVWK_RTL_PRC DECIMAL(7,2),
                TPRX001_SCANPRVDAY_RTL_MLT STRING,
                TPRX001_SCANPRVDAY_RTL_PRC DECIMAL(7,2),
                TPRX001_TAG_PRV_WK_RTL_MLT STRING,
                TPRX001_TAG_PRV_WK_RTL_PRC DECIMAL(7,2),
                ALTERNATE_UPC STRING,
                INVENTORY_CODE STRING,
                INVENTORY_QTY_ON_HAND STRING,
                INVENTORY_SUPER_CATEGORY STRING,
                INVENTORY_MAJOR_CATEGORY STRING,
                INVENTORY_INTMD_CATEGORY STRING,
                INSERT_ID STRING,
                INSERT_TIMESTAMP TIMESTAMP,
                LAST_UPDATE_ID STRING,
                LAST_UPDATE_TIMESTAMP TIMESTAMP,
                EFF_TS TIMESTAMP) USING delta
  Location '{}'
  PARTITIONED BY (SMA_STORE)
  """.format('/mnt/centralized-price-promo/Delhaize/Outbound/SDM/Item_Master_delta'))

# COMMAND ----------

# spark.sql("""DELETE FROM ItemMainAhold;""")
# spark.sql("""DROP TABLE IF EXISTS ItemMainAhold;""")
# spark.sql("""DELETE FROM PriceChange;""")
# spark.sql("""DROP TABLE IF EXISTS PriceChange;""")
# spark.sql("""DELETE FROM PromotionLink;""")
# spark.sql("""DROP TABLE IF EXISTS PromotionLink;""")
# spark.sql("""DELETE FROM PromotionMain;""")
# spark.sql("""DROP TABLE IF EXISTS PromotionMain;""")
# spark.sql("""DELETE FROM CouponAholdTable;""")
# spark.sql("""DROP TABLE IF EXISTS CouponAholdTable;""")
# spark.sql("""DELETE FROM itemMasterAhold;""")
# spark.sql("""DROP TABLE IF EXISTS itemMasterAhold;""")

# dbutils.fs.rm('/mnt/centralized-price-promo/Coupon/Outbound/CDM',recurse=True)
# dbutils.fs.rm('/mnt/centralized-price-promo/Item/Outbound/CDM',recurse=True)

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
fullLoad = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("sep", '|').schema(itemRaw_schema).load("/mnt/ahold-centralized-price-promo/Fullitem/Inbound/RDS/2021/07/04/UnzippedSink/TEST.NQ.GM.SMA.DB.ITEM.LOAD.ZIP/TEST.NQ.GM.SMA.DB.ITEM.LOAD.txt")
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
dailyLoad = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("sep", '|').schema(itemRaw_schema).load("/mnt/ahold-centralized-price-promo/Item/Inbound/RDS/2021/07/04/UnzippedSink/TEST.NQ.CI.SMA.DB.ITEM.MAINT.ZIP/TEST.NQ.CI.SMA.DB.ITEM.MAINT.txt")
dailyLoad = quinn.with_some_columns_renamed(itemflat_promotable, change_col_name)(dailyLoad)
temp_table_name = "dailyLoad"
dailyLoad.createOrReplaceTempView(temp_table_name)
display(dailyLoad)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct sma_store from fullLoad

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct sma_store from dailyLoad

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from fullLoad where SMA_LINK_HDR_COUPON > 0

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from fullLoad where SMA_LINK_END_DATE > 0 and SMA_LINK_END_DATE < 20210706 and SMA_CHG_TYPE in (2,3,4,5)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from ItemMainAhold where SMA_GTIN_NUM=20094600000

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from (select * from fullLoad left outer join CouponAholdTable on SMA_LINK_HDR_COUPON=coupon_no and sma_store=location where SMA_LINK_HDR_COUPON > 0) where location is NULL

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC 
# MAGIC select * from dailyLoad where sma_store=758 and sma_gtin_num in (1504,1562,3056,3116,3117,3121,3178,3179,3207,3239,3293,3294,3445,3450,3492,3496,3605)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC PERF_DETL_SUB_TYPE, SALE_PRICE, SALE_QUANTITY, SMA_GTIN_NUM, SMA_LINK_HDR_COUPON, SMA_LINK_HDR_LOCATION, SMA_MULT_UNIT_RETL, SMA_RETL_MULT_UNIT, LAST_UPDATE_TIMESTAMP  from itemMasterAhold where sma_store=20 and sma_gtin_num like '%340009392%'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC PERF_DETL_SUB_TYPE, SALE_PRICE, SALE_QUANTITY, SMA_GTIN_NUM, SMA_LINK_HDR_COUPON, SMA_LINK_HDR_LOCATION, SMA_MULT_UNIT_RETL, SMA_RETL_MULT_UNIT, LAST_UPDATE_TIMESTAMP  from itemMasterAhold where sma_store=20 and sma_gtin_num like '%340009392%'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from PromotionMain where SMA_PROMO_LINK_UPC =1920049119 and sma_store=20

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from PromotionLink where SMA_PROMO_LINK_UPC =1920049119 and sma_store=20

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from pricechange where SMA_gtin_num =1920049119 and sma_store=20

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from PromotionMain where SMA_PROMO_LINK_UPC like '%340009392%' and sma_store=20

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from PromotionLink where SMA_ITM_EFF_DATE='2021-07-04'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from PromotionMain where SMA_ITM_EFF_DATE='2021-06-28'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from PromotionLink where SMA_PROMO_LINK_UPC like '%340009392%' and sma_store=20

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select COUPON_NO, NUM_TO_BUY_1, CHANGE_AMOUNT_PCT from CouponAholdTable where store=20 and coupon_no in (41100240344, 41100240997, 41100230854, 41100232241)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select COUPON_NO, NUM_TO_BUY_1, CHANGE_AMOUNT_PCT from CouponAholdTable where store=20 and coupon_no in (41100247495, 41100240836, 41100240949, 41100212779, 41100150535)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC update CouponAholdTable set CHANGE_AMOUNT_PCT=34.71 where store=20 and coupon_no = 41100150535

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from itemMasterAhold where SMA_ITM_EFF_DATE <= current_date() and SALE_PRICE > 0

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct sma_store from itemMasterAhold 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select SMA_STORE, SMA_GTIN_NUM, SMA_LEGACY_ITEM, SMA_PRIME_UPC, EFF_TS, SMA_ITEM_DESC, SMA_SUB_DEPT, SMA_MULT_UNIT_RETL, SMA_RETL_MULT_UNIT, SALE_PRICE, SALE_QUANTITY, SMA_LINK_HDR_COUPON, PERF_DETL_SUB_TYPE, SMA_LINK_START_DATE, SMA_LINK_END_DATE, SMA_ITEM_SIZE, SMA_ITEM_UOM, SMA_RETL_MULT_UOM, SMA_SBW_IND, HOW_TO_SELL, AVG_WGT, SMA_VEND_COUPON_FAM1, SMA_VEND_COUPON_FAM2, BTL_DPST_AMT, SMA_TAX_1, SMA_TAX_2, SMA_TAX_3, SMA_TAX_4, SMA_TAX_5, SMA_TAX_6, SMA_TAX_7, SMA_TAX_8, SMA_RECALL_FLAG, SMA_WIC_IND, SMA_RESTR_CODE, SMA_FSA_IND, SMA_FOOD_STAMP_IND, SMA_LINK_UPC, SMA_TARE_PCT, SMA_FIXED_TARE_WGT, SMA_CONTRIB_QTY, SMA_ITEM_STATUS, SMA_STATUS_DATE, LAST_UPDATE_TIMESTAMP from itemMasterAhold where SMA_DEST_STORE= '0758' order by SMA_GTIN_NUM asc

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
# MAGIC Select count(*) from ItemMainAhold 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC update  PromotionMain set SMA_ITM_EFF_DATE = current_date() where SMA_STORE=2812 and  SMA_PROMO_LINK_UPC=00070815232117

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC update  PromotionLink set SMA_ITM_EFF_DATE = current_date() where SMA_STORE=2812 and  SMA_PROMO_LINK_UPC=00070815232117

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select SALE_PRICE from itemMasterAhold where SMA_DEST_STORE= '0758' and SMA_GTIN_NUM in ('954259')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from (Select SMA_STORE, SMA_PROMO_LINK_UPC, SMA_LINK_HDR_COUPON, INSERT_TIMESTAMP, count(*) as count from PromotionLink group by SMA_STORE, SMA_PROMO_LINK_UPC, SMA_LINK_HDR_COUPON, INSERT_TIMESTAMP) where count > 1

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

# MAGIC %sql
# MAGIC select * from (select SMA_STORE, SMA_GTIN_NUM, SMA_ITM_EFF_DATE, INSERT_TIMESTAMP, DENSE_RANK() OVER (PARTITION BY SMA_STORE, SMA_GTIN_NUM ORDER BY SMA_ITM_EFF_DATE desc, INSERT_TIMESTAMP desc) AS Rank  
# MAGIC from PriceChange where SMA_ITM_EFF_DATE < current_date() ORDER BY SMA_STORE, SMA_GTIN_NUM) where Rank > 2

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from (select SMA_STORE, SMA_GTIN_NUM,count(*) as count from PriceChange group BY SMA_STORE, SMA_GTIN_NUM,SMA_ITM_EFF_DATE, INSERT_TIMESTAMP) where count > 2

# COMMAND ----------

