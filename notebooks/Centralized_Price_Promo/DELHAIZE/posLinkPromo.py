# Databricks notebook source
from pyspark.sql.types import * 
from pyspark.sql import *
import json
from pyspark.sql.functions import *
from pytz import timezone
import datetime
import logging 
import quinn
from functools import reduce
from delta.tables import *
from pyspark.sql.functions import regexp_extract
import jsonpickle
from json import JSONEncoder

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = False;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##Calling Logger

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/Logging

# COMMAND ----------

custom_logfile_Name ='posLinkPromoMaintainenceCustomlog'
loggerAtt, p_logfile, file_date = logger(custom_logfile_Name, '/tmp/')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Error Class Definition

# COMMAND ----------

class ErrorReturn:
  def __init__(self, status, errorMessage, functionName):
    self.status = status
    self.errorMessage = str(errorMessage)
    self.functionName = functionName
    self.time = datetime.datetime.now(timezone("America/Chicago")).isoformat()
  def exit(self):
    dbutils.notebook.exit(json.dumps(self.__dict__))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ABC FRAMEWORK

# COMMAND ----------

ABCChecks = {}
def ABC(**kwargs):
  for key, value in kwargs.items():
      #loggerAtt.info("The value of {} is {}".format(key, value))
      ABCChecks[key] = value

# COMMAND ----------

def Merge(dict1, dict2):
    res = {**dict1, **dict2}
    return res

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ##Widgets for getting dynamic paramters from ADF 

# COMMAND ----------

loggerAtt.info("========Widgets call initiated==========")
dbutils.widgets.removeAll()
dbutils.widgets.text("fileName","")
dbutils.widgets.text("filePath","")
dbutils.widgets.text("inputDirectory","")
dbutils.widgets.text("outputDirectory","")
dbutils.widgets.text("container","")
dbutils.widgets.text("pipelineID","")
dbutils.widgets.text("MountPoint","")
dbutils.widgets.text("deltaPath","")
dbutils.widgets.text("logFilesPath","")
dbutils.widgets.text("invalidLinkRecordsPath","")
dbutils.widgets.text("invalidPromoRecordsPath","")
dbutils.widgets.text("clientId","")
dbutils.widgets.text("keyVaultName","")
dbutils.widgets.text("storeDeltaPath","")
dbutils.widgets.text("itemDeltaPath","")
dbutils.widgets.text("archivalFilePath","")
dbutils.widgets.text("itemTempEffDeltaPath","")
dbutils.widgets.text("couponOutboundPath","")
dbutils.widgets.text("bottleDepositDeltaPath","")
dbutils.widgets.text("promoLinkingDeltaPath","")


fileName=dbutils.widgets.get("fileName")
filePath=dbutils.widgets.get("filePath")
inputDirectory=dbutils.widgets.get("inputDirectory")
outputDirectory=dbutils.widgets.get("outputDirectory")
container=dbutils.widgets.get("container")
pipelineid=dbutils.widgets.get("pipelineID")
mount_point=dbutils.widgets.get("MountPoint")
couponDeltaPath=dbutils.widgets.get("deltaPath")
storeDeltaPath=dbutils.widgets.get("storeDeltaPath")
itemDeltaPath=dbutils.widgets.get("itemDeltaPath")
logFilesPath=dbutils.widgets.get("logFilesPath")
invalidLinkRecordsPath=dbutils.widgets.get("invalidLinkRecordsPath")
invalidPromoRecordsPath=dbutils.widgets.get("invalidPromoRecordsPath")
Date = datetime.datetime.now(timezone("America/Chicago")).strftime("%Y-%m-%d")
file_location = '/mnt' + '/' + inputDirectory + '/' + filePath +'/' + fileName 
inputSource= 'abfss://' + inputDirectory + '@' + container + '.dfs.core.windows.net/'
outputSource= 'abfss://' + outputDirectory + '@' + container + '.dfs.core.windows.net/'
clientId=dbutils.widgets.get("clientId")
keyVaultName=dbutils.widgets.get("keyVaultName")
couponArchivalpath=dbutils.widgets.get("archivalFilePath")
itemTempEffDeltaPath=dbutils.widgets.get("itemTempEffDeltaPath")
couponOutboundPath=dbutils.widgets.get("couponOutboundPath")
bottleDepositDeltaPath=dbutils.widgets.get("bottleDepositDeltaPath")
promoLinkingDeltaPath=dbutils.widgets.get("promoLinkingDeltaPath")

loggerAtt.info(f"Date : {Date}")
loggerAtt.info(f"File Location on Mount Point : {file_location}")
loggerAtt.info(f"Source or File Location on Container : {inputSource}")
loggerAtt.info(f"Source or File Location on Container : {outputSource}")



# pipelineid = "temp"
# itemDeltaPath='/mnt/delhaize-centralized-price-promo/POSdaily/Outbound/SDM/Item_Main'
# couponArchivalpath= '/mnt/delhaize-centralized-price-promo/POSdaily/Outbound/SDM/ArchivalLinkPromo'
# mount_point = "/mnt/delhaize-centralized-price-promo"
# couponOutboundPath = "/mnt/delhaize-centralized-price-promo/POSdaily/Outbound/CDM"
# couponDeltaPath = "/mnt/delhaize-centralized-price-promo/POSdaily/Outbound/SDM/Coupon_delta"
# storeDeltaPath = '/mnt/delhaize-centralized-price-promo/Storedetails/Outbound/SDM/Store_delta'
# logFilesPath = "/mnt/delhaize-centralized-price-promo/POSdaily/Outbound/SDM/Logfiles"
# source= "abfss://delhaize-centralized-price-promo@rs06ue2dmasadata02.dfs.core.windows.net/"
# invalidLinkRecordsPath: "/mnt/delhaize-centralized-price-promo/POSdaily/Outbound/SDM/InvalidRecords/Link"
# invalidPromoRecordsPath: "/mnt/delhaize-centralized-price-promo/POSdaily/Outbound/SDM/InvalidRecords/Promo"
# file_location ="/mnt/delhaize-centralized-price-promo/POSemergency/Foodlion/Inbound/RDS/2021/03/02/posLinkPromoFile.txt"
#file_location ="/mnt/delhaize-centralized-price-promo/POSdaily/Hannaford/Inbound/RDS/2021/03/02/FileWithRowNumber"
# file_location ="/mnt/delhaize-centralized-price-promo/POSemergency/Foodlion/Inbound/RDS/2021/03/02/posLinkPromoFile.txt"
# clientId="2cbef55f-e5b2-403d-a3c0-430d6f5e83d4"
# bottleDepositDeltaPath = "/mnt/delhaize-centralized-price-promo/POSdaily/Outbound/SDM/bottleDeposit"
# keyVaultName="MerchandisingApp-Key-Vault-DEV"
# itemTempEffDeltaPath = "/mnt/delhaize-centralized-price-promo/POSdaily/Outbound/SDM/itemEffTemp"
# promoLinkingDeltaPath = '/mnt/delhaize-centralized-price-promo/POSdaily/Outbound/SDM/Promo_Linking_delta'

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Mounting ADLS location

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/Mount_Point_Creation

# COMMAND ----------

try:
  mounting(mount_point, inputSource, clientId, keyVaultName)
  ABC(MountCheck=1)
except Exception as ex:
  # send error message to ADF and send email notification
  ABC(MountCheck=0)
  loggerAtt.error(str(ex))
  err = ErrorReturn('Error', ex,'Mounting input')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

try:
  mounting(mount_point, outputSource, clientId, keyVaultName)
  ABC(MountCheck=1)
except Exception as ex:
  # send error message to ADF and send email notification
  ABC(MountCheck=0)
  loggerAtt.error(str(ex))
  err = ErrorReturn('Error', ex,'Mounting output')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

inputDataSchema = StructType([
                           StructField("Data",StringType(),True),
                           StructField("RowNumber",IntegerType(),False)                           
])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Delta table creation

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Coupon Table

# COMMAND ----------

try:
  ABC(DeltaTableCreateCheck=1)
  spark.sql("""
  CREATE TABLE IF NOT EXISTS CouponDelhaizeTable (
  BANNER_ID STRING,
  LOCATION STRING,
  STATUS STRING,
  COUPON_NO LONG,
  START_DATE DATE,
  END_DATE DATE,
  DEL_DATE DATE,
  PERF_DETL_SUB_TYPE INTEGER,
  LIMIT STRING,
  CHANGE_AMOUNT_PCT DOUBLE,
  CLUB_CARD STRING,
  MIN_QUANTITY INTEGER,
  BUY_QUANTITY INTEGER,
  GET_QUANTITY INTEGER,
  SALE_QUANTITY INTEGER,
  DESCRIPTION STRING,
  SELL_BY_WEIGHT_IND STRING,
  AHO_PERF_DETAIL_ID LONG,
  PROMO_HDR_FILE_NUM INTEGER,
  PROMO_HDR_ACTION INTEGER,
  PROMO_HDR_PART_OFFSET INTEGER,
  PROMO_HDR_PART_LENGTH INTEGER,
  PROMO_HDR_BIT_FLD INTEGER,
  PROMO_HDR_PEND_DATE STRING,
  PROMO_HDR_PEND_TIME INTEGER,
  PROMO_HDR_VERSION INTEGER,
  PROMO_HDR_BATCH_NUM INTEGER,
  PROMO_HDR_STATUS INTEGER,
  PROMO_TYPE INTEGER,
  PROMO_DESCRIPTION STRING,
  PROMO_DEPARTMENT STRING,
  PROMO_MEM_CARD_SCHEME STRING,
  PROMO_REWARD_VALUE INTEGER,
  PROMO_REWARD_VALUE_AMT DOUBLE,
  PROMO_REWARD_VALUE_PER DOUBLE,
  PROMO_MEM_CARD_REQUIRED INTEGER,
  PROMO_ALL_CARD_SCHEMES INTEGER,
  PROMO_CARD_SCHEME INTEGER,
  PROMO_LIMITED_QTY INTEGER,
  PROMO_ENHANCED_GROUP_TYPE STRING,
  PROMO_ENHANCED_THRESHOLD_QTY STRING,
  PROMO_ENHANCED_STEP_COUNT_QTY STRING,
  PROMO_START_TIME INTEGER,
  PROMO_END_TIME INTEGER,
  PROMO_ACTIVATION_DAY_1 INTEGER,
  PROMO_ACTIVATION_DAY_2 INTEGER,
  PROMO_ACTIVATION_DAY_3 INTEGER,
  PROMO_ACTIVATION_DAY_4 INTEGER,
  PROMO_ACTIVATION_DAY_5 INTEGER,
  PROMO_ACTIVATION_DAY_6 INTEGER,
  PROMO_ACTIVATION_DAY_7 INTEGER,
  PROMO_ACTIVATION_TIME_1 INTEGER,
  PROMO_ACTIVATION_TIME_2 INTEGER,
  PROMO_ACTIVATION_TIME_3 INTEGER,
  PROMO_ACTIVATION_TIME_4 INTEGER,
  PROMO_ACTIVATION_TIME_5 INTEGER,
  PROMO_ACTIVATION_TIME_6 INTEGER,
  PROMO_ACTIVATION_TIME_7 INTEGER,  
  PROMO_TRIGGER_FLAGS_2 LONG,
  PROMO_LOW_HIGH_REWARD INTEGER,
  PROMO_MIN_ITEM_VALUE DOUBLE,
  PROMO_MIN_ITEM_WEIGHT DOUBLE,
  PROMO_MIN_PURCHASE DOUBLE,
  PROMO_DELAYED_PROMO INTEGER,
  PROMO_CASHIER_ENTERED INTEGER,
  PROMO_REQ_COUPON_CODE INTEGER,
  PROMO_LINKING_PROMO INTEGER,
  PROMO_MAX_ITEM_WEIGHT DOUBLE,
  PROMO_SEGMENTS_1 INTEGER,
  PROMO_SEGMENTS_2 INTEGER,
  PROMO_SEGMENTS_3 INTEGER,
  PROMO_SEGMENTS_4 INTEGER,
  PROMO_SEGMENTS_5 INTEGER,
  PROMO_SEGMENTS_6 INTEGER,
  PROMO_SEGMENTS_7 INTEGER,
  PROMO_SEGMENTS_8 INTEGER,
  PROMO_SEGMENTS_9 INTEGER,
  PROMO_SEGMENTS_10 INTEGER,
  PROMO_SEGMENTS_11 INTEGER,
  PROMO_SEGMENTS_12 INTEGER,
  PROMO_SEGMENTS_13 INTEGER,
  PROMO_SEGMENTS_14 INTEGER,
  PROMO_SEGMENTS_15 INTEGER,
  PROMO_SEGMENTS_16 INTEGER,
  PROMO_UPD_LOYALTY_SER INTEGER,
  PROMO_CPN_REQ_TYPE INTEGER,
  PROMO_CREDIT_PROGRAM_ID INTEGER,
  PROMO_PROMO_EXTERNAL_ID INTEGER,
  PROMO_DEPARTMENT_4DIG INTEGER,
  INSERT_ID STRING,
  INSERT_TIMESTAMP TIMESTAMP,
  LAST_UPDATE_ID STRING,
  LAST_UPDATE_TIMESTAMP TIMESTAMP
  )
  USING delta
  Location '{}'
  PARTITIONED BY (LOCATION)
  """.format(couponDeltaPath))
except Exception as ex:
  ABC(DeltaTableCreateCheck = 0)
  loggerAtt.error(ex)
  err = ErrorReturn('Error', ex,'deltaCreator CouponDelhaizeTable')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))
    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Item Main Table

# COMMAND ----------

try:
  ABC(DeltaTableCreateCheck=1)
  spark.sql(""" CREATE TABLE IF NOT EXISTS Item_Main(
                RTX_STORE STRING,
                BANNER_ID STRING,
                COUPON_NO LONG,
                RTX_BATCH LONG,
                RTX_TYPE INTEGER,
                RTX_UPC LONG,
                RTX_LOAD STRING,
                SCRTX_DET_PLU_BTCH_NBR INTEGER,
                SCRTX_DET_OP_CODE INTEGER,
                SCRTX_DET_ITM_ID LONG,
                SCRTX_DET_STR_HIER_ID INTEGER,
                SCRTX_DET_DFLT_RTN_LOC_ID INTEGER,
                SCRTX_DET_MSG_CD INTEGER,
                SCRTX_DET_DSPL_DESCR STRING,
                SCRTX_DET_SLS_RESTRICT_GRP INTEGER,
                SCRTX_DET_RCPT_DESCR STRING,
                SCRTX_DET_TAXABILITY_CD INTEGER,
                SCRTX_DET_MDSE_XREF_ID INTEGER,
                SCRTX_DET_NON_MDSE_ID INTEGER,
                SCRTX_DET_UOM STRING,
                SCRTX_DET_UNT_QTY STRING,
                SCRTX_DET_LIN_ITM_CD INTEGER,
                SCRTX_DET_MD_FG INTEGER,
                SCRTX_DET_QTY_RQRD_FG INTEGER,
                SCRTX_DET_SUBPRD_CNT INTEGER,
                SCRTX_DET_QTY_ALLOWED_FG INTEGER,
                SCRTX_DET_SLS_AUTH_FG INTEGER,
                SCRTX_DET_FOOD_STAMP_FG  INTEGER,
                SCRTX_DET_WIC_FG INTEGER,
                SCRTX_DET_PERPET_INV_FG INTEGER,
                SCRTX_DET_RTL_PRC FLOAT,
                SCRTX_HDR_ACT_DATE DATE,
                SCRTX_DET_UNT_CST STRING,
                SCRTX_DET_MAN_PRC_LVL INTEGER,
                SCRTX_DET_MIN_MDSE_AMT STRING,
                SCRTX_DET_RTL_PRC_DATE STRING,
                SCRTX_DET_SERIAL_MDSE_FG INTEGER,
                SCRTX_DET_CNTR_PRC STRING,
                SCRTX_DET_MAX_MDSE_AMT STRING,
                SCRTX_DET_CNTR_PRC_DATE STRING,
                SCRTX_DET_NG_ENTRY_FG INTEGER,
                SCRTX_DET_STR_CPN_FG INTEGER,
                SCRTX_DET_VEN_CPN_FG INTEGER,
                SCRTX_DET_MAN_PRC_FG INTEGER,
                SCRTX_DET_WGT_ITM_FG INTEGER,
                SCRTX_DET_NON_DISC_FG INTEGER,
                SCRTX_DET_COST_PLUS_FG INTEGER,
                SCRTX_DET_PRC_VRFY_FG INTEGER,
                SCRTX_DET_PRC_OVRD_FG INTEGER,
                SCRTX_DET_SPLR_PROM_FG INTEGER,
                SCRTX_DET_SAVE_DISC_FG INTEGER,
                SCRTX_DET_ITM_ONSALE_FG INTEGER,
                SCRTX_DET_INHBT_QTY_FG INTEGER,
                SCRTX_DET_DCML_QTY_FG INTEGER,
                SCRTX_DET_SHELF_LBL_RQRD_FG INTEGER,
                SCRTX_DET_TAX_RATE1_FG INTEGER,
                SCRTX_DET_TAX_RATE2_FG INTEGER,
                SCRTX_DET_TAX_RATE3_FG INTEGER,
                SCRTX_DET_TAX_RATE4_FG INTEGER,
                SCRTX_DET_TAX_RATE5_FG INTEGER,
                SCRTX_DET_TAX_RATE6_FG STRING,
                SCRTX_DET_TAX_RATE7_FG INTEGER,
                SCRTX_DET_TAX_RATE8_FG STRING,
                SCRTX_DET_COST_CASE_PRC STRING,
                SCRTX_DET_DATE_COST_CASE_PRC STRING,
                SCRTX_DET_UNIT_CASE INTEGER,
                SCRTX_DET_MIX_MATCH_CD INTEGER,
                SCRTX_DET_RTN_CD INTEGER,
                SCRTX_DET_FAMILY_CD INTEGER,
                SCRTX_DET_SUBDEP_ID LONG,
                SCRTX_DET_DISC_CD INTEGER,
                SCRTX_DET_LBL_QTY INTEGER,
                SCRTX_DET_SCALE_FG INTEGER,
                SCRTX_DET_LOCAL_DEL_FG INTEGER,
                SCRTX_DET_HOST_DEL_FG INTEGER,
                SCRTX_DET_HEAD_OFFICE_DEP LONG,
                SCRTX_DET_WGT_SCALE_FG INTEGER,
                SCRTX_DET_FREQ_SHOP_TYPE INTEGER,
                SCRTX_DET_FREQ_SHOP_VAL STRING,
                SCRTX_DET_SEC_FAMILY INTEGER,
                SCRTX_DET_POS_MSG INTEGER,
                SCRTX_DET_SHELF_LIFE_DAY INTEGER,
                SCRTX_DET_PROM_NBR INTEGER,
                SCRTX_DET_BCKT_NBR INTEGER,
                SCRTX_DET_EXTND_PROM_NBR INTEGER,
                SCRTX_DET_EXTND_BCKT_NBR INTEGER,
                SCRTX_DET_RCPT_DESCR1 STRING,
                SCRTX_DET_RCPT_DESCR2 STRING,
                SCRTX_DET_RCPT_DESCR3 STRING,
                SCRTX_DET_RCPT_DESCR4 STRING,
                SCRTX_DET_TAR_WGT_NBR INTEGER,
                SCRTX_DET_RSTRCT_LAYOUT INTEGER,
                SCRTX_DET_INTRNL_ID LONG,
                SCRTX_DET_OLD_PRC LONG,
                SCRTX_DET_QDX_FREQ_SHOP_VAL LONG,
                SCRTX_DET_VND_ID STRING,
                SCRTX_DET_VND_ITM_ID STRING,
                SCRTX_DET_VND_ITM_SZ STRING,
                SCRTX_DET_CMPRTV_UOM INTEGER,
                SCRTX_DET_CMPR_QTY LONG,
                SCRTX_DET_CMPR_UNT LONG,
                SCRTX_DET_BNS_CPN_FG INTEGER,
                SCRTX_DET_EX_MIN_PURCH_FG INTEGER,
                SCRTX_DET_FUEL_FG INTEGER,
                SCRTX_DET_SPR_AUTH_RQRD_FG INTEGER,
                SCRTX_DET_SSP_PRDCT_FG INTEGER,
                SCRTX_DET_NU06_FG INTEGER,
                SCRTX_DET_NU07_FG INTEGER,
                SCRTX_DET_NU08_FG INTEGER,
                SCRTX_DET_NU09_FG INTEGER,
                SCRTX_DET_NU10_FG INTEGER,
                SCRTX_DET_FREQ_SHOP_LMT INTEGER,
                SCRTX_DET_ITM_STATUS INTEGER,
                SCRTX_DET_DEA_GRP INTEGER,
                SCRTX_DET_BNS_BY_OPCODE INTEGER,
                SCRTX_DET_BNS_BY_DESCR STRING,
                SCRTX_DET_COMP_TYPE INTEGER,
                SCRTX_DET_COMP_PRC STRING,
                SCRTX_DET_COMP_QTY INTEGER,
                SCRTX_DET_ASSUME_QTY_FG INTEGER,
                SCRTX_DET_EXCISE_TAX_NBR INTEGER,
                SCRTX_DET_RTL_PRICE_DATE STRING,
                SCRTX_DET_PRC_RSN_ID INTEGER,
                SCRTX_DET_ITM_POINT INTEGER,
                SCRTX_DET_PRC_GRP_ID INTEGER,
                SCRTX_DET_SWW_CODE_FG INTEGER,
                SCRTX_DET_SHELF_STOCK_FG INTEGER,
                SCRTX_DET_PRT_PLUID_RCPT_FG INTEGER,
                SCRTX_DET_BLK_GRP INTEGER,
                SCRTX_DET_EXCHNGE_TENDER_ID INTEGER,
                SCRTX_DET_CAR_WASH_FG INTEGER,
                SCRTX_DET_EXMPT_FRM_PROM_FG INTEGER,
                SCRTX_DET_QSR_ITM_TYP INTEGER,
                SCRTX_DET_RSTRCSALE_BRCD_FG INTEGER,
                SCRTX_DET_NON_RX_HEALTH_FG INTEGER,
                SCRTX_DET_RX_FG INTEGER,
                SCRTX_DET_LNK_NBR INTEGER,
                SCRTX_DET_WIC_CVV_FG INTEGER,
                SCRTX_DET_CENTRAL_ITEM INTEGER,
                RowNumber LONG,
                INSERT_ID STRING,
                INSERT_TIMESTAMP TIMESTAMP,
                LAST_UPDATE_ID STRING,
                LAST_UPDATE_TIMESTAMP TIMESTAMP )
              USING delta 
              LOCATION '{}' 
              PARTITIONED BY (RTX_STORE, SCRTX_HDR_ACT_DATE)""".format(itemDeltaPath))
except Exception as ex:
  ABC(DeltaTableCreateCheck = 0)
  loggerAtt.error(ex)
  err = ErrorReturn('Error', ex,'itemDeltaPath deltaCreator')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Temp Item Main Table based on Effective Date

# COMMAND ----------

try:
  ABC(DeltaTableCreateCheck=1)
  spark.sql(""" CREATE  TABLE IF NOT EXISTS ItemMainEffTemp(
                SMA_DEST_STORE STRING,
                BANNER_ID STRING,
                SMA_LINK_HDR_COUPON STRING,
                SMA_BATCH_SERIAL_NBR INTEGER,
                SCRTX_DET_OP_CODE INTEGER,
                SMA_GTIN_NUM LONG,
                SMA_SUB_DEPT STRING,
                SMA_ITEM_DESC STRING,
                SMA_RESTR_CODE STRING,
                SCRTX_DET_RCPT_DESCR STRING,
                SCRTX_DET_NON_MDSE_ID INTEGER,
                SMA_RETL_MULT_UNIT STRING,
                SCRTX_DET_QTY_RQRD_FG INTEGER,
                SMA_FOOD_STAMP_IND STRING,
                SCRTX_DET_WIC_FG INTEGER,
                SMA_MULT_UNIT_RETL FLOAT,
                SMA_ITM_EFF_DATE STRING,
                SCRTX_DET_NG_ENTRY_FG INTEGER,
                SCRTX_DET_STR_CPN_FG INTEGER,
                SCRTX_DET_VEN_CPN_FG INTEGER,
                SCRTX_DET_MAN_PRC_FG INTEGER,
                SMA_SBW_IND STRING,
                SMA_TAX_1 STRING,
                SMA_TAX_2 STRING,
                SMA_TAX_3 STRING,
                SMA_TAX_4 STRING,
                SMA_TAX_5 STRING,
                SMA_TAX_6 STRING,
                SMA_TAX_7 STRING,
                SMA_TAX_8 STRING,
                SCRTX_DET_MIX_MATCH_CD INTEGER,
                SMA_VEND_COUPON_FAM1 STRING,
                SMA_BATCH_SUB_DEPT LONG,
                SCRTX_DET_FREQ_SHOP_TYPE INTEGER,
                SCRTX_DET_FREQ_SHOP_VAL STRING,
                SMA_VEND_COUPON_FAM2 STRING,
                SMA_FIXED_TARE_WGT STRING,
                SCRTX_DET_INTRNL_ID LONG,
                SMA_RETL_VENDOR STRING,
                SCRTX_DET_DEA_GRP INTEGER,
                SCRTX_DET_COMP_TYPE INTEGER,
                SCRTX_DET_COMP_PRC STRING,
                SCRTX_DET_COMP_QTY INTEGER,
                SCRTX_DET_BLK_GRP INTEGER,
                SCRTX_DET_RSTRCSALE_BRCD_FG INTEGER,
                SCRTX_DET_NON_RX_HEALTH_FG INTEGER,
                SCRTX_DET_RX_FG INTEGER,
                SCRTX_DET_LNK_NBR INTEGER,
                SCRTX_DET_WIC_CVV_FG INTEGER,
                SCRTX_DET_CENTRAL_ITEM INTEGER,
                SMA_STATUS_DATE STRING,
                SMA_SMR_EFF_DATE STRING,
                SMA_STORE STRING,
                SMA_VEND_NUM STRING,
                SMA_UPC_DESC STRING,
                SMA_WIC_IND STRING,
                SMA_LINK_UPC STRING,
                SMA_BOTTLE_DEPOSIT_IND STRING,
                SCRTX_DET_PLU_BTCH_NBR INTEGER,
                SMA_SELL_RETL STRING,
                ALTERNATE_UPC LONG,
                SCRTX_DET_SLS_RESTRICT_GRP INTEGER,
                RTX_TYPE INTEGER,
                ALT_UPC_FETCH LONG,
                BTL_DPST_AMT STRING,
                SMA_ITEM_STATUS STRING,
                SMA_FSA_IND String,
                SMA_LEGACY_ITEM STRING,
                SMA_PRIME_UPC STRING,
                INSERT_ID STRING,
                INSERT_TIMESTAMP TIMESTAMP,
                LAST_UPDATE_ID STRING,
                LAST_UPDATE_TIMESTAMP TIMESTAMP)
              USING delta 
              PARTITIONED BY (SMA_DEST_STORE)
              LOCATION '{}' """.format(itemTempEffDeltaPath))
except Exception as ex:
  ABC(DeltaTableCreateCheck = 0)
  loggerAtt.error(ex)
  err = ErrorReturn('Error', ex,'deltaCreator ItemMainTemp')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))
        

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Bottle Deposit Table

# COMMAND ----------

try:
  ABC(DeltaTableCreateCheck=1)
  spark.sql(""" CREATE  TABLE IF NOT EXISTS bottleDeposit(
                BOTTEL_DEPOSIT_STORE STRING,
                BOTTEL_DEPOSIT_LNK_NBR STRING,
                BOTTEL_DEPOSIT_ITM_ID LONG,
                BOTTEL_DEPOSIT_RTL_PRC FLOAT)
              USING delta 
              PARTITIONED BY (BOTTEL_DEPOSIT_STORE)
              LOCATION '{}' """.format(bottleDepositDeltaPath))
except Exception as ex:
  ABC(DeltaTableCreateCheck = 0)
  loggerAtt.error(ex)
  err = ErrorReturn('Error', ex,'bottleDepositDeltaPath deltaCreator')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Promotion Linking table

# COMMAND ----------

try:
  ABC(DeltaTableCreateCheck=1)
  spark.sql(""" CREATE  TABLE IF NOT EXISTS promoLink(
                PROMO_STORE_ID STRING,
                PROMO_COUPON_NO LONG,
                PROMO_ITM_ID LONG,
                PROMO_STATUS STRING,
                INSERT_ID STRING,
                INSERT_TIMESTAMP TIMESTAMP,
                LAST_UPDATE_ID STRING,
                LAST_UPDATE_TIMESTAMP TIMESTAMP)
              USING delta 
              PARTITIONED BY (PROMO_STORE_ID)
              LOCATION '{}' """.format(promoLinkingDeltaPath))
except Exception as ex:
  ABC(DeltaTableCreateCheck = 0)
  loggerAtt.error(ex)
  err = ErrorReturn('Error', ex,'bottleDepositDeltaPath deltaCreator')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Declarations

# COMMAND ----------

# linkNumCheckList = ['PRO_LINK_HDR_FILE_NUM', 'PRO_LINK_HDR_ACTION', 'PRO_LINK_HDR_PART_OFFSET', 'PRO_LINK_HDR_PART_LENGTH', 'PRO_LINK_HDR_BIT_FLD', 'PRO_LINK_HDR_PEND_TIME', 'PRO_LINK_HDR_VERSION', 'PRO_LINK_HDR_BATCH_NUM', 'PRO_LINK_HDR_STATUS', 'PRO_LINK_MEM_PRO_ID', 'PRO_LINK_LINK_PRO_TYPE', 'PRO_LINK_GRP_PRO_ID']

itemMasterList = ["RTX_STORE", "SCRTX_DET_BLK_GRP", "SCRTX_DET_CENTRAL_ITEM", "SCRTX_DET_COMP_PRC", "SCRTX_DET_COMP_QTY", "SCRTX_DET_COMP_TYPE", "SCRTX_DET_DEA_GRP", "SCRTX_DET_DSPL_DESCR", "SCRTX_DET_FAMILY_CD", "SCRTX_DET_FOOD_STAMP_FG", "SCRTX_DET_FREQ_SHOP_TYPE", "SCRTX_DET_FREQ_SHOP_VAL", "SCRTX_DET_INTRNL_ID", "SCRTX_DET_ITM_ID", "SCRTX_DET_LNK_NBR", "SCRTX_DET_MAN_PRC_FG", "SCRTX_DET_MIX_MATCH_CD", "SCRTX_DET_NG_ENTRY_FG", "SCRTX_DET_NON_MDSE_ID", "SCRTX_DET_NON_RX_HEALTH_FG", "SCRTX_DET_OP_CODE", "SCRTX_DET_PLU_BTCH_NBR", "SCRTX_DET_QTY_RQRD_FG", "SCRTX_DET_RCPT_DESCR", "SCRTX_DET_RSTRCSALE_BRCD_FG", "SCRTX_DET_RTL_PRC", "SCRTX_DET_RX_FG", "SCRTX_DET_SEC_FAMILY", "SCRTX_DET_SLS_RESTRICT_GRP", "SCRTX_DET_STR_CPN_FG", "SCRTX_DET_STR_HIER_ID", "SCRTX_DET_SUBDEP_ID", "SCRTX_DET_TAR_WGT_NBR", "SCRTX_DET_TAX_RATE1_FG", "SCRTX_DET_TAX_RATE2_FG", "SCRTX_DET_TAX_RATE3_FG", "SCRTX_DET_TAX_RATE4_FG", "SCRTX_DET_TAX_RATE5_FG", "SCRTX_DET_TAX_RATE6_FG", "SCRTX_DET_TAX_RATE7_FG", "SCRTX_DET_TAX_RATE8_FG", "SCRTX_DET_UNT_QTY", "SCRTX_DET_VEN_CPN_FG", "SCRTX_DET_VND_ID", "SCRTX_DET_WGT_ITM_FG", "SCRTX_DET_WIC_CVV_FG", "SCRTX_DET_WIC_FG", "SCRTX_HDR_ACT_DATE", "COUPON_NO", "BANNER_ID","RTX_TYPE","INSERT_ID", "INSERT_TIMESTAMP", "LAST_UPDATE_ID","LAST_UPDATE_TIMESTAMP"]


itemEffRenaming = []

couponOutputFileColumn = ["BANNER_ID", "LOCATION", "STATUS", "COUPON_NO", "START_DATE", "END_DATE", "DEL_DATE", "PERF_DETL_SUB_TYPE", "LIMIT", "CHANGE_AMOUNT_PCT", "CLUB_CARD", "MIN_QUANTITY", "BUY_QUANTITY", "GET_QUANTITY", "SALE_QUANTITY", "DESCRIPTION", "SELL_BY_WEIGHT_IND", "INSERT_ID", "INSERT_TIMESTAMP", "LAST_UPDATE_ID", "LAST_UPDATE_TIMESTAMP"]

file_location = str(file_location)
file_type = "csv"
infer_schema = "false"
first_row_is_header = "true"
delimiterHeader = "|"
delimiterDetail = "^"
pipelineid= str(pipelineid)
folderDate = Date

processing_file='Delta'
if file_location.find('POSemergency') !=-1:
  processing_file ='COD'
else:
  processing_file='Delta'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## User Defined Functions

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Link and Promo field splitting

# COMMAND ----------

def defineHdrLinkDetail(hdrLinkDetailDf): 
  hdrLinkDetailDf = hdrLinkDetailDf.withColumn('PRO_LINK_HDR_FILE_NUM', col('Data').substr(1,3).cast(IntegerType()))
  hdrLinkDetailDf = hdrLinkDetailDf.withColumn('PRO_LINK_HDR_ACTION', col('Data').substr(4,3).cast(IntegerType()))
  hdrLinkDetailDf = hdrLinkDetailDf.withColumn('PRO_LINK_HDR_PART_OFFSET', col('Data').substr(7,6).cast(IntegerType()))
  hdrLinkDetailDf = hdrLinkDetailDf.withColumn('PRO_LINK_HDR_PART_LENGTH', col('Data').substr(13,6).cast(IntegerType()))
  hdrLinkDetailDf = hdrLinkDetailDf.withColumn('PRO_LINK_HDR_BIT_FLD', col('Data').substr(19,16).cast(IntegerType()))
  hdrLinkDetailDf = hdrLinkDetailDf.withColumn('PRO_LINK_HDR_PEND_DATE', col('Data').substr(35,6))
  hdrLinkDetailDf = hdrLinkDetailDf.withColumn('PRO_LINK_HDR_PEND_TIME', col('Data').substr(41,6))
  hdrLinkDetailDf = hdrLinkDetailDf.withColumn('PRO_LINK_HDR_VERSION', col('Data').substr(47,2).cast(IntegerType()))
  hdrLinkDetailDf = hdrLinkDetailDf.withColumn('PRO_LINK_HDR_FILLER', col('Data').substr(49,9))
  hdrLinkDetailDf = hdrLinkDetailDf.withColumn('PRO_LINK_HDR_BATCH_NUM', col('Data').substr(58,6).cast(IntegerType()))
  hdrLinkDetailDf = hdrLinkDetailDf.withColumn('PRO_LINK_HDR_STATUS', col('Data').substr(64,1).cast(IntegerType()))
  hdrLinkDetailDf = hdrLinkDetailDf.withColumn('PRO_LINK_MEM_PRO_ID', col('Data').substr(65,9).cast(LongType()))
  hdrLinkDetailDf = hdrLinkDetailDf.withColumn('PRO_LINK_LINK_PRO_TYPE', col('Data').substr(74,2).cast(IntegerType()))
  hdrLinkDetailDf = hdrLinkDetailDf.withColumn('PRO_LINK_LINK_ITEM_ID', col('Data').substr(76,13).cast(LongType()))
  hdrLinkDetailDf = hdrLinkDetailDf.withColumn('PRO_LINK_GRP_PRO_ID', col('Data').substr(89,2).cast(IntegerType()))
  return hdrLinkDetailDf

def defineHdrPromoDetail(hdrPromoDetailDf): 
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_HDR_FILE_NUM', col('Data').substr(1,3).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_HDR_ACTION', col('Data').substr(4,3).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_HDR_PART_OFFSET', col('Data').substr(7,6).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_HDR_PART_LENGTH', col('Data').substr(13,6).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_HDR_BIT_FLD', col('Data').substr(19,16).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_HDR_PEND_DATE', col('Data').substr(35,6))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_HDR_PEND_TIME', col('Data').substr(41,6))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_HDR_VERSION', col('Data').substr(47,2).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_HDR_FILLER', col('Data').substr(49,9))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_HDR_BATCH_NUM', col('Data').substr(58,6).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_HDR_STATUS', col('Data').substr(64,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_NUMBER', col('Data').substr(65,9).cast(LongType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_TYPE', col('Data').substr(74,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_END_DATE', col('Data').substr(75,6))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_DESCRIPTION', col('Data').substr(81,20))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_REWARD_TYPE', col('Data').substr(101,2).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_DEPARTMENT', col('Data').substr(103,3).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_MEM_CARD_SCHEME', col('Data').substr(106,3).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_REWARD_VALUE', col('Data').substr(109,9).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_START_DATE', col('Data').substr(118,6))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_MEM_CARD_REQUIRED', col('Data').substr(124,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_ALL_CARD_SCHEMES', col('Data').substr(125,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_FILLER_1', col('Data').substr(126,6))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_CARD_SCHEME', col('Data').substr(132,30).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_LIMITED_QTY', col('Data').substr(162,8).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_FILLER_2', col('Data').substr(170,76))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_ENHANCED_GROUP_TYPE', col('Data').substr(246,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_ENHANCED_THRESHOLD_QTY', col('Data').substr(247,6).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_ENHANCED_STEP_COUNT_QTY', col('Data').substr(253,6).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_FILLER_3_1', col('Data').substr(259,126))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_FILLER_3', col('Data').substr(385,116))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_START_TIME', col('Data').substr(401,4).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_END_TIME', col('Data').substr(405,4).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_ACTIVATION_DAY_1', col('Data').substr(409,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_ACTIVATION_DAY_2', col('Data').substr(410,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_ACTIVATION_DAY_3', col('Data').substr(411,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_ACTIVATION_DAY_4', col('Data').substr(412,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_ACTIVATION_DAY_5', col('Data').substr(413,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_ACTIVATION_DAY_6', col('Data').substr(414,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_ACTIVATION_DAY_7', col('Data').substr(415,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_ACTIVATION_TIME_1', col('Data').substr(416,8).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_ACTIVATION_TIME_2', col('Data').substr(424,8).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_ACTIVATION_TIME_3', col('Data').substr(432,8).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_ACTIVATION_TIME_4', col('Data').substr(440,8).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_ACTIVATION_TIME_5', col('Data').substr(448,8).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_ACTIVATION_TIME_6', col('Data').substr(456,8).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_ACTIVATION_TIME_7', col('Data').substr(464,8).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_TRIGGER_FLAGS_2', col('Data').substr(472,10).cast(IntegerType())) 
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_LOW_HIGH_REWARD', col('Data').substr(482,1).cast(IntegerType())) 
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_MIN_ITEM_VALUE', col('Data').substr(483,6).cast(IntegerType())) 
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_MIN_ITEM_WEIGHT', col('Data').substr(489,6).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_MIN_PURCHASE', col('Data').substr(495,6).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_DELAYED_PROMO', col('Data').substr(501,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_CASHIER_ENTERED', col('Data').substr(502,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_REQ_COUPON_CODE', col('Data').substr(503,13).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_LINKING_PROMO', col('Data').substr(516,9).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_MAX_ITEM_WEIGHT', col('Data').substr(525,6).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_SEGMENTS_1', col('Data').substr(531,1).cast(IntegerType()))  
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_SEGMENTS_2', col('Data').substr(532,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_SEGMENTS_3', col('Data').substr(533,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_SEGMENTS_4', col('Data').substr(534,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_SEGMENTS_5', col('Data').substr(535,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_SEGMENTS_6', col('Data').substr(536,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_SEGMENTS_7', col('Data').substr(537,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_SEGMENTS_8', col('Data').substr(538,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_SEGMENTS_9', col('Data').substr(539,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_SEGMENTS_10', col('Data').substr(540,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_SEGMENTS_11', col('Data').substr(541,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_SEGMENTS_12', col('Data').substr(542,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_SEGMENTS_13', col('Data').substr(543,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_SEGMENTS_14', col('Data').substr(544,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_SEGMENTS_15', col('Data').substr(545,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_SEGMENTS_16', col('Data').substr(546,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_UPD_LOYALTY_SER', col('Data').substr(547,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_CPN_REQ_TYPE', col('Data').substr(548,1).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_CREDIT_PROGRAM_ID', col('Data').substr(549,8).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_PROMO_EXTERNAL_ID', col('Data').substr(557,8).cast(IntegerType()))
  hdrPromoDetailDf = hdrPromoDetailDf.withColumn('PROMO_DEPARTMENT_4DIG', col('Data').substr(565,4).cast(IntegerType()))
  return hdrPromoDetailDf

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Processing functions

# COMMAND ----------

def itemEff_promotable(s):
    return itemEffRenaming[s]
def itemEff_change_col_name(s):
    return s in itemEffRenaming
  
def itemMain_promotable(s):
    return itemMasterRenaming[s]
def itemMain_change_col_name(s):
    return s in itemMasterRenaming

def link_table(s):
    return linkRenaming[s]
def link_change_col_name(s):
    return s in linkRenaming  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Fetching start and end row number for Headers

# COMMAND ----------

def fetchHeaderLinkPromo(posRawDf):
  ## if the column name changed, change the select statement in the below code
  ABC(fetchHeaderLinkPromoCheck=1)
  rawDataColumn = "Data"
  rowNumberColumn = "RowNumber"
  endRowNumberColumn = "endRowNumber"
  storeColumn = "Store_ID"
  storeHeaderExpr = "^.&EMBCNTRL"
  allHdrExpr = "^/HDR"
  hdrExpr = "^/HDR$"
  hdrPromoExpr = "^/HDR PROMO"
  hdrLinkExpr = "^/HDR LINK"
  maxCount = posRawDf.count()
  
  ## Step1: Fetching all header records from raw dataframe
  allHeaderDf = posRawDf.filter(posRawDf[rawDataColumn].rlike(allHdrExpr) | posRawDf[rawDataColumn].rlike(storeHeaderExpr))
  
  ## Step2: Finding End Row Number
  part_by_regex = Window().partitionBy().orderBy(col(rowNumberColumn))
  allHeaderDf = allHeaderDf.select("*", lead(col(rowNumberColumn) - 1,1,maxCount).over(part_by_regex).alias(endRowNumberColumn))
  ABC(allHeaderCount=allHeaderDf.count())
  loggerAtt.info("allHeader record count before join is " + str(allHeaderDf.count()))
  
  ## Step3: Fetching store header records from all Header
  storeDf = allHeaderDf.filter(posRawDf[rawDataColumn].rlike(storeHeaderExpr))
  storeDf = storeDf.drop(endRowNumberColumn)
  ABC(storeDfCount=storeDf.count())
  loggerAtt.info("storeDf record count before banner id fetch is " + str(storeDf.count()))
  
  ## Step4: Finding end row number records for store headers
  part_by_regex = Window().partitionBy().orderBy(col(rowNumberColumn))
  storeDf = storeDf.select("*", lead(col(rowNumberColumn) - 1,1,maxCount).over(part_by_regex).alias(endRowNumberColumn))
  storeDf = storeDf.withColumn(storeColumn, col(rawDataColumn).substr(55,4))
  storeDf = storeDf.selectExpr("RowNumber as storeRowNumber","Data as storeData","endRowNumber as storeEndRowNumber","Store_ID")
  
  ## Step5: Fetch Banner ID
  # Write query to Store detail to fetch Banner id
  storeDf = fetchBannerId(storeDf)
  loggerAtt.info("storeDf record count after banner id fetch is " + str(storeDf.count()))
#   storeDf = storeDf.withColumn("BANNER",lit("HAN"))
  
  ## Step6: Creating all header df with proper start and end rownumbers for hdr header with store value
  allHeaderDf = allHeaderDf.join(storeDf, [allHeaderDf.RowNumber > storeDf.storeRowNumber, allHeaderDf.RowNumber <= storeDf.storeEndRowNumber], how='inner').select([col(xx) for xx in allHeaderDf.columns]+['Store_ID',"BANNER_ID"])
  loggerAtt.info("allHeader record count after join is " + str(allHeaderDf.count()))
  
  ## Step7: Creating seperate dataframe for each header: HDR, HDR LINK and HDR PROMO
  promoHeaderDf = allHeaderDf.filter(posRawDf[rawDataColumn].rlike(hdrPromoExpr))
  promoHeaderDf = promoHeaderDf.withColumn(rowNumberColumn, col(rowNumberColumn)+2)
  promoHeaderDf = promoHeaderDf.selectExpr("RowNumber as promoRowNumber","Data as promoData","endRowNumber as promoEndRowNumber","Store_ID", "BANNER_ID")
  ABC(promoHeaderDfCount=promoHeaderDf.count())
  loggerAtt.info("promoHeaderDf record count is " + str(promoHeaderDf.count()))
  
  linkHeaderDf = allHeaderDf.filter(posRawDf[rawDataColumn].rlike(hdrLinkExpr))
  linkHeaderDf = linkHeaderDf.withColumn(rowNumberColumn, col(rowNumberColumn)+2)
  linkHeaderDf = linkHeaderDf.selectExpr("RowNumber as linkRowNumber","Data as linkData","endRowNumber as linkEndRowNumber","Store_ID", "BANNER_ID")
  ABC(linkHeaderDfCount=linkHeaderDf.count())
  loggerAtt.info("linkHeaderDf record count is " + str(linkHeaderDf.count()))
  
  ## Step8: Fetching Link record details
  linkDf = posRawDf.join(linkHeaderDf, [posRawDf.RowNumber >= linkHeaderDf.linkRowNumber, posRawDf.RowNumber <= linkHeaderDf.linkEndRowNumber], how='inner').select([col(xx) for xx in posRawDf.columns]+["Store_ID", "linkRowNumber", "linkEndRowNumber", "BANNER_ID"])
  ABC(linkDfCount=linkDf.count())
  loggerAtt.info("linkDf record count is " + str(linkDf.count()))
  
  ## Step9: Fetching promo record details
  promoDf = posRawDf.join(promoHeaderDf, [posRawDf.RowNumber >= promoHeaderDf.promoRowNumber, posRawDf.RowNumber <= promoHeaderDf.promoEndRowNumber], how='inner').select([col(xx) for xx in posRawDf.columns]+["Store_ID","promoRowNumber","promoEndRowNumber", "BANNER_ID"])
  ABC(promoDfCount=promoDf.count())
  loggerAtt.info("promoDf record count is " + str(promoDf.count()))
  
  return promoDf, linkDf, storeDf

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Fetching Link, Promo, HDR Detail records

# COMMAND ----------

def fetchLinkRecords(linkDf):
  rawDataColumn = "Data"
  rowNumberColumn = "RowNumber"
  PRO_LINK_HDR_FILLER = "PRO_LINK_HDR_FILLER"
  itemUPC = "PRO_LINK_LINK_ITEM_ID"
  storeColumn = "Store_ID"
  bannerId = "BANNER_ID"
  
  ## Step1: Splitting data based on link sub string record split
  linkDf = defineHdrLinkDetail(linkDf.selectExpr(rawDataColumn,storeColumn,bannerId)).drop(rawDataColumn).drop(PRO_LINK_HDR_FILLER)
  
  ## Step2: Casting column to its relative datatype
  linkDf = linkDf.withColumn(itemUPC, col(itemUPC).cast(LongType()))
  
  ## Step3: Fetching Null records 
  linkDetailTransformedNullDf = linkDf.where(reduce(lambda x, y: x | y, (col(x).isNull() for x in linkDf.columns)))
  loggerAtt.info("Dimension of the Null Link records:("+str(linkDetailTransformedNullDf.count())+"," +str(len(linkDetailTransformedNullDf.columns))+")")
  
  ## Step4: Removing Null Records from original Dataframe
  linkDf = linkDf.na.drop()
  loggerAtt.info("Dimension of the Not null Link records:("+str(linkDf.count())+"," +str(len(linkDf.columns))+")")
  
#   ## Step5: Writing Link null records to invalid dataframe
  ABC(NullValueLinkCount=linkDetailTransformedNullDf.count())
  ABC(LinkCount = linkDf.count())
  
  linkDetailTransformedNullDf.write.mode('Append').format('parquet').save(invalidLinkRecordsPath + "/" +Date+ "/" + "Invalid_Data")
  
  return linkDf


def fetchPromoRecords(promoDf):
  rawDataColumn = "Data"
  rowNumberColumn = "RowNumber"
  promoTriggerFlag = "PROMO_TRIGGER_FLAGS_2"
  promoSegments = "PROMO_SEGMENTS"
  storeColumn = "Store_ID"
  bannerId = "BANNER_ID"
  PROMO_HDR_FILLER = "PROMO_HDR_FILLER"
  PROMO_FILLER_1 = "PROMO_FILLER_1"
  PROMO_FILLER_2 = "PROMO_FILLER_2"
  PROMO_FILLER_3_1 = "PROMO_FILLER_3_1"
  PROMO_FILLER_3 = "PROMO_FILLER_3"
  
  ## Step1: Splitting data based on link sub string record split
  promoDf = defineHdrPromoDetail(promoDf.selectExpr(rawDataColumn,storeColumn,bannerId)).drop(rawDataColumn).drop(PROMO_HDR_FILLER)
  
  promoDf = promoDf.select([c for c in promoDf.columns if c not in {rawDataColumn,PROMO_HDR_FILLER,PROMO_FILLER_1,PROMO_FILLER_2,PROMO_FILLER_3_1,PROMO_FILLER_3}])
  
  ## Step2: Casting column to its relative datatype
  
  promoDf = promoDf.withColumn(promoTriggerFlag, col(promoTriggerFlag).cast(LongType()))
  
  ## Step3: Fetching Null records
  promoDetailTransformedNullDf = promoDf.where(reduce(lambda x, y: x | y, (col(x).isNull() for x in promoDf.columns)))
  loggerAtt.info("Dimension of the Null Promo records:("+str(promoDetailTransformedNullDf.count())+"," +str(len(promoDetailTransformedNullDf.columns))+")")
  
  ## Step4: Removing Null Records from original Dataframe
  promoDf = promoDf.na.drop()
  loggerAtt.info("Dimension of the Not null Promo records:("+str(promoDf.count())+"," +str(len(promoDf.columns))+")")
  
#   ## Step5: Writing Promo null records to invalid dataframe
#   # 
  ABC(NullValuePromoCount=promoDetailTransformedNullDf.count())
  ABC(PromoCount = promoDf.count())
  
  promoDetailTransformedNullDf.write.mode('Append').format('parquet').save(invalidPromoRecordsPath + "/" +Date+ "/" + "Invalid_Data")
  
  return promoDf

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Reading file

# COMMAND ----------

def readFileHeader(file_location, infer_schema, first_row_is_header, delimiter,file_type):
  inputDataDF = spark.read.format("csv") \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .schema(inputDataSchema) \
    .load(file_location)
  ABC(ReadDataCheck=1)
  RawDataCount = inputDataDF.count()
  ABC(RawDataCount=RawDataCount)
  loggerAtt.info("Raw count check initiated for readFileHeader")
  loggerAtt.info(f"Count of Records in the File: {RawDataCount}")
  return inputDataDF

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### UDF

# COMMAND ----------

date_func =  udf (lambda x: datetime.datetime.strptime(str(x), '%Y%m%d'), DateType())

def convertDate(s):
  year = "0000"
  month = "00"
  date = "00"
  try:
    if s[0:1] == "A":
      year = 2000
    elif s[0:1] == "B":
      year = 2010
    elif s[0:1] == "C":
      year = 2020
    elif s[0:1] == "D":
      year = 2030
    elif s[0:1] == "E":
      year = 2040   
    year = int(year) + int(s[1:2])
    month = s[2:4]
    date = s[4:6]
  except Exception as ex:
    return "00000000"
  
  return str(year)+month+date

convertDateUDF = udf(convertDate)

def rewardAmt(s):
  if s == 8 or s == 17:
    return True
  else:
    return False

rewardAmtUDF = udf(rewardAmt)  
  
def statusChange(s):
  if s == 8:
    return 'D'
  elif s == 6:
    return 'C'
  else:
    return None

statusChangeUDF = udf(statusChange)  

def bottleDepositNumber(DET_LNK_NBR, FOOD_STAMP_FG):
  if FOOD_STAMP_FG > 0 and DET_LNK_NBR > 0:
    return 40000 + DET_LNK_NBR
  elif DET_LNK_NBR > 0:
    return 41000 + DET_LNK_NBR
  else:
    return 0
  
bottleDepositNumberUDF = udf(bottleDepositNumber)

def chgAmtPct(s, amt, val, pct):
  if s == 8 or s == 17:
    return amt
  elif s == 15:
    return val
  elif s == 2:
    return pct
  else:
    return 0

chgAmtPctUDF = udf(chgAmtPct)

def promoRenamingflat_promotable(s):
    return promoRenaming[s]
def change_col_name(s):
    return s in promoRenaming


def itemStatus(s):
  if s == 1 or s == 6:
    return 'C'
  elif s == 2:
    return 'M'
  elif s == 4:
    return 'D'
  else:
    return null
  
itemStatusUDF = udf(itemStatus) 

def flagChange(s):
  if s == 1:
    return 'Y'
  elif s == 0:
    return 'N'
  else:
    return None

flagChangeUDF = udf(flagChange)   

def fetchFirst(s):
  if len(s) != 0:
    return s[0]
  else:
    return None

spark.udf.register("fetchFirstFunction", fetchFirst)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Promo Transformation

# COMMAND ----------

def promoLinkTransformation(promoDf, linkDf):
  ABC(TransformationCheck=1)
  
  linkDf = linkDf.withColumn("INSERT_ID",lit(pipelineid))
  linkDf = linkDf.withColumn("INSERT_TIMESTAMP",current_timestamp())
  linkDf = linkDf.withColumn("LAST_UPDATE_ID",lit(pipelineid))
  linkDf = linkDf.withColumn("LAST_UPDATE_TIMESTAMP",current_timestamp())
  linkDf = linkDf.withColumn("PRO_LINK_LINK_ITEM_ID",col("PRO_LINK_LINK_ITEM_ID")*10)
  
  invalidDf = None
  
  promoDf = promoDf.withColumn("PROMO_END_DATE", convertDateUDF(col("PROMO_END_DATE")))
  
  promoDf = promoDf.withColumn("PROMO_START_DATE", convertDateUDF(col("PROMO_START_DATE")))
  
  invalidDf = promoDf.filter((col("PROMO_START_DATE") == "00000000") | (col("PROMO_END_DATE") == "00000000"))
  
  promoDf = promoDf.withColumn("PROMO_REWARD_VALUE_PER", when((col("PROMO_REWARD_TYPE") == 2), col("PROMO_REWARD_VALUE")/100).otherwise(lit(0)).cast(DecimalType(6,3)))
  
  promoDf = promoDf.withColumn("PROMO_REWARD_VALUE_AMT", when(((col("PROMO_REWARD_TYPE") == 17) | (col("PROMO_REWARD_TYPE") == 8)), col("PROMO_REWARD_VALUE")/1000).otherwise(lit(0)).cast(DecimalType(7,2)))
  
  promoDf = promoDf.filter((col("PROMO_START_DATE") != "00000000") & (col("PROMO_END_DATE") != "00000000"))
  
  promoDf = promoDf.withColumn("PROMO_START_DATE", to_date(date_format(date_func(col('PROMO_START_DATE')), 'yyyy-MM-dd')))
  
  promoDf = promoDf.withColumn("PROMO_END_DATE", to_date(date_format(date_func(col('PROMO_END_DATE')), 'yyyy-MM-dd')))
  
  promoDf = promoDf.withColumn("PROMO_DELETE_DATE", lit(None).cast(DateType()))
  
  promoDf = promoDf.withColumn("STATUS", statusChangeUDF(col('PROMO_HDR_ACTION')))
  
  promoDf = promoDf.withColumn("PROMO_ENHANCED_THRESHOLD_QTY", when((col("PROMO_ENHANCED_THRESHOLD_QTY") > 1), col("PROMO_ENHANCED_THRESHOLD_QTY")).otherwise(lit(1)))
  
  promoDf = promoDf.withColumn("PROMO_ENHANCED_STEP_COUNT_QTY", when((col("PROMO_ENHANCED_STEP_COUNT_QTY") > 1), 0).otherwise(lit(1)))
  
  promoDf = promoDf.withColumn("PROMO_MIN_ITEM_VALUE", (col("PROMO_MIN_ITEM_VALUE")/100).cast(DecimalType(4,2)))
  
  promoDf = promoDf.withColumn("PROMO_MIN_ITEM_WEIGHT", (col("PROMO_MIN_ITEM_WEIGHT")/1000).cast(DecimalType(3,3)))
  
  promoDf = promoDf.withColumn("PROMO_MIN_PURCHASE", (col("PROMO_MIN_PURCHASE")/100).cast(DecimalType(4,2)))
  
  promoDf = promoDf.withColumn("PROMO_MAX_ITEM_WEIGHT", (col("PROMO_MAX_ITEM_WEIGHT")/1000).cast(DecimalType(3,3)))
  
  promoDf = promoDf.withColumn("SALE_QUANTITY", when((col("PROMO_REWARD_TYPE") == 17), col("PROMO_ENHANCED_THRESHOLD_QTY")).otherwise(lit(1)))
  
  promoDf = promoDf.withColumn("MIN_QUANTITY", when(((col("PROMO_REWARD_TYPE") == 15) | (col("PROMO_REWARD_TYPE") == 8)), col("PROMO_ENHANCED_THRESHOLD_QTY")).otherwise(lit(1)))
  
  promoDf = promoDf.withColumn("BUY_QUANTITY", when(((col("PROMO_REWARD_TYPE") == 15) | (col("PROMO_REWARD_TYPE") == 8)), col("PROMO_ENHANCED_THRESHOLD_QTY")).otherwise(lit(1)))
  
  promoDf = promoDf.withColumn("GET_QUANTITY", when((col("PROMO_REWARD_TYPE") == 15), col("PROMO_REWARD_VALUE")).otherwise(lit(0)))
  
  promoDf = promoDf.withColumn("SELL_BY_WEIGHT_IND", lit(None))
  
  promoDf = promoDf.withColumn("DESCRIPTION",lit(None))
  
  promoDf = promoDf.withColumn("DEL_DATE",lit(None))
  
  promoDf = promoDf.withColumn("PROMO_NUMBER", col("PROMO_NUMBER").cast(LongType()))
  
  promoDf = promoDf.withColumn("CLUB_CARD",lit('N'))
  
  promoDf = promoDf.withColumn("AHO_PERF_DETAIL_ID",lit(None))
  
  promoDf = promoDf.withColumn("CHANGE_AMOUNT_PCT", chgAmtPctUDF(col("PROMO_REWARD_TYPE"), col("PROMO_REWARD_VALUE_AMT"), col("PROMO_REWARD_VALUE"), col("PROMO_REWARD_VALUE_PER")))
  
  promoDf = promoDf.withColumn("LIMIT", when((col("PROMO_LIMITED_QTY") == 0), lit(999)).otherwise(col("PROMO_ENHANCED_THRESHOLD_QTY")))
  
  promoDf = promoDf.withColumn("PROMO_REWARD_TYPE", when((col("PROMO_REWARD_TYPE") == 2), lit(4)).otherwise(col("PROMO_REWARD_TYPE")))
  promoDf = promoDf.withColumn("PROMO_REWARD_TYPE", when((col("PROMO_REWARD_TYPE") == 15), lit(9)).otherwise(col("PROMO_REWARD_TYPE")))
  
  promoDf = promoDf.withColumn("INSERT_ID",lit(pipelineid))
  promoDf = promoDf.withColumn("INSERT_TIMESTAMP",current_timestamp())
  promoDf = promoDf.withColumn("LAST_UPDATE_ID",lit(pipelineid))
  promoDf = promoDf.withColumn("LAST_UPDATE_TIMESTAMP",current_timestamp())
  
  promoDf = quinn.with_some_columns_renamed(promoRenamingflat_promotable, change_col_name)(promoDf)
  
  if invalidDf is not None:
    if invalidDf.count() > 0:
      ABC(InvalidRecordSaveCheck = 1)
      loggerAtt.info(f"Count of Invalid Records in Transformation: {invalidDf.count()}")
      ABC(invalidTransCount=invalidDf.count())
      invalidDf.write.mode('Append').format('parquet').save(invalidPromoRecordsPath + "/" +Date+ "/" + "Invalid_Data")
  
  loggerAtt.info(f"Count of Records in the Promo Df: {promoDf.count()}")
  return promoDf, linkDf


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Temp Item Main Eff Transformation

# COMMAND ----------

def itemMasterTransformation(itemMasterDf):
  itemMasterDf = itemMasterDf.withColumn("SMA_ITM_EFF_DATE", date_format(col("SMA_ITM_EFF_DATE"), 'yyyy/MM/dd').cast(StringType()))
  itemMasterDf = itemMasterDf.withColumn("SMA_BATCH_SERIAL_NBR", col("SCRTX_DET_PLU_BTCH_NBR")) 
  itemMasterDf = itemMasterDf.withColumn("SMA_RESTR_CODE", col("SCRTX_DET_SLS_RESTRICT_GRP")) 
  itemMasterDf = itemMasterDf.withColumn("SMA_STATUS_DATE", col("SMA_ITM_EFF_DATE"))
  itemMasterDf = itemMasterDf.withColumn("SMA_SMR_EFF_DATE", col("SMA_ITM_EFF_DATE"))
  itemMasterDf = itemMasterDf.withColumn("SMA_SBW_IND", flagChangeUDF(col('SMA_SBW_IND')))
  itemMasterDf = itemMasterDf.withColumn("SMA_TAX_1", flagChangeUDF(col('SMA_TAX_1')))
  itemMasterDf = itemMasterDf.withColumn("SMA_TAX_2", flagChangeUDF(col('SMA_TAX_2')))
  itemMasterDf = itemMasterDf.withColumn("SMA_TAX_3", flagChangeUDF(col('SMA_TAX_3')))
  itemMasterDf = itemMasterDf.withColumn("SMA_TAX_4", flagChangeUDF(col('SMA_TAX_4')))
  itemMasterDf = itemMasterDf.withColumn("SMA_TAX_5", flagChangeUDF(col('SMA_TAX_5')))
  itemMasterDf = itemMasterDf.withColumn("SMA_TAX_6", flagChangeUDF(col('SMA_TAX_6')))
  itemMasterDf = itemMasterDf.withColumn("SMA_TAX_7", flagChangeUDF(col('SMA_TAX_7')))
  itemMasterDf = itemMasterDf.withColumn("SMA_TAX_8", flagChangeUDF(col('SMA_TAX_8')))
  itemMasterDf = itemMasterDf.withColumn("SMA_ITEM_STATUS", itemStatusUDF(col('SCRTX_DET_OP_CODE')))
  itemMasterDf = itemMasterDf.withColumn("SMA_SUB_DEPT", col("SMA_SUB_DEPT").cast(StringType()))
  itemMasterDf = itemMasterDf.withColumn("SMA_STORE", col("SMA_DEST_STORE"))
  itemMasterDf = itemMasterDf.withColumn("SMA_VEND_NUM", col("SMA_RETL_VENDOR"))
  itemMasterDf = itemMasterDf.withColumn("SMA_UPC_DESC", col("SMA_ITEM_DESC"))
  itemMasterDf = itemMasterDf.withColumn('SMA_VEND_COUPON_FAM1', lpad(col('SMA_VEND_COUPON_FAM1'),3,'0'))
  itemMasterDf = itemMasterDf.withColumn('SMA_VEND_COUPON_FAM2', lpad(col('SMA_VEND_COUPON_FAM2'),4,'0'))
  itemMasterDf = itemMasterDf.withColumn("SMA_FIXED_TARE_WGT", lpad(col('SMA_FIXED_TARE_WGT'),5,'0'))
#   itemMasterDf = itemMasterDf.withColumn('SMA_GTIN_NUM', lpad(col('SMA_GTIN_NUM'),14,'0'))
  itemMasterDf = itemMasterDf.withColumn("TEMP_SCRTX_DET_LNK_NBR", bottleDepositNumberUDF(col("SCRTX_DET_LNK_NBR"), col("SMA_FOOD_STAMP_IND")))
  itemMasterDf = itemMasterDf.withColumn("SMA_FOOD_STAMP_IND", flagChangeUDF(col("SMA_FOOD_STAMP_IND")))
  itemMasterDf = itemMasterDf.withColumn("SMA_WIC_IND", when(((col("SCRTX_DET_WIC_FG") == 1) | (col("SCRTX_DET_WIC_CVV_FG") == 1)),lit("Y")).otherwise(lit("N")))
  itemMasterDf = itemMasterDf.withColumn('SMA_LINK_UPC', lpad(col('SMA_GTIN_NUM'),14,'0'))
  itemMasterDf = itemMasterDf.withColumn("SMA_LINK_HDR_COUPON", lpad(col('SMA_LINK_HDR_COUPON'),14,'0'))
  itemMasterDf = itemMasterDf.withColumn("ALTERNATE_UPC", lit(None).cast(LongType()))
  itemMasterDf = itemMasterDf.withColumn("SMA_BOTTLE_DEPOSIT_IND", lit(None).cast(StringType()))
  itemMasterDf = itemMasterDf.withColumn("SMA_SELL_RETL", lpad(regexp_replace(format_number((col("SMA_MULT_UNIT_RETL").cast(DecimalType(10,2))/col("SMA_RETL_MULT_UNIT")), 2), ",", ""),10,'0'))
#   itemMasterDf = itemMasterDf.withColumn("SMA_SELL_RETL", lit(None).cast(StringType())) # needs change
#   itemMasterDf = itemMasterDf.withColumn("SMA_MULT_UNIT_RETL", lit(None).cast(StringType())) # needs change
  itemMasterDf = itemMasterDf.withColumn("ALT_UPC_FETCH", lit(None).cast(LongType()))
  itemMasterDf = itemMasterDf.withColumn("SMA_RESTR_CODE", lpad(col('SMA_RESTR_CODE'),2,'0'))
  itemMasterDf = itemMasterDf.withColumn("SMA_RETL_MULT_UNIT", lpad(col('SMA_RETL_MULT_UNIT'),10,'0'))
  return itemMasterDf

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Item Main Transformation

# COMMAND ----------

def hdrDataTypeChgTransformation(hdrDetailValueDf):
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_PLU_BTCH_NBR",col("SCRTX_DET_PLU_BTCH_NBR").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_OP_CODE",col("SCRTX_DET_OP_CODE").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_STR_HIER_ID",col("SCRTX_DET_STR_HIER_ID").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_DFLT_RTN_LOC_ID",col("SCRTX_DET_DFLT_RTN_LOC_ID").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_MSG_CD",col("SCRTX_DET_MSG_CD").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_SLS_RESTRICT_GRP",col("SCRTX_DET_SLS_RESTRICT_GRP").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_TAXABILITY_CD",col("SCRTX_DET_TAXABILITY_CD").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_MDSE_XREF_ID",col("SCRTX_DET_MDSE_XREF_ID").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_NON_MDSE_ID",col("SCRTX_DET_NON_MDSE_ID").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_LIN_ITM_CD",col("SCRTX_DET_LIN_ITM_CD").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_MD_FG",col("SCRTX_DET_MD_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_QTY_RQRD_FG",col("SCRTX_DET_QTY_RQRD_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_SUBPRD_CNT",col("SCRTX_DET_SUBPRD_CNT").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_QTY_ALLOWED_FG",col("SCRTX_DET_QTY_ALLOWED_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_SLS_AUTH_FG",col("SCRTX_DET_SLS_AUTH_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_FOOD_STAMP_FG",col("SCRTX_DET_FOOD_STAMP_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_WIC_FG",col("SCRTX_DET_WIC_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_PERPET_INV_FG",col("SCRTX_DET_PERPET_INV_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_MAN_PRC_LVL",col("SCRTX_DET_MAN_PRC_LVL").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_SERIAL_MDSE_FG",col("SCRTX_DET_SERIAL_MDSE_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_NG_ENTRY_FG",col("SCRTX_DET_NG_ENTRY_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_STR_CPN_FG",col("SCRTX_DET_STR_CPN_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_VEN_CPN_FG",col("SCRTX_DET_VEN_CPN_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_MAN_PRC_FG",col("SCRTX_DET_MAN_PRC_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_WGT_ITM_FG",col("SCRTX_DET_WGT_ITM_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_NON_DISC_FG",col("SCRTX_DET_NON_DISC_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_COST_PLUS_FG",col("SCRTX_DET_COST_PLUS_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_PRC_VRFY_FG",col("SCRTX_DET_PRC_VRFY_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_PRC_OVRD_FG",col("SCRTX_DET_PRC_OVRD_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_SPLR_PROM_FG",col("SCRTX_DET_SPLR_PROM_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_SAVE_DISC_FG",col("SCRTX_DET_SAVE_DISC_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_ITM_ONSALE_FG",col("SCRTX_DET_ITM_ONSALE_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_INHBT_QTY_FG",col("SCRTX_DET_INHBT_QTY_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_DCML_QTY_FG",col("SCRTX_DET_DCML_QTY_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_SHELF_LBL_RQRD_FG",col("SCRTX_DET_SHELF_LBL_RQRD_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_TAX_RATE1_FG",col("SCRTX_DET_TAX_RATE1_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_TAX_RATE2_FG",col("SCRTX_DET_TAX_RATE2_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_TAX_RATE3_FG",col("SCRTX_DET_TAX_RATE3_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_TAX_RATE4_FG",col("SCRTX_DET_TAX_RATE4_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_TAX_RATE5_FG",col("SCRTX_DET_TAX_RATE5_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_TAX_RATE7_FG",col("SCRTX_DET_TAX_RATE7_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_UNIT_CASE",col("SCRTX_DET_UNIT_CASE").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_MIX_MATCH_CD",col("SCRTX_DET_MIX_MATCH_CD").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_RTN_CD",col("SCRTX_DET_RTN_CD").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_FAMILY_CD",col("SCRTX_DET_FAMILY_CD").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_DISC_CD",col("SCRTX_DET_DISC_CD").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_LBL_QTY",col("SCRTX_DET_LBL_QTY").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_SCALE_FG",col("SCRTX_DET_SCALE_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_LOCAL_DEL_FG",col("SCRTX_DET_LOCAL_DEL_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_HOST_DEL_FG",col("SCRTX_DET_HOST_DEL_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_WGT_SCALE_FG",col("SCRTX_DET_WGT_SCALE_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_FREQ_SHOP_TYPE",col("SCRTX_DET_FREQ_SHOP_TYPE").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_SEC_FAMILY",col("SCRTX_DET_SEC_FAMILY").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_POS_MSG",col("SCRTX_DET_POS_MSG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_SHELF_LIFE_DAY",col("SCRTX_DET_SHELF_LIFE_DAY").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_PROM_NBR",col("SCRTX_DET_PROM_NBR").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_BCKT_NBR",col("SCRTX_DET_BCKT_NBR").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_EXTND_PROM_NBR",col("SCRTX_DET_EXTND_PROM_NBR").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_EXTND_BCKT_NBR",col("SCRTX_DET_EXTND_BCKT_NBR").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_TAR_WGT_NBR",col("SCRTX_DET_TAR_WGT_NBR").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_RSTRCT_LAYOUT",col("SCRTX_DET_RSTRCT_LAYOUT").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_CMPRTV_UOM",col("SCRTX_DET_CMPRTV_UOM").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_BNS_CPN_FG",col("SCRTX_DET_BNS_CPN_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_EX_MIN_PURCH_FG",col("SCRTX_DET_EX_MIN_PURCH_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_FUEL_FG",col("SCRTX_DET_FUEL_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_SPR_AUTH_RQRD_FG",col("SCRTX_DET_SPR_AUTH_RQRD_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_SSP_PRDCT_FG",col("SCRTX_DET_SSP_PRDCT_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_NU06_FG",col("SCRTX_DET_NU06_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_NU07_FG",col("SCRTX_DET_NU07_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_NU08_FG",col("SCRTX_DET_NU08_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_NU09_FG",col("SCRTX_DET_NU09_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_NU10_FG",col("SCRTX_DET_NU10_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_FREQ_SHOP_LMT",col("SCRTX_DET_FREQ_SHOP_LMT").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_ITM_STATUS",col("SCRTX_DET_ITM_STATUS").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_DEA_GRP",col("SCRTX_DET_DEA_GRP").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_BNS_BY_OPCODE",col("SCRTX_DET_BNS_BY_OPCODE").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_COMP_TYPE",col("SCRTX_DET_COMP_TYPE").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_COMP_QTY",col("SCRTX_DET_COMP_QTY").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_ASSUME_QTY_FG",col("SCRTX_DET_ASSUME_QTY_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_EXCISE_TAX_NBR",col("SCRTX_DET_EXCISE_TAX_NBR").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_PRC_RSN_ID",col("SCRTX_DET_PRC_RSN_ID").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_ITM_POINT",col("SCRTX_DET_ITM_POINT").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_PRC_GRP_ID",col("SCRTX_DET_PRC_GRP_ID").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_SWW_CODE_FG",col("SCRTX_DET_SWW_CODE_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_SHELF_STOCK_FG",col("SCRTX_DET_SHELF_STOCK_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_PRT_PLUID_RCPT_FG",col("SCRTX_DET_PRT_PLUID_RCPT_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_BLK_GRP",col("SCRTX_DET_BLK_GRP").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_EXCHNGE_TENDER_ID",col("SCRTX_DET_EXCHNGE_TENDER_ID").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_CAR_WASH_FG",col("SCRTX_DET_CAR_WASH_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_EXMPT_FRM_PROM_FG",col("SCRTX_DET_EXMPT_FRM_PROM_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_QSR_ITM_TYP",col("SCRTX_DET_QSR_ITM_TYP").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_RSTRCSALE_BRCD_FG",col("SCRTX_DET_RSTRCSALE_BRCD_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_NON_RX_HEALTH_FG",col("SCRTX_DET_NON_RX_HEALTH_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_RX_FG",col("SCRTX_DET_RX_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_LNK_NBR",col("SCRTX_DET_LNK_NBR").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_WIC_CVV_FG",col("SCRTX_DET_WIC_CVV_FG").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_CENTRAL_ITEM",col("SCRTX_DET_CENTRAL_ITEM").cast(IntegerType()))
  hdrDetailValueDf=hdrDetailValueDf.withColumn("SCRTX_DET_RTL_PRC",col("SCRTX_DET_RTL_PRC").cast(FloatType()))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("SCRTX_DET_SUBDEP_ID",col("SCRTX_DET_SUBDEP_ID").cast(LongType()))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("SCRTX_DET_HEAD_OFFICE_DEP",col("SCRTX_DET_HEAD_OFFICE_DEP").cast(LongType()))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("SCRTX_DET_INTRNL_ID",col("SCRTX_DET_INTRNL_ID").cast(LongType()))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("SCRTX_DET_OLD_PRC",col("SCRTX_DET_OLD_PRC").cast(LongType()))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("SCRTX_DET_QDX_FREQ_SHOP_VAL",col("SCRTX_DET_QDX_FREQ_SHOP_VAL").cast(LongType()))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("SCRTX_DET_CMPR_QTY",col("SCRTX_DET_CMPR_QTY").cast(LongType()))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("SCRTX_DET_CMPR_UNT",col("SCRTX_DET_CMPR_UNT").cast(LongType()))
#   hdrDetailValueDf = hdrDetailValueDf.withColumn("SCRTX_DET_ITM_ID", (col("SCRTX_DET_ITM_ID")*10).cast(LongType()))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("COUPON_NO", col("COUPON_NO").cast(LongType()))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("RTX_BATCH", col("RTX_BATCH").cast(LongType()))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("RTX_TYPE", col("RTX_TYPE").cast(IntegerType()))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("RTX_LOAD", col("RTX_LOAD").cast(StringType()))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("RTX_UPC", col('RTX_UPC').cast(LongType()))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("INSERT_TIMESTAMP",col('INSERT_TIMESTAMP').cast(TimestampType()))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("LAST_UPDATE_TIMESTAMP",col('LAST_UPDATE_TIMESTAMP').cast(TimestampType()))
  
  return hdrDetailValueDf
  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming

# COMMAND ----------

linkRenaming = { "Store_ID":"PROMO_STORE_ID",
                  "PRO_LINK_MEM_PRO_ID":"PROMO_COUPON_NO",
                  "PRO_LINK_LINK_ITEM_ID":"PROMO_ITM_ID"}

promoRenaming = { "Store_ID":"LOCATION",
                  "PROMO_NUMBER":"COUPON_NO",
                  "PROMO_START_DATE":"START_DATE",
                  "PROMO_END_DATE":"END_DATE",
                  "PROMO_REWARD_TYPE":"PERF_DETL_SUB_TYPE"
                }
itemMasterRenaming = {  "SCRTX_HDR_ACT_DATE":"SMA_ITM_EFF_DATE",
                        "SCRTX_DET_TAR_WGT_NBR":"SMA_FIXED_TARE_WGT",
                        "SCRTX_DET_WGT_ITM_FG":"SMA_SBW_IND",
                        "SCRTX_DET_STR_HIER_ID":"SMA_SUB_DEPT",
                        "RTX_STORE":"SMA_DEST_STORE",
                        "SCRTX_DET_SUBDEP_ID":"SMA_BATCH_SUB_DEPT",
                        "SCRTX_DET_VND_ID":"SMA_RETL_VENDOR",
                        "SCRTX_DET_ITM_ID":"SMA_GTIN_NUM",
                        "SCRTX_DET_DSPL_DESCR":"SMA_ITEM_DESC",
                        "SCRTX_DET_FAMILY_CD":"SMA_VEND_COUPON_FAM1",
                        "SCRTX_DET_SEC_FAMILY":"SMA_VEND_COUPON_FAM2",
                        "SCRTX_DET_FOOD_STAMP_FG":"SMA_FOOD_STAMP_IND",
                        "SCRTX_DET_TAX_RATE1_FG":"SMA_TAX_1",
                        "SCRTX_DET_TAX_RATE2_FG":"SMA_TAX_2",
                        "SCRTX_DET_TAX_RATE3_FG":"SMA_TAX_3",
                        "SCRTX_DET_TAX_RATE4_FG":"SMA_TAX_4",
                        "SCRTX_DET_TAX_RATE5_FG":"SMA_TAX_5",
                        "SCRTX_DET_TAX_RATE6_FG":"SMA_TAX_6",
                        "SCRTX_DET_TAX_RATE7_FG":"SMA_TAX_7",
                        "SCRTX_DET_TAX_RATE8_FG":"SMA_TAX_8",
                        "SCRTX_DET_UNT_QTY":"SMA_RETL_MULT_UNIT",
                        "SCRTX_DET_RTL_PRC":"SMA_MULT_UNIT_RETL",
                        "COUPON_NO":"SMA_LINK_HDR_COUPON"
                    }

itemEffRenaming = {   "FIRST_SCRTX_DET_PLU_BTCH_NBR":"SCRTX_DET_PLU_BTCH_NBR",
                      "FIRST_SCRTX_DET_OP_CODE":"SCRTX_DET_OP_CODE",
                      "FIRST_SCRTX_DET_STR_HIER_ID":"SCRTX_DET_STR_HIER_ID",
                      "FIRST_SCRTX_DET_DFLT_RTN_LOC_ID":"SCRTX_DET_DFLT_RTN_LOC_ID",
                      "FIRST_SCRTX_DET_MSG_CD":"SCRTX_DET_MSG_CD",
                      "FIRST_SCRTX_DET_DSPL_DESCR":"SCRTX_DET_DSPL_DESCR",
                      "FIRST_SCRTX_DET_SLS_RESTRICT_GRP":"SCRTX_DET_SLS_RESTRICT_GRP",
                      "FIRST_SCRTX_DET_RCPT_DESCR":"SCRTX_DET_RCPT_DESCR",
                      "FIRST_SCRTX_DET_TAXABILITY_CD":"SCRTX_DET_TAXABILITY_CD",
                      "FIRST_SCRTX_DET_MDSE_XREF_ID":"SCRTX_DET_MDSE_XREF_ID",
                      "FIRST_SCRTX_DET_NON_MDSE_ID":"SCRTX_DET_NON_MDSE_ID",
                      "FIRST_SCRTX_DET_UOM":"SCRTX_DET_UOM",
                      "FIRST_SCRTX_DET_UNT_QTY":"SCRTX_DET_UNT_QTY",
                      "FIRST_SCRTX_DET_LIN_ITM_CD":"SCRTX_DET_LIN_ITM_CD",
                      "FIRST_SCRTX_DET_MD_FG":"SCRTX_DET_MD_FG",
                      "FIRST_SCRTX_DET_QTY_RQRD_FG":"SCRTX_DET_QTY_RQRD_FG",
                      "FIRST_SCRTX_DET_SUBPRD_CNT":"SCRTX_DET_SUBPRD_CNT",
                      "FIRST_SCRTX_DET_QTY_ALLOWED_FG":"SCRTX_DET_QTY_ALLOWED_FG",
                      "FIRST_SCRTX_DET_SLS_AUTH_FG":"SCRTX_DET_SLS_AUTH_FG",
                      "FIRST_SCRTX_DET_FOOD_STAMP_FG":"SCRTX_DET_FOOD_STAMP_FG",
                      "FIRST_SCRTX_DET_WIC_FG":"SCRTX_DET_WIC_FG",
                      "FIRST_SCRTX_DET_PERPET_INV_FG":"SCRTX_DET_PERPET_INV_FG",
                      "FIRST_SCRTX_DET_RTL_PRC":"SCRTX_DET_RTL_PRC",
                      "FIRST_SCRTX_DET_UNT_CST":"SCRTX_DET_UNT_CST",
                      "FIRST_SCRTX_DET_MAN_PRC_LVL":"SCRTX_DET_MAN_PRC_LVL",
                      "FIRST_SCRTX_DET_MIN_MDSE_AMT":"SCRTX_DET_MIN_MDSE_AMT",
                      "FIRST_SCRTX_DET_RTL_PRC_DATE":"SCRTX_DET_RTL_PRC_DATE",
                      "FIRST_SCRTX_DET_SERIAL_MDSE_FG":"SCRTX_DET_SERIAL_MDSE_FG",
                      "FIRST_SCRTX_DET_CNTR_PRC":"SCRTX_DET_CNTR_PRC",
                      "FIRST_SCRTX_DET_MAX_MDSE_AMT":"SCRTX_DET_MAX_MDSE_AMT",
                      "FIRST_SCRTX_DET_CNTR_PRC_DATE":"SCRTX_DET_CNTR_PRC_DATE",
                      "FIRST_SCRTX_DET_NG_ENTRY_FG":"SCRTX_DET_NG_ENTRY_FG",
                      "FIRST_SCRTX_DET_STR_CPN_FG":"SCRTX_DET_STR_CPN_FG",
                      "FIRST_SCRTX_DET_VEN_CPN_FG":"SCRTX_DET_VEN_CPN_FG",
                      "FIRST_SCRTX_DET_MAN_PRC_FG":"SCRTX_DET_MAN_PRC_FG",
                      "FIRST_SCRTX_DET_WGT_ITM_FG":"SCRTX_DET_WGT_ITM_FG",
                      "FIRST_SCRTX_DET_NON_DISC_FG":"SCRTX_DET_NON_DISC_FG",
                      "FIRST_SCRTX_DET_COST_PLUS_FG":"SCRTX_DET_COST_PLUS_FG",
                      "FIRST_SCRTX_DET_PRC_VRFY_FG":"SCRTX_DET_PRC_VRFY_FG",
                      "FIRST_SCRTX_DET_PRC_OVRD_FG":"SCRTX_DET_PRC_OVRD_FG",
                      "FIRST_SCRTX_DET_SPLR_PROM_FG":"SCRTX_DET_SPLR_PROM_FG",
                      "FIRST_SCRTX_DET_SAVE_DISC_FG":"SCRTX_DET_SAVE_DISC_FG",
                      "FIRST_SCRTX_DET_ITM_ONSALE_FG":"SCRTX_DET_ITM_ONSALE_FG",
                      "FIRST_SCRTX_DET_INHBT_QTY_FG":"SCRTX_DET_INHBT_QTY_FG",
                      "FIRST_SCRTX_DET_DCML_QTY_FG":"SCRTX_DET_DCML_QTY_FG",
                      "FIRST_SCRTX_DET_SHELF_LBL_RQRD_FG":"SCRTX_DET_SHELF_LBL_RQRD_FG",
                      "FIRST_SCRTX_DET_TAX_RATE1_FG":"SCRTX_DET_TAX_RATE1_FG",
                      "FIRST_SCRTX_DET_TAX_RATE2_FG":"SCRTX_DET_TAX_RATE2_FG",
                      "FIRST_SCRTX_DET_TAX_RATE3_FG":"SCRTX_DET_TAX_RATE3_FG",
                      "FIRST_SCRTX_DET_TAX_RATE4_FG":"SCRTX_DET_TAX_RATE4_FG",
                      "FIRST_SCRTX_DET_TAX_RATE5_FG":"SCRTX_DET_TAX_RATE5_FG",
                      "FIRST_SCRTX_DET_TAX_RATE6_FG":"SCRTX_DET_TAX_RATE6_FG",
                      "FIRST_SCRTX_DET_TAX_RATE7_FG":"SCRTX_DET_TAX_RATE7_FG",
                      "FIRST_SCRTX_DET_TAX_RATE8_FG":"SCRTX_DET_TAX_RATE8_FG",
                      "FIRST_SCRTX_DET_COST_CASE_PRC":"SCRTX_DET_COST_CASE_PRC",
                      "FIRST_SCRTX_DET_DATE_COST_CASE_PRC":"SCRTX_DET_DATE_COST_CASE_PRC",
                      "FIRST_SCRTX_DET_UNIT_CASE":"SCRTX_DET_UNIT_CASE",
                      "FIRST_SCRTX_DET_MIX_MATCH_CD":"SCRTX_DET_MIX_MATCH_CD",
                      "FIRST_SCRTX_DET_RTN_CD":"SCRTX_DET_RTN_CD",
                      "FIRST_SCRTX_DET_FAMILY_CD":"SCRTX_DET_FAMILY_CD",
                      "FIRST_SCRTX_DET_SUBDEP_ID":"SCRTX_DET_SUBDEP_ID",
                      "FIRST_SCRTX_DET_DISC_CD":"SCRTX_DET_DISC_CD",
                      "FIRST_SCRTX_DET_LBL_QTY":"SCRTX_DET_LBL_QTY",
                      "FIRST_SCRTX_DET_SCALE_FG":"SCRTX_DET_SCALE_FG",
                      "FIRST_SCRTX_DET_LOCAL_DEL_FG":"SCRTX_DET_LOCAL_DEL_FG",
                      "FIRST_SCRTX_DET_HOST_DEL_FG":"SCRTX_DET_HOST_DEL_FG",
                      "FIRST_SCRTX_DET_HEAD_OFFICE_DEP":"SCRTX_DET_HEAD_OFFICE_DEP",
                      "FIRST_SCRTX_DET_WGT_SCALE_FG":"SCRTX_DET_WGT_SCALE_FG",
                      "FIRST_SCRTX_DET_FREQ_SHOP_TYPE":"SCRTX_DET_FREQ_SHOP_TYPE",
                      "FIRST_SCRTX_DET_FREQ_SHOP_VAL":"SCRTX_DET_FREQ_SHOP_VAL",
                      "FIRST_SCRTX_DET_SEC_FAMILY":"SCRTX_DET_SEC_FAMILY",
                      "FIRST_SCRTX_DET_POS_MSG":"SCRTX_DET_POS_MSG",
                      "FIRST_SCRTX_DET_SHELF_LIFE_DAY":"SCRTX_DET_SHELF_LIFE_DAY",
                      "FIRST_SCRTX_DET_PROM_NBR":"SCRTX_DET_PROM_NBR",
                      "FIRST_SCRTX_DET_BCKT_NBR":"SCRTX_DET_BCKT_NBR",
                      "FIRST_SCRTX_DET_EXTND_PROM_NBR":"SCRTX_DET_EXTND_PROM_NBR",
                      "FIRST_SCRTX_DET_EXTND_BCKT_NBR":"SCRTX_DET_EXTND_BCKT_NBR",
                      "FIRST_SCRTX_DET_RCPT_DESCR1":"SCRTX_DET_RCPT_DESCR1",
                      "FIRST_SCRTX_DET_RCPT_DESCR2":"SCRTX_DET_RCPT_DESCR2",
                      "FIRST_SCRTX_DET_RCPT_DESCR3":"SCRTX_DET_RCPT_DESCR3",
                      "FIRST_SCRTX_DET_RCPT_DESCR4":"SCRTX_DET_RCPT_DESCR4",
                      "FIRST_SCRTX_DET_CPN_NBR":"SCRTX_DET_CPN_NBR",
                      "FIRST_SCRTX_DET_TAR_WGT_NBR":"SCRTX_DET_TAR_WGT_NBR",
                      "FIRST_SCRTX_DET_RSTRCT_LAYOUT":"SCRTX_DET_RSTRCT_LAYOUT",
                      "FIRST_SCRTX_DET_INTRNL_ID":"SCRTX_DET_INTRNL_ID",
                      "FIRST_SCRTX_DET_OLD_PRC":"SCRTX_DET_OLD_PRC",
                      "FIRST_SCRTX_DET_QDX_FREQ_SHOP_VAL":"SCRTX_DET_QDX_FREQ_SHOP_VAL",
                      "FIRST_SCRTX_DET_VND_ID":"SCRTX_DET_VND_ID",
                      "FIRST_SCRTX_DET_VND_ITM_ID":"SCRTX_DET_VND_ITM_ID",
                      "FIRST_SCRTX_DET_VND_ITM_SZ":"SCRTX_DET_VND_ITM_SZ",
                      "FIRST_SCRTX_DET_CMPRTV_UOM":"SCRTX_DET_CMPRTV_UOM",
                      "FIRST_SCRTX_DET_CMPR_QTY":"SCRTX_DET_CMPR_QTY",
                      "FIRST_SCRTX_DET_CMPR_UNT":"SCRTX_DET_CMPR_UNT",
                      "FIRST_SCRTX_DET_BNS_CPN_FG":"SCRTX_DET_BNS_CPN_FG",
                      "FIRST_SCRTX_DET_EX_MIN_PURCH_FG":"SCRTX_DET_EX_MIN_PURCH_FG",
                      "FIRST_SCRTX_DET_FUEL_FG":"SCRTX_DET_FUEL_FG",
                      "FIRST_SCRTX_DET_SPR_AUTH_RQRD_FG":"SCRTX_DET_SPR_AUTH_RQRD_FG",
                      "FIRST_SCRTX_DET_SSP_PRDCT_FG":"SCRTX_DET_SSP_PRDCT_FG",
                      "FIRST_SCRTX_DET_NU06_FG":"SCRTX_DET_NU06_FG",
                      "FIRST_SCRTX_DET_NU07_FG":"SCRTX_DET_NU07_FG",
                      "FIRST_SCRTX_DET_NU08_FG":"SCRTX_DET_NU08_FG",
                      "FIRST_SCRTX_DET_NU09_FG":"SCRTX_DET_NU09_FG",
                      "FIRST_SCRTX_DET_NU10_FG":"SCRTX_DET_NU10_FG",
                      "FIRST_SCRTX_DET_FREQ_SHOP_LMT":"SCRTX_DET_FREQ_SHOP_LMT",
                      "FIRST_SCRTX_DET_ITM_STATUS":"SCRTX_DET_ITM_STATUS",
                      "FIRST_SCRTX_DET_DEA_GRP":"SCRTX_DET_DEA_GRP",
                      "FIRST_SCRTX_DET_BNS_BY_OPCODE":"SCRTX_DET_BNS_BY_OPCODE",
                      "FIRST_SCRTX_DET_BNS_BY_DESCR":"SCRTX_DET_BNS_BY_DESCR",
                      "FIRST_SCRTX_DET_COMP_TYPE":"SCRTX_DET_COMP_TYPE",
                      "FIRST_SCRTX_DET_COMP_PRC":"SCRTX_DET_COMP_PRC",
                      "FIRST_SCRTX_DET_COMP_QTY":"SCRTX_DET_COMP_QTY",
                      "FIRST_SCRTX_DET_ASSUME_QTY_FG":"SCRTX_DET_ASSUME_QTY_FG",
                      "FIRST_SCRTX_DET_EXCISE_TAX_NBR":"SCRTX_DET_EXCISE_TAX_NBR",
                      "FIRST_SCRTX_DET_RTL_PRICE_DATE":"SCRTX_DET_RTL_PRICE_DATE",
                      "FIRST_SCRTX_DET_PRC_RSN_ID":"SCRTX_DET_PRC_RSN_ID",
                      "FIRST_SCRTX_DET_ITM_POINT":"SCRTX_DET_ITM_POINT",
                      "FIRST_SCRTX_DET_PRC_GRP_ID":"SCRTX_DET_PRC_GRP_ID",
                      "FIRST_SCRTX_DET_SWW_CODE_FG":"SCRTX_DET_SWW_CODE_FG",
                      "FIRST_SCRTX_DET_SHELF_STOCK_FG":"SCRTX_DET_SHELF_STOCK_FG",
                      "FIRST_SCRTX_DET_PRT_PLUID_RCPT_FG":"SCRTX_DET_PRT_PLUID_RCPT_FG",
                      "FIRST_SCRTX_DET_BLK_GRP":"SCRTX_DET_BLK_GRP",
                      "FIRST_SCRTX_DET_EXCHNGE_TENDER_ID":"SCRTX_DET_EXCHNGE_TENDER_ID",
                      "FIRST_SCRTX_DET_CAR_WASH_FG":"SCRTX_DET_CAR_WASH_FG",
                      "FIRST_SCRTX_DET_EXMPT_FRM_PROM_FG":"SCRTX_DET_EXMPT_FRM_PROM_FG",
                      "FIRST_SCRTX_DET_QSR_ITM_TYP":"SCRTX_DET_QSR_ITM_TYP",
                      "FIRST_SCRTX_DET_RSTRCSALE_BRCD_FG":"SCRTX_DET_RSTRCSALE_BRCD_FG",
                      "FIRST_SCRTX_DET_NON_RX_HEALTH_FG":"SCRTX_DET_NON_RX_HEALTH_FG",
                      "FIRST_SCRTX_DET_RX_FG":"SCRTX_DET_RX_FG",
                      "FIRST_SCRTX_DET_LNK_NBR":"SCRTX_DET_LNK_NBR",
                      "FIRST_SCRTX_DET_WIC_CVV_FG":"SCRTX_DET_WIC_CVV_FG",
                      "FIRST_SCRTX_DET_CENTRAL_ITEM":"SCRTX_DET_CENTRAL_ITEM",
                      "FIRST_SCRTX_HDR_ACT_DATE":"SCRTX_HDR_ACT_DATE",
                      "FIRST_BANNER_ID":"BANNER_ID",
                      "FIRST_INSERT_ID":"INSERT_ID",
                      "FIRST_INSERT_TIMESTAMP":"INSERT_TIMESTAMP",
                      "FIRST_LAST_UPDATE_ID":"LAST_UPDATE_ID",
                      "FIRST_LAST_UPDATE_TIMESTAMP":"LAST_UPDATE_TIMESTAMP",
                      "FIRST_COUPON_NO":"COUPON_NO",
                      "FIRST_RTX_BATCH":"RTX_BATCH",
                      "FIRST_RTX_TYPE":"RTX_TYPE",
                      "FIRST_RTX_UPC":"RTX_UPC",
                      "FIRST_RTX_LOAD":"RTX_LOAD" }

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SQL Table query function

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Fetch Banner id from store detail table

# COMMAND ----------

def fetchBannerId(storeDf):
  loggerAtt.info("Fetch Banner ID from Store Detial Delta table initiated")
  try:
    temp_table_name = "storeDf"
    storeDf.createOrReplaceTempView(temp_table_name)
    storeDf = spark.sql('''SELECT BANNER_ID, storeDf.Store_ID, storeRowNumber, storeEndRowNumber, storeData FROM delta.`{}` as STORE_DETAILS INNER JOIN storeDf ON storeDf.Store_ID = STORE_DETAILS.STORE_NUMBER'''.format(storeDeltaPath))
    storeDf = storeDf.withColumn('BANNER_ID', regexp_replace(col("BANNER_ID"), " ", ""))
    spark.catalog.dropTempView(temp_table_name)
  except Exception as ex:
    loggerAtt.info("Fetch Banner ID from Store Detial Delta table failed and throwed error")
    loggerAtt.error(str(ex))
    err = ErrorReturn('Error', ex,'fetchBannerId')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  loggerAtt.info("Fetch Banner ID from Store Detial Delta table end")
  return storeDf

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Coupon Archival Process

# COMMAND ----------

def couponArchival(couponDeltaPath,Date,couponArchivalpath):
  couponArchivalDf = spark.read.format('delta').load(couponDeltaPath)
  
  initial_recs = couponArchivalDf.count()
  loggerAtt.info(f"Initial count of records in delta table: {initial_recs}")
  ABC(archivalInitCount=initial_recs)
  
  if couponArchivalDf.count() >0:
    couponArchivalDf = couponArchivalDf.filter((col("STATUS") == "D") & (datediff(to_date(current_date()),to_date(date_format(date_func(col('LAST_UPDATE_TIMESTAMP')), 'yyyy-MM-dd'))) >=1)) 
    if couponArchivalDf.count() >0:
      couponArchivalDf=couponArchivalDf.withColumn("START_DATE",date_format(col("START_DATE"), 'yyyy/MM/dd').cast(StringType()))
      couponArchivalDf=couponArchivalDf.withColumn("END_DATE",date_format(col("END_DATE"), 'yyyy/MM/dd').cast(StringType()))
      couponArchivalDf=couponArchivalDf.withColumn('COUPON_NO', lpad(col('COUPON_NO'),14,'0'))
      couponArchivalDf.write.mode('Append').format('parquet').save(couponArchivalpath + "/" +Date+ "/" +"Coupon_Archival_Records")
      deltaTable = DeltaTable.forPath(spark, couponDeltaPath)
      deltaTable.delete((col("STATUS") == "D") &  (datediff(to_date(current_date()),to_date(date_format(date_func(col('LAST_UPDATE_TIMESTAMP')), 'yyyy-MM-dd'))) >=1))

      after_recs = spark.read.format('delta').load(couponDeltaPath).count()
      loggerAtt.info(f"After count of records in delta table: {after_recs}")
      ABC(archivalAfterCount=after_recs)

      loggerAtt.info('========coupon Records Archival successful ========')
    else:
      loggerAtt.info('======== No coupon Records Archival Done ========')
  else:
    loggerAtt.info('======== No coupon Records Archival Done ========')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Coupon Write to ADLS location

# COMMAND ----------

def couponWrite(couponDeltaPath,couponOutbondPath,storeList):
  couponOutputDf = spark.read.format('delta').load(couponDeltaPath)
  if processing_file == 'COD':
    couponOutputDf = couponOutputDf.filter((col("LOCATION").isin(storeList))) 
  if couponOutputDf.count() >0:
    couponOutputDf=couponOutputDf.withColumn("START_DATE",date_format(col("START_DATE"), 'yyyy/MM/dd').cast(StringType()))
    couponOutputDf=couponOutputDf.withColumn("END_DATE",date_format(col("END_DATE"), 'yyyy/MM/dd').cast(StringType()))
    couponOutputDf=couponOutputDf.withColumn('COUPON_NO', lpad(col('COUPON_NO'),14,'0'))
    ABC(couponOutputFileCount=couponOutputDf.count())
    couponOutputDf.write.partitionBy('LOCATION').mode('overwrite').format('parquet').save(couponOutboundPath + "/" +"Coupon_Output")
    loggerAtt.info('========coupon Records Output successful ========')
  else:
    loggerAtt.info('======== No coupon Records Output Done ========')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Update Item table for coupon delete

# COMMAND ----------

def updateItemPromoLinkRecords(linkDf):
  loggerAtt.info("Merge into Delta table initiated for coupon item promo link")
  try:
    linkDf = linkDf.withColumn("PROMO_STATUS", lit('C'))
    
    linkDf = linkDf.select([c for c in ["PRO_LINK_LINK_ITEM_ID", "Store_ID", "PRO_LINK_MEM_PRO_ID", "PROMO_STATUS", "INSERT_ID", "INSERT_TIMESTAMP", "LAST_UPDATE_ID", "LAST_UPDATE_TIMESTAMP"]])
    
    linkDf = quinn.with_some_columns_renamed(link_table, link_change_col_name)(linkDf)
    
    temp_table_name = "linkDf"
    linkDf.createOrReplaceTempView(temp_table_name)    
    
    loggerAtt.info(f"No of coupon Item records to update table: {linkDf.count()}")
    ABC(updateItemPromoLinkCount=linkDf.count())
    
    initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(promoLinkingDeltaPath))
    loggerAtt.info(f"Initial count of records in Delta Table: {initial_recs.head(1)}")
    initial_recs = initial_recs.head(1)
    ABC(updateItemPromoInitCount=initial_recs[0][0])
    
    spark.sql('''
      MERGE INTO delta.`{}` as ItemDelta
      USING linkDf
      ON ItemDelta.PROMO_ITM_ID = linkDf.PROMO_ITM_ID AND
         ItemDelta.PROMO_STORE_ID = linkDf.PROMO_STORE_ID
      WHEN MATCHED THEN
        UPDATE SET ItemDelta.PROMO_COUPON_NO = linkDf.PROMO_COUPON_NO,
                   ItemDelta.PROMO_STATUS = 'M',
                   ItemDelta.LAST_UPDATE_ID = linkDf.LAST_UPDATE_ID,
                   ItemDelta.LAST_UPDATE_TIMESTAMP = linkDf.LAST_UPDATE_TIMESTAMP
      WHEN NOT MATCHED THEN INSERT *  '''.format(promoLinkingDeltaPath))     
    
    appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(promoLinkingDeltaPath))
    loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
    appended_recs = appended_recs.head(1)
    ABC(updateItemPromoFinalCount=appended_recs[0][0])
    spark.catalog.dropTempView(temp_table_name)
  except Exception as ex:
    ABC(couponItemPromoLinkCheck=0)
    ABC(updateItemPromoLinkCount='')
    ABC(updateItemPromoInitCount='')
    ABC(updateItemPromoFinalCount='')
    loggerAtt.info("Merge into Delta table failed and throwed error")
    loggerAtt.error(str(ex))
    err = ErrorReturn('Error', ex,'updateItemPromoLinkRecords')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  loggerAtt.info("Merge into Delta table initiated for coupon item promo link successful")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Update Link table new coupon promotion

# COMMAND ----------

def updateItemCouponDeleteRecords(promoDf):
  loggerAtt.info("Merge into Delta table initiated for coupon item promo link")
  try:
    promoDf = promoDf.filter((col('STATUS')=='D'))
    
    loggerAtt.info(f"No of coupon delete records to update Item table: {promoDf.count()}")
    
    if promoDf.count() > 0:
    
      itemDeltaDf = spark.read.format('delta').load(promoLinkingDeltaPath)
      itemDeltaDf = itemDeltaDf.join(promoDf, [promoDf.COUPON_NO == itemDeltaDf.PROMO_COUPON_NO, promoDf.LOCATION == itemDeltaDf.PROMO_STORE_ID], how='inner').select(['PROMO_STORE_ID','PROMO_ITM_ID'])

      ABC(deleteCouponCount=promoDf.count())

      
      itemDeltaDf = itemDeltaDf.withColumn("LAST_UPDATE_ID",lit(pipelineid))
      itemDeltaDf = itemDeltaDf.withColumn("LAST_UPDATE_TIMESTAMP",current_timestamp())
      
      temp_table_name = "itemDeltaDf"
      itemDeltaDf.createOrReplaceTempView(temp_table_name)
      
      loggerAtt.info(f"No of item records to update Item table: {itemDeltaDf.count()}")
      ABC(deleteItemCouponUpdateCount=itemDeltaDf.count())

      spark.sql('''
        MERGE INTO delta.`{}` as ItemDelta
        USING itemDeltaDf
        ON ItemDelta.PROMO_ITM_ID = itemDeltaDf.PROMO_ITM_ID AND
           ItemDelta.PROMO_STORE_ID = itemDeltaDf.PROMO_STORE_ID
        WHEN MATCHED THEN
          UPDATE SET ItemDelta.PROMO_COUPON_NO = NULL,
                     ItemDelta.LAST_UPDATE_ID = itemDeltaDf.LAST_UPDATE_ID,
                     ItemDelta.LAST_UPDATE_TIMESTAMP = itemDeltaDf.LAST_UPDATE_TIMESTAMP
                      '''.format(promoLinkingDeltaPath))     
      spark.catalog.dropTempView(temp_table_name)
  except Exception as ex:
    ABC(deleteItemCouponUpdateCount='')
    ABC(deleteCouponCount='')
    ABC(couponDeleteItemCheck=0)
    loggerAtt.info("Merge into Delta table failed and throwed error")
    loggerAtt.error(str(ex))
    err = ErrorReturn('Error', ex,'updateItemCouponDeleteRecords')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  loggerAtt.info("Merge into Delta table initiated for coupon item promo link successful")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Upsert records into coupon table

# COMMAND ----------

def updateInsertCouponRecords(promoDf):
  loggerAtt.info("Merge into Delta table initiated for Coupon update & Insert")
  try:
    temp_table_name = "promoDf"
    promoDf.createOrReplaceTempView(temp_table_name)
    
    initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(couponDeltaPath))
    loggerAtt.info(f"Initial count of records in Delta Table: {initial_recs.head(1)}")
    initial_recs = initial_recs.head(1)
    ABC(DeltaTableInitCount=initial_recs[0][0])
    
    spark.sql('''
      MERGE INTO delta.`{}` as Coupon
      USING promoDf
      ON Coupon.COUPON_NO = promoDf.COUPON_NO AND
         Coupon.LOCATION = promoDf.LOCATION
      WHEN MATCHED and promoDf.STATUS = 'D' THEN
        UPDATE SET
                  Coupon.BANNER_ID = promoDf.BANNER_ID,
                  Coupon.STATUS = promoDf.STATUS,
                  Coupon.START_DATE = promoDf.START_DATE,
                  Coupon.END_DATE = promoDf.END_DATE,
                  Coupon.DEL_DATE = promoDf.DEL_DATE,
                  Coupon.PERF_DETL_SUB_TYPE = promoDf.PERF_DETL_SUB_TYPE,
                  Coupon.LIMIT = promoDf.LIMIT,
                  Coupon.CHANGE_AMOUNT_PCT = promoDf.CHANGE_AMOUNT_PCT,
                  Coupon.CLUB_CARD = promoDf.CLUB_CARD,
                  Coupon.MIN_QUANTITY = promoDf.MIN_QUANTITY,
                  Coupon.BUY_QUANTITY = promoDf.BUY_QUANTITY,
                  Coupon.GET_QUANTITY = promoDf.GET_QUANTITY,
                  Coupon.SALE_QUANTITY = promoDf.SALE_QUANTITY,
                  Coupon.DESCRIPTION = promoDf.DESCRIPTION,
                  Coupon.SELL_BY_WEIGHT_IND = promoDf.SELL_BY_WEIGHT_IND,
                  Coupon.AHO_PERF_DETAIL_ID = promoDf.AHO_PERF_DETAIL_ID,
                  Coupon.PROMO_HDR_FILE_NUM = promoDf.PROMO_HDR_FILE_NUM,
                  Coupon.PROMO_HDR_ACTION = promoDf.PROMO_HDR_ACTION,
                  Coupon.PROMO_HDR_PART_OFFSET = promoDf.PROMO_HDR_PART_OFFSET,
                  Coupon.PROMO_HDR_PART_LENGTH = promoDf.PROMO_HDR_PART_LENGTH,
                  Coupon.PROMO_HDR_BIT_FLD = promoDf.PROMO_HDR_BIT_FLD,
                  Coupon.PROMO_HDR_PEND_DATE = promoDf.PROMO_HDR_PEND_DATE,
                  Coupon.PROMO_HDR_PEND_TIME = promoDf.PROMO_HDR_PEND_TIME,
                  Coupon.PROMO_HDR_VERSION = promoDf.PROMO_HDR_VERSION,
                  Coupon.PROMO_HDR_BATCH_NUM = promoDf.PROMO_HDR_BATCH_NUM,
                  Coupon.PROMO_HDR_STATUS = promoDf.PROMO_HDR_STATUS,
                  Coupon.PROMO_TYPE = promoDf.PROMO_TYPE,
                  Coupon.PROMO_DESCRIPTION = promoDf.PROMO_DESCRIPTION,
                  Coupon.PROMO_DEPARTMENT = promoDf.PROMO_DEPARTMENT,
                  Coupon.PROMO_MEM_CARD_SCHEME = promoDf.PROMO_MEM_CARD_SCHEME,
                  Coupon.PROMO_REWARD_VALUE = promoDf.PROMO_REWARD_VALUE,
                  Coupon.PROMO_REWARD_VALUE_AMT = promoDf.PROMO_REWARD_VALUE_AMT,
                  Coupon.PROMO_REWARD_VALUE_PER = promoDf.PROMO_REWARD_VALUE_PER,
                  Coupon.PROMO_MEM_CARD_REQUIRED = promoDf.PROMO_MEM_CARD_REQUIRED,
                  Coupon.PROMO_ALL_CARD_SCHEMES = promoDf.PROMO_ALL_CARD_SCHEMES,
                  Coupon.PROMO_CARD_SCHEME = promoDf.PROMO_CARD_SCHEME,
                  Coupon.PROMO_LIMITED_QTY = promoDf.PROMO_LIMITED_QTY,
                  Coupon.PROMO_ENHANCED_GROUP_TYPE = promoDf.PROMO_ENHANCED_GROUP_TYPE,
                  Coupon.PROMO_ENHANCED_THRESHOLD_QTY = promoDf.PROMO_ENHANCED_THRESHOLD_QTY,
                  Coupon.PROMO_ENHANCED_STEP_COUNT_QTY = promoDf.PROMO_ENHANCED_STEP_COUNT_QTY,
                  Coupon.PROMO_START_TIME = promoDf.PROMO_START_TIME,
                  Coupon.PROMO_END_TIME = promoDf.PROMO_END_TIME,
                  Coupon.PROMO_ACTIVATION_DAY_1 = promoDf.PROMO_ACTIVATION_DAY_1,
                  Coupon.PROMO_ACTIVATION_DAY_2 = promoDf.PROMO_ACTIVATION_DAY_2,
                  Coupon.PROMO_ACTIVATION_DAY_3 = promoDf.PROMO_ACTIVATION_DAY_3,
                  Coupon.PROMO_ACTIVATION_DAY_4 = promoDf.PROMO_ACTIVATION_DAY_4,
                  Coupon.PROMO_ACTIVATION_DAY_5 = promoDf.PROMO_ACTIVATION_DAY_5,
                  Coupon.PROMO_ACTIVATION_DAY_6 = promoDf.PROMO_ACTIVATION_DAY_6,
                  Coupon.PROMO_ACTIVATION_DAY_7 = promoDf.PROMO_ACTIVATION_DAY_7,
                  Coupon.PROMO_ACTIVATION_TIME_1 = promoDf.PROMO_ACTIVATION_TIME_1,
                  Coupon.PROMO_ACTIVATION_TIME_2 = promoDf.PROMO_ACTIVATION_TIME_2,
                  Coupon.PROMO_ACTIVATION_TIME_3 = promoDf.PROMO_ACTIVATION_TIME_3,
                  Coupon.PROMO_ACTIVATION_TIME_4 = promoDf.PROMO_ACTIVATION_TIME_4,
                  Coupon.PROMO_ACTIVATION_TIME_5 = promoDf.PROMO_ACTIVATION_TIME_5,
                  Coupon.PROMO_ACTIVATION_TIME_6 = promoDf.PROMO_ACTIVATION_TIME_6,
                  Coupon.PROMO_ACTIVATION_TIME_7   = promoDf.PROMO_ACTIVATION_TIME_7  ,
                  Coupon.PROMO_TRIGGER_FLAGS_2 = promoDf.PROMO_TRIGGER_FLAGS_2,
                  Coupon.PROMO_LOW_HIGH_REWARD = promoDf.PROMO_LOW_HIGH_REWARD,
                  Coupon.PROMO_MIN_ITEM_VALUE = promoDf.PROMO_MIN_ITEM_VALUE,
                  Coupon.PROMO_MIN_ITEM_WEIGHT = promoDf.PROMO_MIN_ITEM_WEIGHT,
                  Coupon.PROMO_MIN_PURCHASE = promoDf.PROMO_MIN_PURCHASE,
                  Coupon.PROMO_DELAYED_PROMO = promoDf.PROMO_DELAYED_PROMO,
                  Coupon.PROMO_CASHIER_ENTERED = promoDf.PROMO_CASHIER_ENTERED,
                  Coupon.PROMO_REQ_COUPON_CODE = promoDf.PROMO_REQ_COUPON_CODE,
                  Coupon.PROMO_LINKING_PROMO = promoDf.PROMO_LINKING_PROMO,
                  Coupon.PROMO_MAX_ITEM_WEIGHT = promoDf.PROMO_MAX_ITEM_WEIGHT,
                  Coupon.PROMO_SEGMENTS_1 = promoDf.PROMO_SEGMENTS_1,
                  Coupon.PROMO_SEGMENTS_2 = promoDf.PROMO_SEGMENTS_2,
                  Coupon.PROMO_SEGMENTS_3 = promoDf.PROMO_SEGMENTS_3,
                  Coupon.PROMO_SEGMENTS_4 = promoDf.PROMO_SEGMENTS_4,
                  Coupon.PROMO_SEGMENTS_5 = promoDf.PROMO_SEGMENTS_5,
                  Coupon.PROMO_SEGMENTS_6 = promoDf.PROMO_SEGMENTS_6,
                  Coupon.PROMO_SEGMENTS_7 = promoDf.PROMO_SEGMENTS_7,
                  Coupon.PROMO_SEGMENTS_8 = promoDf.PROMO_SEGMENTS_8,
                  Coupon.PROMO_SEGMENTS_9 = promoDf.PROMO_SEGMENTS_9,
                  Coupon.PROMO_SEGMENTS_10 = promoDf.PROMO_SEGMENTS_10,
                  Coupon.PROMO_SEGMENTS_11 = promoDf.PROMO_SEGMENTS_11,
                  Coupon.PROMO_SEGMENTS_12 = promoDf.PROMO_SEGMENTS_12,
                  Coupon.PROMO_SEGMENTS_13 = promoDf.PROMO_SEGMENTS_13,
                  Coupon.PROMO_SEGMENTS_14 = promoDf.PROMO_SEGMENTS_14,
                  Coupon.PROMO_SEGMENTS_15 = promoDf.PROMO_SEGMENTS_15,
                  Coupon.PROMO_SEGMENTS_16 = promoDf.PROMO_SEGMENTS_16,
                  Coupon.PROMO_UPD_LOYALTY_SER = promoDf.PROMO_UPD_LOYALTY_SER,
                  Coupon.PROMO_CPN_REQ_TYPE = promoDf.PROMO_CPN_REQ_TYPE,
                  Coupon.PROMO_CREDIT_PROGRAM_ID = promoDf.PROMO_CREDIT_PROGRAM_ID,
                  Coupon.PROMO_PROMO_EXTERNAL_ID = promoDf.PROMO_PROMO_EXTERNAL_ID,
                  Coupon.PROMO_DEPARTMENT_4DIG = promoDf.PROMO_DEPARTMENT_4DIG,
                  Coupon.LAST_UPDATE_ID = promoDf.LAST_UPDATE_ID,
                  Coupon.LAST_UPDATE_TIMESTAMP = promoDf.LAST_UPDATE_TIMESTAMP
      WHEN MATCHED and promoDf.STATUS = 'C' THEN
        UPDATE SET
                  Coupon.BANNER_ID = promoDf.BANNER_ID,
                  Coupon.STATUS = 'M',
                  Coupon.START_DATE = promoDf.START_DATE,
                  Coupon.END_DATE = promoDf.END_DATE,
                  Coupon.DEL_DATE = promoDf.DEL_DATE,
                  Coupon.PERF_DETL_SUB_TYPE = promoDf.PERF_DETL_SUB_TYPE,
                  Coupon.LIMIT = promoDf.LIMIT,
                  Coupon.CHANGE_AMOUNT_PCT = promoDf.CHANGE_AMOUNT_PCT,
                  Coupon.CLUB_CARD = promoDf.CLUB_CARD,
                  Coupon.MIN_QUANTITY = promoDf.MIN_QUANTITY,
                  Coupon.BUY_QUANTITY = promoDf.BUY_QUANTITY,
                  Coupon.GET_QUANTITY = promoDf.GET_QUANTITY,
                  Coupon.SALE_QUANTITY = promoDf.SALE_QUANTITY,
                  Coupon.DESCRIPTION = promoDf.DESCRIPTION,
                  Coupon.SELL_BY_WEIGHT_IND = promoDf.SELL_BY_WEIGHT_IND,
                  Coupon.AHO_PERF_DETAIL_ID = promoDf.AHO_PERF_DETAIL_ID,
                  Coupon.PROMO_HDR_FILE_NUM = promoDf.PROMO_HDR_FILE_NUM,
                  Coupon.PROMO_HDR_ACTION = promoDf.PROMO_HDR_ACTION,
                  Coupon.PROMO_HDR_PART_OFFSET = promoDf.PROMO_HDR_PART_OFFSET,
                  Coupon.PROMO_HDR_PART_LENGTH = promoDf.PROMO_HDR_PART_LENGTH,
                  Coupon.PROMO_HDR_BIT_FLD = promoDf.PROMO_HDR_BIT_FLD,
                  Coupon.PROMO_HDR_PEND_DATE = promoDf.PROMO_HDR_PEND_DATE,
                  Coupon.PROMO_HDR_PEND_TIME = promoDf.PROMO_HDR_PEND_TIME,
                  Coupon.PROMO_HDR_VERSION = promoDf.PROMO_HDR_VERSION,
                  Coupon.PROMO_HDR_BATCH_NUM = promoDf.PROMO_HDR_BATCH_NUM,
                  Coupon.PROMO_HDR_STATUS = promoDf.PROMO_HDR_STATUS,
                  Coupon.PROMO_TYPE = promoDf.PROMO_TYPE,
                  Coupon.PROMO_DESCRIPTION = promoDf.PROMO_DESCRIPTION,
                  Coupon.PROMO_DEPARTMENT = promoDf.PROMO_DEPARTMENT,
                  Coupon.PROMO_MEM_CARD_SCHEME = promoDf.PROMO_MEM_CARD_SCHEME,
                  Coupon.PROMO_REWARD_VALUE = promoDf.PROMO_REWARD_VALUE,
                  Coupon.PROMO_REWARD_VALUE_AMT = promoDf.PROMO_REWARD_VALUE_AMT,
                  Coupon.PROMO_REWARD_VALUE_PER = promoDf.PROMO_REWARD_VALUE_PER,
                  Coupon.PROMO_MEM_CARD_REQUIRED = promoDf.PROMO_MEM_CARD_REQUIRED,
                  Coupon.PROMO_ALL_CARD_SCHEMES = promoDf.PROMO_ALL_CARD_SCHEMES,
                  Coupon.PROMO_CARD_SCHEME = promoDf.PROMO_CARD_SCHEME,
                  Coupon.PROMO_LIMITED_QTY = promoDf.PROMO_LIMITED_QTY,
                  Coupon.PROMO_ENHANCED_GROUP_TYPE = promoDf.PROMO_ENHANCED_GROUP_TYPE,
                  Coupon.PROMO_ENHANCED_THRESHOLD_QTY = promoDf.PROMO_ENHANCED_THRESHOLD_QTY,
                  Coupon.PROMO_ENHANCED_STEP_COUNT_QTY = promoDf.PROMO_ENHANCED_STEP_COUNT_QTY,
                  Coupon.PROMO_START_TIME = promoDf.PROMO_START_TIME,
                  Coupon.PROMO_END_TIME = promoDf.PROMO_END_TIME,
                  Coupon.PROMO_ACTIVATION_DAY_1 = promoDf.PROMO_ACTIVATION_DAY_1,
                  Coupon.PROMO_ACTIVATION_DAY_2 = promoDf.PROMO_ACTIVATION_DAY_2,
                  Coupon.PROMO_ACTIVATION_DAY_3 = promoDf.PROMO_ACTIVATION_DAY_3,
                  Coupon.PROMO_ACTIVATION_DAY_4 = promoDf.PROMO_ACTIVATION_DAY_4,
                  Coupon.PROMO_ACTIVATION_DAY_5 = promoDf.PROMO_ACTIVATION_DAY_5,
                  Coupon.PROMO_ACTIVATION_DAY_6 = promoDf.PROMO_ACTIVATION_DAY_6,
                  Coupon.PROMO_ACTIVATION_DAY_7 = promoDf.PROMO_ACTIVATION_DAY_7,
                  Coupon.PROMO_ACTIVATION_TIME_1 = promoDf.PROMO_ACTIVATION_TIME_1,
                  Coupon.PROMO_ACTIVATION_TIME_2 = promoDf.PROMO_ACTIVATION_TIME_2,
                  Coupon.PROMO_ACTIVATION_TIME_3 = promoDf.PROMO_ACTIVATION_TIME_3,
                  Coupon.PROMO_ACTIVATION_TIME_4 = promoDf.PROMO_ACTIVATION_TIME_4,
                  Coupon.PROMO_ACTIVATION_TIME_5 = promoDf.PROMO_ACTIVATION_TIME_5,
                  Coupon.PROMO_ACTIVATION_TIME_6 = promoDf.PROMO_ACTIVATION_TIME_6,
                  Coupon.PROMO_ACTIVATION_TIME_7   = promoDf.PROMO_ACTIVATION_TIME_7  ,
                  Coupon.PROMO_TRIGGER_FLAGS_2 = promoDf.PROMO_TRIGGER_FLAGS_2,
                  Coupon.PROMO_LOW_HIGH_REWARD = promoDf.PROMO_LOW_HIGH_REWARD,
                  Coupon.PROMO_MIN_ITEM_VALUE = promoDf.PROMO_MIN_ITEM_VALUE,
                  Coupon.PROMO_MIN_ITEM_WEIGHT = promoDf.PROMO_MIN_ITEM_WEIGHT,
                  Coupon.PROMO_MIN_PURCHASE = promoDf.PROMO_MIN_PURCHASE,
                  Coupon.PROMO_DELAYED_PROMO = promoDf.PROMO_DELAYED_PROMO,
                  Coupon.PROMO_CASHIER_ENTERED = promoDf.PROMO_CASHIER_ENTERED,
                  Coupon.PROMO_REQ_COUPON_CODE = promoDf.PROMO_REQ_COUPON_CODE,
                  Coupon.PROMO_LINKING_PROMO = promoDf.PROMO_LINKING_PROMO,
                  Coupon.PROMO_MAX_ITEM_WEIGHT = promoDf.PROMO_MAX_ITEM_WEIGHT,
                  Coupon.PROMO_SEGMENTS_1 = promoDf.PROMO_SEGMENTS_1,
                  Coupon.PROMO_SEGMENTS_2 = promoDf.PROMO_SEGMENTS_2,
                  Coupon.PROMO_SEGMENTS_3 = promoDf.PROMO_SEGMENTS_3,
                  Coupon.PROMO_SEGMENTS_4 = promoDf.PROMO_SEGMENTS_4,
                  Coupon.PROMO_SEGMENTS_5 = promoDf.PROMO_SEGMENTS_5,
                  Coupon.PROMO_SEGMENTS_6 = promoDf.PROMO_SEGMENTS_6,
                  Coupon.PROMO_SEGMENTS_7 = promoDf.PROMO_SEGMENTS_7,
                  Coupon.PROMO_SEGMENTS_8 = promoDf.PROMO_SEGMENTS_8,
                  Coupon.PROMO_SEGMENTS_9 = promoDf.PROMO_SEGMENTS_9,
                  Coupon.PROMO_SEGMENTS_10 = promoDf.PROMO_SEGMENTS_10,
                  Coupon.PROMO_SEGMENTS_11 = promoDf.PROMO_SEGMENTS_11,
                  Coupon.PROMO_SEGMENTS_12 = promoDf.PROMO_SEGMENTS_12,
                  Coupon.PROMO_SEGMENTS_13 = promoDf.PROMO_SEGMENTS_13,
                  Coupon.PROMO_SEGMENTS_14 = promoDf.PROMO_SEGMENTS_14,
                  Coupon.PROMO_SEGMENTS_15 = promoDf.PROMO_SEGMENTS_15,
                  Coupon.PROMO_SEGMENTS_16 = promoDf.PROMO_SEGMENTS_16,
                  Coupon.PROMO_UPD_LOYALTY_SER = promoDf.PROMO_UPD_LOYALTY_SER,
                  Coupon.PROMO_CPN_REQ_TYPE = promoDf.PROMO_CPN_REQ_TYPE,
                  Coupon.PROMO_CREDIT_PROGRAM_ID = promoDf.PROMO_CREDIT_PROGRAM_ID,
                  Coupon.PROMO_PROMO_EXTERNAL_ID = promoDf.PROMO_PROMO_EXTERNAL_ID,
                  Coupon.PROMO_DEPARTMENT_4DIG = promoDf.PROMO_DEPARTMENT_4DIG,
                  Coupon.LAST_UPDATE_ID = promoDf.LAST_UPDATE_ID,
                  Coupon.LAST_UPDATE_TIMESTAMP = promoDf.LAST_UPDATE_TIMESTAMP                  
      WHEN NOT MATCHED THEN INSERT *  '''.format(couponDeltaPath))     
    
    appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(couponDeltaPath))
    loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
    appended_recs = appended_recs.head(1)
    ABC(DeltaTableFinalCount=appended_recs[0][0])
    spark.catalog.dropTempView(temp_table_name)
  
  except Exception as ex:
    loggerAtt.info("Merge into Delta table failed and throwed error")
    loggerAtt.error(str(ex))
    ABC(DeltaTableCreateCheck = 0)
    ABC(DeltaTableInitCount='')
    ABC(DeltaTableFinalCount='')
    err = ErrorReturn('Error', ex,'MergeDeltaTable')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  loggerAtt.info("Merge into Delta table initiated for Coupon update & Insert successful")                  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Fetching Current day effective records and Modified Active Records

# COMMAND ----------

def itemMasterRecordsModified(itemDeltaPath, Date, itemMasterList, itemMain, bottleDepositDeltaPath):
  itemMain = itemMain.select([c for c in itemMain.columns if c in itemMasterList])
  itemMain = quinn.with_some_columns_renamed(itemMain_promotable, itemMain_change_col_name)(itemMain)
  itemMain = itemMasterTransformation(itemMain)
  
#   bottleDeposit = spark.read.format('delta').load(bottleDepositDeltaPath)
#   itemMain = itemMain.join(bottleDeposit, [bottleDeposit.BOTTEL_DEPOSIT_ITM_ID == itemMain.TEMP_SCRTX_DET_LNK_NBR, bottleDeposit.BOTTEL_DEPOSIT_STORE == itemDeltaDf.SMA_DEST_STORE], how='left').select([col(xx) for xx in itemMain.columns] + ['BOTTEL_DEPOSIT_RTL_PRC'])
#   itemMain = itemMain.withColumn("SMA_BOTTLE_DEPOSIT_IND", when((col("BOTTEL_DEPOSIT_RTL_PRC").isNotNull()), lit('Y')).otherwise(lit('N')))
#   itemMain = itemMain.withColumn("BTL_DPST_AMT", col("BOTTEL_DEPOSIT_RTL_PRC")).
  itemMain = itemMain.withColumn("BTL_DPST_AMT", lit(None))
  itemMain = itemMain.drop(col("TEMP_SCRTX_DET_LNK_NBR")).drop(col("BOTTEL_DEPOSIT_RTL_PRC"))
  itemMainTemp = spark.sql('''DELETE FROM delta.`{}`'''.format(itemTempEffDeltaPath))
  itemMain.write.partitionBy('SMA_DEST_STORE').format('delta').mode('append').save(itemTempEffDeltaPath)
  ABC(itemMasterCount=itemMain.count())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Merging records with same effective date, store number and upc

# COMMAND ----------

def mergeItemRecords():
  ABC(mergeItemRecordsCheck = 1)
  loggerAtt.info("Combining records based on effective date")
  itemMain = spark.read.format('delta').load(itemDeltaPath)
  initial_recs = itemMain.count()
  loggerAtt.info(f"Initial count of records in Dataframe: {initial_recs}")
  ABC(mergeItemInitCount=initial_recs)
  itemMain = spark.sql('''SELECT 
                  fetchFirstFunction(collect_list(SCRTX_DET_PLU_BTCH_NBR)) as FIRST_SCRTX_DET_PLU_BTCH_NBR,
                  fetchFirstFunction(collect_list(SCRTX_DET_OP_CODE)) as FIRST_SCRTX_DET_OP_CODE,
                  fetchFirstFunction(collect_list(SCRTX_DET_STR_HIER_ID)) as FIRST_SCRTX_DET_STR_HIER_ID,
                  fetchFirstFunction(collect_list(SCRTX_DET_DFLT_RTN_LOC_ID)) as FIRST_SCRTX_DET_DFLT_RTN_LOC_ID,
                  fetchFirstFunction(collect_list(SCRTX_DET_MSG_CD)) as FIRST_SCRTX_DET_MSG_CD,
                  fetchFirstFunction(collect_list(SCRTX_DET_DSPL_DESCR)) as FIRST_SCRTX_DET_DSPL_DESCR,
                  fetchFirstFunction(collect_list(SCRTX_DET_SLS_RESTRICT_GRP)) as FIRST_SCRTX_DET_SLS_RESTRICT_GRP,
                  fetchFirstFunction(collect_list(SCRTX_DET_RCPT_DESCR)) as FIRST_SCRTX_DET_RCPT_DESCR,
                  fetchFirstFunction(collect_list(SCRTX_DET_TAXABILITY_CD)) as FIRST_SCRTX_DET_TAXABILITY_CD,
                  fetchFirstFunction(collect_list(SCRTX_DET_MDSE_XREF_ID)) as FIRST_SCRTX_DET_MDSE_XREF_ID,
                  fetchFirstFunction(collect_list(SCRTX_DET_NON_MDSE_ID)) as FIRST_SCRTX_DET_NON_MDSE_ID,
                  fetchFirstFunction(collect_list(SCRTX_DET_UOM)) as FIRST_SCRTX_DET_UOM,
                  fetchFirstFunction(collect_list(SCRTX_DET_UNT_QTY)) as FIRST_SCRTX_DET_UNT_QTY,
                  fetchFirstFunction(collect_list(SCRTX_DET_LIN_ITM_CD)) as FIRST_SCRTX_DET_LIN_ITM_CD,
                  fetchFirstFunction(collect_list(SCRTX_DET_MD_FG)) as FIRST_SCRTX_DET_MD_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_QTY_RQRD_FG)) as FIRST_SCRTX_DET_QTY_RQRD_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_SUBPRD_CNT)) as FIRST_SCRTX_DET_SUBPRD_CNT,
                  fetchFirstFunction(collect_list(SCRTX_DET_QTY_ALLOWED_FG)) as FIRST_SCRTX_DET_QTY_ALLOWED_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_SLS_AUTH_FG)) as FIRST_SCRTX_DET_SLS_AUTH_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_FOOD_STAMP_FG)) as FIRST_SCRTX_DET_FOOD_STAMP_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_WIC_FG)) as FIRST_SCRTX_DET_WIC_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_PERPET_INV_FG)) as FIRST_SCRTX_DET_PERPET_INV_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_RTL_PRC)) as FIRST_SCRTX_DET_RTL_PRC,
                  fetchFirstFunction(collect_list(SCRTX_DET_UNT_CST)) as FIRST_SCRTX_DET_UNT_CST,
                  fetchFirstFunction(collect_list(SCRTX_DET_MAN_PRC_LVL)) as FIRST_SCRTX_DET_MAN_PRC_LVL,
                  fetchFirstFunction(collect_list(SCRTX_DET_MIN_MDSE_AMT)) as FIRST_SCRTX_DET_MIN_MDSE_AMT,
                  fetchFirstFunction(collect_list(SCRTX_DET_RTL_PRC_DATE)) as FIRST_SCRTX_DET_RTL_PRC_DATE,
                  fetchFirstFunction(collect_list(SCRTX_DET_SERIAL_MDSE_FG)) as FIRST_SCRTX_DET_SERIAL_MDSE_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_CNTR_PRC)) as FIRST_SCRTX_DET_CNTR_PRC,
                  fetchFirstFunction(collect_list(SCRTX_DET_MAX_MDSE_AMT)) as FIRST_SCRTX_DET_MAX_MDSE_AMT,
                  fetchFirstFunction(collect_list(SCRTX_DET_CNTR_PRC_DATE)) as FIRST_SCRTX_DET_CNTR_PRC_DATE,
                  fetchFirstFunction(collect_list(SCRTX_DET_NG_ENTRY_FG)) as FIRST_SCRTX_DET_NG_ENTRY_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_STR_CPN_FG)) as FIRST_SCRTX_DET_STR_CPN_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_VEN_CPN_FG)) as FIRST_SCRTX_DET_VEN_CPN_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_MAN_PRC_FG)) as FIRST_SCRTX_DET_MAN_PRC_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_WGT_ITM_FG)) as FIRST_SCRTX_DET_WGT_ITM_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_NON_DISC_FG)) as FIRST_SCRTX_DET_NON_DISC_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_COST_PLUS_FG)) as FIRST_SCRTX_DET_COST_PLUS_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_PRC_VRFY_FG)) as FIRST_SCRTX_DET_PRC_VRFY_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_PRC_OVRD_FG)) as FIRST_SCRTX_DET_PRC_OVRD_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_SPLR_PROM_FG)) as FIRST_SCRTX_DET_SPLR_PROM_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_SAVE_DISC_FG)) as FIRST_SCRTX_DET_SAVE_DISC_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_ITM_ONSALE_FG)) as FIRST_SCRTX_DET_ITM_ONSALE_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_INHBT_QTY_FG)) as FIRST_SCRTX_DET_INHBT_QTY_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_DCML_QTY_FG)) as FIRST_SCRTX_DET_DCML_QTY_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_SHELF_LBL_RQRD_FG)) as FIRST_SCRTX_DET_SHELF_LBL_RQRD_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_TAX_RATE1_FG)) as FIRST_SCRTX_DET_TAX_RATE1_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_TAX_RATE2_FG)) as FIRST_SCRTX_DET_TAX_RATE2_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_TAX_RATE3_FG)) as FIRST_SCRTX_DET_TAX_RATE3_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_TAX_RATE4_FG)) as FIRST_SCRTX_DET_TAX_RATE4_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_TAX_RATE5_FG)) as FIRST_SCRTX_DET_TAX_RATE5_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_TAX_RATE6_FG)) as FIRST_SCRTX_DET_TAX_RATE6_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_TAX_RATE7_FG)) as FIRST_SCRTX_DET_TAX_RATE7_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_TAX_RATE8_FG)) as FIRST_SCRTX_DET_TAX_RATE8_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_COST_CASE_PRC)) as FIRST_SCRTX_DET_COST_CASE_PRC,
                  fetchFirstFunction(collect_list(SCRTX_DET_DATE_COST_CASE_PRC)) as FIRST_SCRTX_DET_DATE_COST_CASE_PRC,
                  fetchFirstFunction(collect_list(SCRTX_DET_UNIT_CASE)) as FIRST_SCRTX_DET_UNIT_CASE,
                  fetchFirstFunction(collect_list(SCRTX_DET_MIX_MATCH_CD)) as FIRST_SCRTX_DET_MIX_MATCH_CD,
                  fetchFirstFunction(collect_list(SCRTX_DET_RTN_CD)) as FIRST_SCRTX_DET_RTN_CD,
                  fetchFirstFunction(collect_list(SCRTX_DET_FAMILY_CD)) as FIRST_SCRTX_DET_FAMILY_CD,
                  fetchFirstFunction(collect_list(SCRTX_DET_SUBDEP_ID)) as FIRST_SCRTX_DET_SUBDEP_ID,
                  fetchFirstFunction(collect_list(SCRTX_DET_DISC_CD)) as FIRST_SCRTX_DET_DISC_CD,
                  fetchFirstFunction(collect_list(SCRTX_DET_LBL_QTY)) as FIRST_SCRTX_DET_LBL_QTY,
                  fetchFirstFunction(collect_list(SCRTX_DET_SCALE_FG)) as FIRST_SCRTX_DET_SCALE_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_LOCAL_DEL_FG)) as FIRST_SCRTX_DET_LOCAL_DEL_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_HOST_DEL_FG)) as FIRST_SCRTX_DET_HOST_DEL_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_HEAD_OFFICE_DEP)) as FIRST_SCRTX_DET_HEAD_OFFICE_DEP,
                  fetchFirstFunction(collect_list(SCRTX_DET_WGT_SCALE_FG)) as FIRST_SCRTX_DET_WGT_SCALE_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_FREQ_SHOP_TYPE)) as FIRST_SCRTX_DET_FREQ_SHOP_TYPE,
                  fetchFirstFunction(collect_list(SCRTX_DET_FREQ_SHOP_VAL)) as FIRST_SCRTX_DET_FREQ_SHOP_VAL,
                  fetchFirstFunction(collect_list(SCRTX_DET_SEC_FAMILY)) as FIRST_SCRTX_DET_SEC_FAMILY,
                  fetchFirstFunction(collect_list(SCRTX_DET_POS_MSG)) as FIRST_SCRTX_DET_POS_MSG,
                  fetchFirstFunction(collect_list(SCRTX_DET_SHELF_LIFE_DAY)) as FIRST_SCRTX_DET_SHELF_LIFE_DAY,
                  fetchFirstFunction(collect_list(SCRTX_DET_PROM_NBR)) as FIRST_SCRTX_DET_PROM_NBR,
                  fetchFirstFunction(collect_list(SCRTX_DET_BCKT_NBR)) as FIRST_SCRTX_DET_BCKT_NBR,
                  fetchFirstFunction(collect_list(SCRTX_DET_EXTND_PROM_NBR)) as FIRST_SCRTX_DET_EXTND_PROM_NBR,
                  fetchFirstFunction(collect_list(SCRTX_DET_EXTND_BCKT_NBR)) as FIRST_SCRTX_DET_EXTND_BCKT_NBR,
                  fetchFirstFunction(collect_list(SCRTX_DET_RCPT_DESCR1)) as FIRST_SCRTX_DET_RCPT_DESCR1,
                  fetchFirstFunction(collect_list(SCRTX_DET_RCPT_DESCR2)) as FIRST_SCRTX_DET_RCPT_DESCR2,
                  fetchFirstFunction(collect_list(SCRTX_DET_RCPT_DESCR3)) as FIRST_SCRTX_DET_RCPT_DESCR3,
                  fetchFirstFunction(collect_list(SCRTX_DET_RCPT_DESCR4)) as FIRST_SCRTX_DET_RCPT_DESCR4,
                  fetchFirstFunction(collect_list(SCRTX_DET_TAR_WGT_NBR)) as FIRST_SCRTX_DET_TAR_WGT_NBR,
                  fetchFirstFunction(collect_list(SCRTX_DET_RSTRCT_LAYOUT)) as FIRST_SCRTX_DET_RSTRCT_LAYOUT,
                  fetchFirstFunction(collect_list(SCRTX_DET_INTRNL_ID)) as FIRST_SCRTX_DET_INTRNL_ID,
                  fetchFirstFunction(collect_list(SCRTX_DET_OLD_PRC)) as FIRST_SCRTX_DET_OLD_PRC,
                  fetchFirstFunction(collect_list(SCRTX_DET_QDX_FREQ_SHOP_VAL)) as FIRST_SCRTX_DET_QDX_FREQ_SHOP_VAL,
                  fetchFirstFunction(collect_list(SCRTX_DET_VND_ID)) as FIRST_SCRTX_DET_VND_ID,
                  fetchFirstFunction(collect_list(SCRTX_DET_VND_ITM_ID)) as FIRST_SCRTX_DET_VND_ITM_ID,
                  fetchFirstFunction(collect_list(SCRTX_DET_VND_ITM_SZ)) as FIRST_SCRTX_DET_VND_ITM_SZ,
                  fetchFirstFunction(collect_list(SCRTX_DET_CMPRTV_UOM)) as FIRST_SCRTX_DET_CMPRTV_UOM,
                  fetchFirstFunction(collect_list(SCRTX_DET_CMPR_QTY)) as FIRST_SCRTX_DET_CMPR_QTY,
                  fetchFirstFunction(collect_list(SCRTX_DET_CMPR_UNT)) as FIRST_SCRTX_DET_CMPR_UNT,
                  fetchFirstFunction(collect_list(SCRTX_DET_BNS_CPN_FG)) as FIRST_SCRTX_DET_BNS_CPN_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_EX_MIN_PURCH_FG)) as FIRST_SCRTX_DET_EX_MIN_PURCH_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_FUEL_FG)) as FIRST_SCRTX_DET_FUEL_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_SPR_AUTH_RQRD_FG)) as FIRST_SCRTX_DET_SPR_AUTH_RQRD_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_SSP_PRDCT_FG)) as FIRST_SCRTX_DET_SSP_PRDCT_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_NU06_FG)) as FIRST_SCRTX_DET_NU06_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_NU07_FG)) as FIRST_SCRTX_DET_NU07_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_NU08_FG)) as FIRST_SCRTX_DET_NU08_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_NU09_FG)) as FIRST_SCRTX_DET_NU09_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_NU10_FG)) as FIRST_SCRTX_DET_NU10_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_FREQ_SHOP_LMT)) as FIRST_SCRTX_DET_FREQ_SHOP_LMT,
                  fetchFirstFunction(collect_list(SCRTX_DET_ITM_STATUS)) as FIRST_SCRTX_DET_ITM_STATUS,
                  fetchFirstFunction(collect_list(SCRTX_DET_DEA_GRP)) as FIRST_SCRTX_DET_DEA_GRP,
                  fetchFirstFunction(collect_list(SCRTX_DET_BNS_BY_OPCODE)) as FIRST_SCRTX_DET_BNS_BY_OPCODE,
                  fetchFirstFunction(collect_list(SCRTX_DET_BNS_BY_DESCR)) as FIRST_SCRTX_DET_BNS_BY_DESCR,
                  fetchFirstFunction(collect_list(SCRTX_DET_COMP_TYPE)) as FIRST_SCRTX_DET_COMP_TYPE,
                  fetchFirstFunction(collect_list(SCRTX_DET_COMP_PRC)) as FIRST_SCRTX_DET_COMP_PRC,
                  fetchFirstFunction(collect_list(SCRTX_DET_COMP_QTY)) as FIRST_SCRTX_DET_COMP_QTY,
                  fetchFirstFunction(collect_list(SCRTX_DET_ASSUME_QTY_FG)) as FIRST_SCRTX_DET_ASSUME_QTY_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_EXCISE_TAX_NBR)) as FIRST_SCRTX_DET_EXCISE_TAX_NBR,
                  fetchFirstFunction(collect_list(SCRTX_DET_RTL_PRICE_DATE)) as FIRST_SCRTX_DET_RTL_PRICE_DATE,
                  fetchFirstFunction(collect_list(SCRTX_DET_PRC_RSN_ID)) as FIRST_SCRTX_DET_PRC_RSN_ID,
                  fetchFirstFunction(collect_list(SCRTX_DET_ITM_POINT)) as FIRST_SCRTX_DET_ITM_POINT,
                  fetchFirstFunction(collect_list(SCRTX_DET_PRC_GRP_ID)) as FIRST_SCRTX_DET_PRC_GRP_ID,
                  fetchFirstFunction(collect_list(SCRTX_DET_SWW_CODE_FG)) as FIRST_SCRTX_DET_SWW_CODE_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_SHELF_STOCK_FG)) as FIRST_SCRTX_DET_SHELF_STOCK_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_PRT_PLUID_RCPT_FG)) as FIRST_SCRTX_DET_PRT_PLUID_RCPT_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_BLK_GRP)) as FIRST_SCRTX_DET_BLK_GRP,
                  fetchFirstFunction(collect_list(SCRTX_DET_EXCHNGE_TENDER_ID)) as FIRST_SCRTX_DET_EXCHNGE_TENDER_ID,
                  fetchFirstFunction(collect_list(SCRTX_DET_CAR_WASH_FG)) as FIRST_SCRTX_DET_CAR_WASH_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_EXMPT_FRM_PROM_FG)) as FIRST_SCRTX_DET_EXMPT_FRM_PROM_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_QSR_ITM_TYP)) as FIRST_SCRTX_DET_QSR_ITM_TYP,
                  fetchFirstFunction(collect_list(SCRTX_DET_RSTRCSALE_BRCD_FG)) as FIRST_SCRTX_DET_RSTRCSALE_BRCD_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_NON_RX_HEALTH_FG)) as FIRST_SCRTX_DET_NON_RX_HEALTH_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_RX_FG)) as FIRST_SCRTX_DET_RX_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_LNK_NBR)) as FIRST_SCRTX_DET_LNK_NBR,
                  fetchFirstFunction(collect_list(SCRTX_DET_WIC_CVV_FG)) as FIRST_SCRTX_DET_WIC_CVV_FG,
                  fetchFirstFunction(collect_list(SCRTX_DET_CENTRAL_ITEM)) as FIRST_SCRTX_DET_CENTRAL_ITEM,
                  fetchFirstFunction(collect_list(BANNER_ID)) as FIRST_BANNER_ID,
                  fetchFirstFunction(collect_list(SCRTX_HDR_ACT_DATE)) as FIRST_SCRTX_HDR_ACT_DATE,
                  fetchFirstFunction(collect_list(INSERT_ID)) as FIRST_INSERT_ID,
                  fetchFirstFunction(collect_list(INSERT_TIMESTAMP)) as FIRST_INSERT_TIMESTAMP,
                  fetchFirstFunction(collect_list(LAST_UPDATE_ID)) as FIRST_LAST_UPDATE_ID,
                  fetchFirstFunction(collect_list(LAST_UPDATE_TIMESTAMP)) as FIRST_LAST_UPDATE_TIMESTAMP,
                  fetchFirstFunction(collect_list(COUPON_NO)) as FIRST_COUPON_NO,
                  fetchFirstFunction(collect_list(RTX_BATCH)) as FIRST_RTX_BATCH,
                  fetchFirstFunction(collect_list(RTX_TYPE)) as FIRST_RTX_TYPE,
                  fetchFirstFunction(collect_list(RTX_UPC)) as FIRST_RTX_UPC,
                  fetchFirstFunction(collect_list(RTX_LOAD)) as FIRST_RTX_LOAD,
                  RTX_STORE,
                  SCRTX_DET_ITM_ID
                  FROM (select * from delta.`{}` where SCRTX_HDR_ACT_DATE<=to_date(current_date()) order by SCRTX_HDR_ACT_DATE desc, LAST_UPDATE_TIMESTAMP desc, RowNumber desc) as hdrOrdered
                  GROUP BY RTX_STORE,SCRTX_DET_ITM_ID'''.format(itemDeltaPath))
  after_recs = itemMain.count()
  loggerAtt.info(f"After count of records in Dataframe: {after_recs}")
  ABC(mergeItemafterCount=after_recs)
#   spark.catalog.dropTempView(temp_table_name)
  loggerAtt.info("Combining records based on effective date successful")  
  return itemMain

#                   fetchFirstFunction(collect_list(SCRTX_DET_CPN_NBR)) as FIRST_SCRTX_DET_CPN_NBR, not used in item delta table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Main file processing

# COMMAND ----------

if __name__ == "__main__":
  loggerAtt.info('======== Input Product file processing initiated ========')
  ## Step 1: Reading input file
  try:
    posRawDf = readFileHeader(file_location, infer_schema, first_row_is_header, delimiterHeader, file_type)
    
  except Exception as ex:
    if 'java.io.FileNotFoundException' in str(ex):
      loggerAtt.error('File does not exists')
    else:
      loggerAtt.error(ex)
    ABC(ReadDataCheck=0)
    ABC(RawDataCount="")
    err = ErrorReturn('Error', ex,'readFileHeader')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
  ## Step 2: Checking if the row numbers have null values
  try:
    ABC(NullValueCheck=1)
    if (posRawDf.where(col("RowNumber").isNull()).count()>0):
      raise Exception('Null records in RowNumber of the input file')
  except Exception as ex:
    ABC(NullValueCheck=0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'Row Number Null check')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

  if posRawDf is not None:
    ## Step 3: Fetching Different Header records with start and end row number
    try:
      promoDf, linkDf, storeDf = fetchHeaderLinkPromo(posRawDf)
    except Exception as ex:
      loggerAtt.error(ex)
      ABC(fetchHeaderLinkPromoCheck=0)
      ABC(allHeaderCount='')
      ABC(storeDfCount='')
      ABC(promoHeaderDfCount='')
      ABC(linkHeaderDfCount='')
      ABC(linkDfCount='')
      ABC(promoDfCount='')
      err = ErrorReturn('Error', ex,'fetchHeaderLinkPromo')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
    
    ## Step 4: Fetching Link records from dataframe
    try:  
      ABC(NullValueCheck=1)
      ABC(DropNACheck = 1)
      if linkDf is not None: 
        if linkDf.count() > 0:
          linkDf = fetchLinkRecords(linkDf)
    except Exception as ex:
      ABC(NullValueCheck=0)
      ABC(DropNACheck = 0)
      ABC(NullValueLinkCount='')
      ABC(LinkCount = '')
      err = ErrorReturn('Error', ex,'fetchLinkRecords')
      loggerAtt.error(ex)
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
    
    ## Step 5: Fetching Promo records from dataframe
    try:  
      ABC(NullValueCheck=1)
      ABC(DropNACheck = 1)
      if promoDf is not None: 
        if promoDf.count() > 0:
          promoDf = fetchPromoRecords(promoDf)
    except Exception as ex:
      ABC(NullValueCheck=0)
      ABC(DropNACheck = 0)
      ABC(NullValuePromoCount='')
      ABC(PromoCount = '')
      err = ErrorReturn('Error', ex,'fetchPromoRecords')
      loggerAtt.error(ex)
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
    ## Step 6: Perform Link/Promo Transformation
    try:  
      ABC(RenamingCheck=1)
      ABC(InvalidRecordSaveCheck = 1)
      ABC(TransformationCheck=1)
      if promoDf is not None and linkDf is not None:
        promoDf, linkDf = promoLinkTransformation(promoDf, linkDf)
    except Exception as ex:
      ABC(TransformationCheck=0)
      ABC(InvalidRecordSaveCheck = 0)
      ABC(invalidTransCount='')
      err = ErrorReturn('Error', ex,'promoLinkTransformation')
      loggerAtt.error(ex)
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
    ## Step 7: Insert/Update/Delete Item and Coupon Link UPC
    if linkDf is not None:
      if linkDf.count() > 0:
        ABC(couponItemPromoLinkCheck=1)
        updateItemPromoLinkRecords(linkDf)

    ## Step 8: Remove coupon delete records greater then 7 days and move it to archive
    try:
      ABC(archivalCheck = 1)
      if processing_file =='Delta':
        couponArchival(couponDeltaPath,Date,couponArchivalpath)
    except Exception as ex:
      ABC(archivalCheck = 0)
      ABC(archivalInitCount='')
      ABC(archivalAfterCount='')
      err = ErrorReturn('Error', ex,'posArchival')
      loggerAtt.error(ex)
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
    ## Step 9: Insert/Update/Delete Coupon delta table
    ABC(DeltaTableCreateCheck = 1)
    if promoDf is not None:
      if promoDf.count() > 0:
        updateInsertCouponRecords(promoDf)
    ## Step 10: Write Coupon output file
    try:
      ABC(couponWriteCheck=1)
      couponWrite(couponDeltaPath,couponOutboundPath,list(storeDf.select('Store_id').toPandas()['Store_id']))
    except Exception as ex:
      ABC(couponOutputFileCount='')
      ABC(couponWriteCheck=0)
      err = ErrorReturn('Error', ex,'couponWrite')
      loggerAtt.error(ex)
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
    ## Step 11: Modify Item Main table for coupon with status change as D
    if promoDf is not None:
      if promoDf.count() > 0:
        ABC(couponDeleteItemCheck=1)
        updateItemCouponDeleteRecords(promoDf)
        
    ## Step 12: Grouping records based on effective date, Store number and item id
    try:
      itemMain = mergeItemRecords()
    except Exception as ex:
      loggerAtt.info("Combining records based on effective date failed and throwed error")
      loggerAtt.error(str(ex))
      ABC(mergeItemRecordsCheck = 0)
      ABC(mergeItemInitCount='')
      ABC(mergeItemafterCount='')
      err = ErrorReturn('Error', ex,'mergeItemRecords')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
    
    ## Step 13: Renaming columns to original column names
    try:
      ABC(renameCheck = 1)
      itemMain = quinn.with_some_columns_renamed(itemEff_promotable, itemEff_change_col_name)(itemMain)
      itemMain = hdrDataTypeChgTransformation(itemMain)
    except Exception as ex:
      ABC(renameCheck = 0)
      loggerAtt.error(str(ex))
      err = ErrorReturn('Error', ex,'Error in renaming column to orginal column name')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
    
    ## Step 14: Create Final Item Main temp table for Unified Item records
    
    try:
      ABC(itemMasterRecordsCheck=1)
      itemMasterRecordsModified(itemDeltaPath, Date, itemMasterList, itemMain, bottleDepositDeltaPath)
    except Exception as ex:
      ABC(itemMasterCount='')
      ABC(itemMasterRecordsCheck=0)
      err = ErrorReturn('Error', ex,'itemMasterRecordsModified')
      loggerAtt.error(ex)
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
  else:
    loggerAtt.info("Error in input file reading")
    
loggerAtt.info('======== Input product file processing ended ========')

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Writing log file to ADLS location

# COMMAND ----------

dbutils.fs.mv("file:"+p_logfile, logFilesPath+"/"+ custom_logfile_Name + file_date + '.log')
loggerAtt.info('======== Log file is updated at ADLS Location ========')
logging.shutdown()
err = ErrorReturn('Success', '','')
errJson = jsonpickle.encode(err)
errJson = json.loads(errJson)
Merge(ABCChecks,errJson)
dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

