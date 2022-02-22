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

# MAGIC %sql
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = False;
# MAGIC SET TIME ZONE 'America/Halifax';

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##Calling Logger

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/Logging

# COMMAND ----------

custom_logfile_Name ='item_dailymaintainence_customlog'
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
    self.time = datetime.datetime.now(timezone("America/Halifax")).isoformat()
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
dbutils.widgets.text("directory","")
dbutils.widgets.text("outputDirectory","")
dbutils.widgets.text("Container","")
dbutils.widgets.text("pipelineID","")
dbutils.widgets.text("MountPoint","")
dbutils.widgets.text("deltaPath","")
dbutils.widgets.text("archivalFilePath","")
dbutils.widgets.text("logFilesPath","")
dbutils.widgets.text("invalidRecordsPath","")
dbutils.widgets.text("clientId","")
dbutils.widgets.text("keyVaultName","")
dbutils.widgets.text("promoLinkingDeltaPath","")
dbutils.widgets.text("itemMasterOutboundPath","")
dbutils.widgets.text("couponDeltaPath","")
dbutils.widgets.text("promoLinkingMainDeltaPath","")
dbutils.widgets.text("priceChangeDeltaPath","")

FileName=dbutils.widgets.get("fileName")
Filepath=dbutils.widgets.get("filePath")
inputDirectory=dbutils.widgets.get("directory")
outputDirectory=dbutils.widgets.get("outputDirectory")
container=dbutils.widgets.get("Container")
PipelineID=dbutils.widgets.get("pipelineID")
mount_point=dbutils.widgets.get("MountPoint")
ItemMainDeltaPath=dbutils.widgets.get("deltaPath")
Archivalfilelocation=dbutils.widgets.get("archivalFilePath")
Item_OutboundPath=dbutils.widgets.get("itemMasterOutboundPath")
Log_FilesPath=dbutils.widgets.get("logFilesPath")
Invalid_RecordsPath=dbutils.widgets.get("invalidRecordsPath")
Date = datetime.datetime.now(timezone("America/Halifax")).strftime("%Y-%m-%d")
file_location = '/mnt' + '/' + inputDirectory + '/' + Filepath +'/' + FileName 
clientId=dbutils.widgets.get("clientId")
keyVaultName=dbutils.widgets.get("keyVaultName")
PromotionLinkDeltaPath = dbutils.widgets.get("promoLinkingDeltaPath")
couponDeltaPath = dbutils.widgets.get("couponDeltaPath")
PromotionMainDeltaPath = dbutils.widgets.get("promoLinkingMainDeltaPath")
PriceChangeDeltaPath = dbutils.widgets.get("priceChangeDeltaPath")

Date_serial = datetime.datetime.now(timezone("America/Halifax")).strftime("%Y%m%d")

promoErrorPath = Archivalfilelocation + "/" +Date+ "/" + "promoErrorData"

productRecallErrorPath = Archivalfilelocation + "/" +Date+ "/" + "productRecallErrorData"

invalidRecordsPath = Archivalfilelocation + "/" +Date+ "/" + "invalidRecords"

promoLinkTemp = Archivalfilelocation + '/promoLinkTemp'


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

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Mounting ADLS location

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/Mount_Point_Creation

# COMMAND ----------

try:
  source= 'abfss://' + inputDirectory + '@' + container + '.dfs.core.windows.net/'
  mounting(mount_point, source, clientId, keyVaultName)
  ABC(MountCheck=1)
except Exception as ex:
  # send error message to ADF and send email notification
  ABC(MountCheck=0)
  loggerAtt.error(str(ex))
  err = ErrorReturn('Error', ex,'Mounting Input Source')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
try:
  source= 'abfss://' + outputDirectory + '@' + container + '.dfs.core.windows.net/'
  mounting(mount_point, source, clientId, keyVaultName)
except Exception as ex:
  # send error message to ADF and send email notification
  ABC(MountCheck=0)
  loggerAtt.error(str(ex))
  err = ErrorReturn('Error', ex,'Mounting Output Source')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Declaration

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Field Declaration

# COMMAND ----------

file_type = "csv"
infer_schema = "false"
first_row_is_header = "true"
delimiter = "|"
item_raw_df = None
item_duplicate_records = None
invalid_transformed_df = None
item_productrecall = None
nonItemRecallFile = None
item_nomaintainence = None
recall_error_df =None
PipelineID= str(PipelineID)

p_filename = "item_dailymaintainence_custom_log"
folderDate = Date

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Item field names

# COMMAND ----------

Item_List = ['SMA_STORE', 'SMA_GTIN_NUM', 'ITEM_PARENT', 'SMA_SUB_DEPT', 'GROUP_NO', 'SMA_COMM', 'SMA_SUB_COMM', 'SMA_SEGMENT', 'SMA_ITEM_DESC', 'SMA_ITEM_MVMT', 'SMA_ITEM_POG_COMM', 'SMA_UPC_DESC', 'SMA_RETL_MULT_UOM', 'SMA_SBW_IND', 'SMA_PRIME_UPC', 'SMA_ITEM_SIZE', 'SMA_ITEM_UOM', 'SMA_ITEM_BRAND', 'SMA_ITEM_TYPE', 'SMA_VEND_COUPON_FAM1', 'SMA_VEND_COUPON_FAM2', 'SMA_FSA_IND', 'SMA_NAT_ORG_IND', 'SMA_NEW_ITMUPC', 'ATC', 'SMA_PRIV_LABEL','SMA_TAG_SZ', 'SMA_FUEL_PROD_CATEG', 'SMA_CONTRIB_QTY', 'SMA_CONTRIB_QTY_UOM', 'SMA_GAS_IND', 'SMA_ITEM_STATUS', 'SMA_STATUS_DATE', 'SMA_VEND_NUM', 'SMA_SOURCE_METHOD', 'SMA_SOURCE_WHSE', 'SMA_RECV_TYPE', 'SMA_MEAS_OF_EACH', 'SMA_MEAS_OF_EACH_WIP', 'SMA_MEAS_OF_PRICE', 'SMA_MKT_AREA', 'SMA_UOM_OF_PRICE', 'SMA_VENDOR_PACK', 'SMA_SHELF_TAG_REQ', 'SMA_QTY_KEY_OPTIONS', 'SMA_MANUAL_PRICE_ENTRY', 'SMA_FOOD_STAMP_IND', 'SMA_HIP_IND', 'SMA_WIC_IND', 'SMA_TARE_PCT', 'SMA_FIXED_TARE_WGT', 'SMA_TARE_UOM', 'SMA_POINTS_ELIGIBLE', 'SMA_RECALL_FLAG', 'SMA_TAX_CHG_IND', 'SMA_RESTR_CODE', 'SMA_WIC_SHLF_MIN', 'SMA_WIC_ALT', 'SMA_TAX_1', 'SMA_TAX_2', 'SMA_TAX_3', 'SMA_TAX_4', 'SMA_TAX_5', 'SMA_TAX_6', 'SMA_TAX_7', 'SMA_TAX_8', 'SMA_DISCOUNTABLE_IND', 'SMA_CPN_MLTPY_IND', 'SMA_MULT_UNIT_RETL', 'SMA_RETL_MULT_UNIT', 'SMA_LINK_UPC', 'SMA_ACTIVATION_CODE', 'SMA_GLUTEN_FREE', 'SMA_NON_PRICED_ITEM', 'SMA_ORIG_CHG_TYPE', 'SMA_ORIG_LIN', 'SMA_ORIG_VENDOR', 'SMA_IFPS_CODE', 'SMA_IN_STORE_TAG_PRINT', 'SMA_BATCH_SERIAL_NBR', 'SMA_POS_REQUIRED', 'SMA_TAG_REQUIRED', 'SMA_UPC_OVERRIDE_GRP_NUM', 'SMA_TENDER_RESTRICTION_CODE', 'SMA_REFUND_RECEIPT_EXCLUSION', 'SMA_FEE_ITEM_TYPE', 'SMA_FEE_TYPE', 'SMA_KOSHER_FLAG', 'SMA_LEGACY_ITEM', 'SMA_GUIDING_STARS', 'PROMO_MIN_REQ_QTY', 'PROMO_LIM_VAL', 'PROMO_BUY_QTY', 'PROMO_GET_QTY', 'HOW_TO_SELL', 'BRAND_LOW', 'MANUFACTURER', 'INNER_PACK_SIZE', 'AVG_WGT', 'EFF_TS', 'INSERT_ID', 'INSERT_TIMESTAMP', 'LAST_UPDATE_ID', 'LAST_UPDATE_TIMESTAMP', 'SMA_COMP_ID', 'SMA_COMP_OBSERV_DATE', 'SMA_COMP_PRICE', 'SMA_EFF_DOW', 'SMA_CHG_TYPE', 'SMA_RETL_VENDOR', 'SMA_RETL_CHG_TYPE', 'SMA_BATCH_SUB_DEPT', 'SMA_BOTTLE_DEPOSIT_IND', 'SMA_DATA_TYPE', 'SMA_DEMANDTEC_PROMO_ID', 'SMA_DEST_STORE', 'SMA_EMERGY_UPDATE_IND', 'SMA_POS_SYSTEM', 'SMA_SMR_EFF_DATE', 'SMA_STORE_DIV', 'SMA_STORE_STATE', 'SMA_STORE_ZONE', 'SMA_TAX_CATEG', 'SMA_TIBCO_DATE', 'SMA_TRANSMIT_DATE', 'SMA_UNIT_PRICE_CODE', 'SMA_WPG', 'SMA_HLTHY_IDEAS'] 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Price Change field names

# COMMAND ----------

PriceChange_List = ['SMA_PRICE_CHG_ID','SMA_GTIN_NUM','SMA_STORE','LOCATION_TYPE','SELL_UNIT_CHG_IND','MLT_UNIT_CHG_IND','PRICECHANGE_STATUS','SELL_RETAIL_CURR','MLT_UNIT_CUR','PRICECHANGE_CUSTOM_FIELD_2','PRICECHANGE_CUSTOM_FIELD_3','SMA_ITM_EFF_DATE','SMA_SELL_RETL','SMA_RETL_UOM','SMA_MULT_UNIT_RETL','SMA_RETL_PRC_ENTRY_CHG','SMA_BATCH_SERIAL_NBR','INSERT_ID','INSERT_TIMESTAMP', 'SMA_RETL_MULT_UOM', 'SMA_RETL_MULT_UNIT']

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### PromotionLink field names

# COMMAND ----------

PromotionLink_List = ['SMA_PROMO_LINK_UPC', 'LOCATION_TYPE', 'SMA_ITM_EFF_DATE', 'SMA_LINK_HDR_COUPON', 'SMA_LINK_START_DATE', 'SMA_LINK_RCRD_TYPE', 'SMA_LINK_END_DATE', 'SMA_CHG_TYPE', 'SMA_LINK_CHG_TYPE', 'SMA_BATCH_SERIAL_NBR', 'SMA_LINK_HDR_MAINT_TYPE', 'SMA_LINK_TYPE', 'SMA_LINK_SYS_DIGIT', 'SMA_LINK_FAMCD_PROMOCD', 'SMA_STORE', 'INSERT_ID', 'INSERT_TIMESTAMP', 'LAST_UPDATE_ID', 'LAST_UPDATE_TIMESTAMP', 'SMA_LINK_HDR_LOCATION', 'SMA_LINK_ITEM_NBR', 'SMA_LINK_OOPS_ADWK', 'SMA_LINK_OOPS_FILE_ID', 'MERCH_TYPE']

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### PromotionLinkMain field names

# COMMAND ----------

PromotionLinkMain_List = ['SMA_PROMO_LINK_UPC', 'SMA_ITM_EFF_DATE', 'SMA_LINK_HDR_COUPON', 'SMA_LINK_APPLY_DATE', 'SMA_LINK_APPLY_TIME', 'SMA_LINK_END_DATE', 'SMA_CHG_TYPE', 'SMA_LINK_CHG_TYPE', 'SMA_STORE', 'SMA_LINK_HDR_LOCATION', 'SMA_BATCH_SERIAL_NBR','INSERT_ID','INSERT_TIMESTAMP']

# COMMAND ----------

# MAGIC %md
# MAGIC ### Code to determine the  type of file maintainence to be processed based on directory path

# COMMAND ----------

processing_file='Delta'
if Filepath.find('COD') !=-1:
  processing_file ='COD'
elif Filepath.find('Full')!=-1:
  processing_file ='FullItem'
elif Filepath.find('Productrecall')!=-1:
  processing_file ='Recall'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Item Schema definition

# COMMAND ----------

#Defining the schema for Coupon Table
loggerAtt.info('========Schema definition initiated ========')
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
loggerAtt.info('========Schema definition ended ========')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Delta table creation

# COMMAND ----------

# %sql

# delete from ItemMainAhold where SMA_STORE=0020;
# delete from PriceChange where SMA_STORE=0020;
# delete from PromotionLink where SMA_STORE=0020;
# delete from PromotionMain where SMA_STORE=0020;
# delete from itemMasterAhold where sma_store=0020;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Item Main

# COMMAND ----------

try:
    ABC(DeltaTableCreateCheck=1)
    spark.sql("""
    CREATE TABLE IF NOT EXISTS ItemMainAhold (
      SMA_STORE STRING,
      SMA_DEST_STORE STRING,
      SMA_GTIN_NUM LONG,
      ITEM_PARENT LONG,
      SMA_SUB_DEPT STRING,
      GROUP_NO STRING,
      SMA_COMM INTEGER,
      SMA_SUB_COMM INTEGER,
      SMA_DATA_TYPE STRING,
      SMA_DEMANDTEC_PROMO_ID STRING,
      SMA_BOTTLE_DEPOSIT_IND STRING,
      SMA_POS_SYSTEM STRING,
      SMA_SEGMENT STRING,
      SMA_HLTHY_IDEAS STRING,
      SMA_ITEM_DESC STRING,
      SMA_ITEM_MVMT STRING,
      SMA_ITEM_POG_COMM STRING,
      SMA_UPC_DESC STRING,
      SMA_RETL_MULT_UOM STRING,
      SMA_SBW_IND STRING,
      SMA_PRIME_UPC STRING,
      SMA_ITEM_SIZE STRING,
      SMA_ITEM_UOM STRING,
      SMA_ITEM_BRAND STRING,
      SMA_ITEM_TYPE STRING,
      SMA_VEND_COUPON_FAM1 INTEGER,
      SMA_VEND_COUPON_FAM2 INTEGER,
      SMA_FSA_IND STRING,
      SMA_NAT_ORG_IND STRING,
      SMA_NEW_ITMUPC STRING,
      ATC INTEGER,
      SMA_PRIV_LABEL STRING,
      SMA_TAG_SZ STRING,
      SMA_FUEL_PROD_CATEG STRING,
      SMA_CONTRIB_QTY STRING,
      SMA_CONTRIB_QTY_UOM STRING,
      SMA_GAS_IND STRING,
      SMA_ITEM_STATUS STRING,
      SMA_STATUS_DATE STRING,
      SMA_VEND_NUM INTEGER,
      SMA_SOURCE_METHOD STRING,
      SMA_SOURCE_WHSE STRING,
      SMA_RECV_TYPE STRING,
      SMA_MEAS_OF_EACH STRING,
      SMA_MEAS_OF_EACH_WIP STRING,
      SMA_MEAS_OF_PRICE STRING,
      SMA_MKT_AREA STRING,
      SMA_UOM_OF_PRICE STRING,
      SMA_VENDOR_PACK INTEGER,
      SMA_SHELF_TAG_REQ STRING,
      SMA_QTY_KEY_OPTIONS STRING,
      SMA_MANUAL_PRICE_ENTRY STRING,
      SMA_FOOD_STAMP_IND STRING,
      SMA_HIP_IND STRING,
      SMA_WIC_IND STRING,
      SMA_TARE_PCT STRING,
      SMA_FIXED_TARE_WGT STRING,
      SMA_TARE_UOM STRING,
      SMA_POINTS_ELIGIBLE STRING,
      SMA_RECALL_FLAG STRING,
      SMA_TAX_CHG_IND STRING,
      SMA_RESTR_CODE STRING,
      SMA_WIC_SHLF_MIN STRING,
      SMA_WIC_ALT STRING,
      SMA_TAX_1 STRING,
      SMA_TAX_2 STRING,
      SMA_TAX_3 STRING,
      SMA_TAX_4 STRING,
      SMA_TAX_5 STRING,
      SMA_TAX_6 STRING,
      SMA_TAX_7 STRING,
      SMA_TAX_8 STRING,
      SMA_DISCOUNTABLE_IND STRING,
      SMA_CPN_MLTPY_IND STRING,
      SMA_MULT_UNIT_RETL FLOAT,
      SMA_RETL_MULT_UNIT FLOAT,
      SMA_LINK_UPC LONG,
      SMA_ACTIVATION_CODE STRING,
      SMA_GLUTEN_FREE STRING,
      SMA_NON_PRICED_ITEM STRING,
      SMA_ORIG_CHG_TYPE INTEGER,
      SMA_ORIG_LIN STRING,
      SMA_ORIG_VENDOR STRING,
      SMA_IFPS_CODE STRING,
      SMA_IN_STORE_TAG_PRINT STRING,
      SMA_BATCH_SERIAL_NBR LONG,
      SMA_BATCH_SUB_DEPT LONG,
      SMA_EFF_DOW STRING,
      SMA_SMR_EFF_DATE STRING,
      SMA_STORE_DIV STRING,
      SMA_STORE_STATE STRING,
      SMA_STORE_ZONE STRING,
      SMA_TAX_CATEG STRING,
      SMA_TIBCO_DATE STRING,
      SMA_TRANSMIT_DATE STRING,
      SMA_UNIT_PRICE_CODE STRING,
      SMA_WPG STRING,
      SMA_EMERGY_UPDATE_IND STRING,
      SMA_CHG_TYPE INTEGER,
      SMA_RETL_VENDOR STRING,
      SMA_POS_REQUIRED STRING,
      SMA_TAG_REQUIRED STRING,
      SMA_UPC_OVERRIDE_GRP_NUM STRING,
      SMA_TENDER_RESTRICTION_CODE STRING,
      SMA_REFUND_RECEIPT_EXCLUSION STRING,
      SMA_FEE_ITEM_TYPE STRING,
      SMA_FEE_TYPE STRING,
      SMA_KOSHER_FLAG STRING,
      SMA_LEGACY_ITEM STRING,
      SMA_GUIDING_STARS INTEGER,
      SMA_COMP_ID STRING,
      SMA_COMP_OBSERV_DATE STRING,
      SMA_COMP_PRICE STRING,
      SMA_RETL_CHG_TYPE STRING,      
      PROMO_MIN_REQ_QTY STRING,
      PROMO_LIM_VAL STRING,
      PROMO_BUY_QTY STRING,
      PROMO_GET_QTY STRING,
      HOW_TO_SELL STRING,
      BRAND_LOW STRING,
      MANUFACTURER STRING,
      INNER_PACK_SIZE STRING,
      AVG_WGT STRING,
      EFF_TS TIMESTAMP,
      INSERT_ID STRING,
      INSERT_TIMESTAMP TIMESTAMP,
      LAST_UPDATE_ID STRING,
      LAST_UPDATE_TIMESTAMP TIMESTAMP
    )
    USING delta
    Location '{}'
    PARTITIONED BY (SMA_STORE)
    """.format(ItemMainDeltaPath))
except Exception as ex:
    ABC(DeltaTableCreateCheck = 0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'deltaCreator')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson)) 


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Price Change Table

# COMMAND ----------

try:
    ABC(DeltaTableCreateCheck=1)
    spark.sql("""
    CREATE TABLE IF NOT EXISTS PriceChange (
      SMA_PRICE_CHG_ID STRING,
      SMA_GTIN_NUM LONG,
      SMA_STORE STRING, 
      LOCATION_TYPE	INTEGER,
      SMA_ITM_EFF_DATE DATE,
      SELL_UNIT_CHG_IND STRING,
      SMA_SELL_RETL FLOAT,
      SMA_RETL_UOM STRING,
      SELL_RETAIL_CURR STRING,
      MLT_UNIT_CHG_IND STRING,
      SMA_RETL_MULT_UNIT FLOAT,
      SMA_MULT_UNIT_RETL FLOAT,
      SMA_RETL_MULT_UOM STRING,
      MLT_UNIT_CUR STRING,
      SMA_BATCH_SERIAL_NBR LONG,
      STATUS STRING,
      SMA_RETL_PRC_ENTRY_CHG STRING,
      CUSTOM_FIELD_2 STRING,
      CUSTOM_FIELD_3 STRING,
      INSERT_ID STRING,
      INSERT_TIMESTAMP TIMESTAMP
    )
    USING delta
    Location '{}'
    PARTITIONED BY (SMA_STORE)
    """.format(PriceChangeDeltaPath))
except Exception as ex:
    ABC(DeltaTableCreateCheck = 0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'deltaCreator')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson)) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Promotion Linking Table

# COMMAND ----------

# %sql
# delete from PromotionLink where SMA_STORE=0020

# COMMAND ----------

try:
    ABC(DeltaTableCreateCheck=1)
    spark.sql("""
    CREATE TABLE IF NOT EXISTS PromotionLink(
    SMA_LINK_HDR_COUPON LONG,
    SMA_LINK_HDR_LOCATION STRING,
    SMA_STORE STRING,
    SMA_CHG_TYPE INTEGER,
    SMA_LINK_START_DATE DATE,
    SMA_LINK_RCRD_TYPE STRING,
    SMA_LINK_END_DATE DATE,
    SMA_ITM_EFF_DATE DATE,
    SMA_LINK_CHG_TYPE INTEGER,
    SMA_PROMO_LINK_UPC LONG, 
    LOCATION_TYPE INTEGER,
    MERCH_TYPE INTEGER,
    SMA_LINK_HDR_MAINT_TYPE STRING,
    SMA_LINK_ITEM_NBR STRING,
    SMA_LINK_OOPS_ADWK STRING,
    SMA_LINK_OOPS_FILE_ID STRING,
    SMA_LINK_TYPE STRING,
    SMA_LINK_SYS_DIGIT STRING,
    SMA_LINK_FAMCD_PROMOCD STRING,
    SMA_BATCH_SERIAL_NBR LONG,
    INSERT_ID  STRING,
    INSERT_TIMESTAMP TIMESTAMP,
    LAST_UPDATE_ID  STRING,
    LAST_UPDATE_TIMESTAMP TIMESTAMP
    )
    USING delta
    Location '{}'
    PARTITIONED BY (SMA_STORE)
    """.format(PromotionLinkDeltaPath))
except Exception as ex:
    ABC(DeltaTableCreateCheck = 0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'deltaCreator')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson)) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Promotion Linking Main Table

# COMMAND ----------

# %sql
# delete from PromotionMain where SMA_STORE=0020

# COMMAND ----------

try:
    ABC(DeltaTableCreateCheck=1)
    spark.sql("""
    CREATE TABLE IF NOT EXISTS PromotionMain(
    SMA_PROMO_LINK_UPC LONG,
    SMA_ITM_EFF_DATE DATE,
    SMA_LINK_HDR_COUPON LONG,
    SMA_LINK_HDR_LOCATION STRING,
    SMA_LINK_APPLY_DATE DATE,
    SMA_LINK_APPLY_TIME STRING,
    SMA_LINK_END_DATE DATE,
    SMA_LINK_CHG_TYPE INTEGER,
    SMA_CHG_TYPE INTEGER,
    SMA_STORE STRING,
    SMA_BATCH_SERIAL_NBR LONG,
    INSERT_ID STRING,
    INSERT_TIMESTAMP TIMESTAMP)
    USING delta
    Location '{}'
    PARTITIONED BY (SMA_STORE)
    """.format(PromotionMainDeltaPath))
except Exception as ex:
    ABC(DeltaTableCreateCheck = 0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'deltaCreator')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson)) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Promo Link Temp

# COMMAND ----------

try:
  spark.sql("""CREATE TABLE IF NOT EXISTS promoLinkTemp(
              SMA_LINK_HDR_COUPON LONG,
              SMA_LINK_HDR_LOCATION STRING,
              SMA_STORE STRING,
              SMA_CHG_TYPE INTEGER,
              SMA_LINK_START_DATE DATE,
              SMA_LINK_RCRD_TYPE STRING,
              SMA_LINK_END_DATE DATE,
              SMA_ITM_EFF_DATE DATE,
              SMA_LINK_CHG_TYPE INTEGER,
              SMA_PROMO_LINK_UPC LONG,
              LOCATION_TYPE INTEGER,
              MERCH_TYPE INTEGER,
              SMA_LINK_HDR_MAINT_TYPE STRING,
              SMA_LINK_ITEM_NBR STRING,
              SMA_LINK_OOPS_ADWK STRING,
              SMA_LINK_OOPS_FILE_ID STRING,
              SMA_LINK_TYPE STRING,
              SMA_LINK_SYS_DIGIT STRING,
              SMA_LINK_FAMCD_PROMOCD STRING,
              SMA_BATCH_SERIAL_NBR LONG,
              INSERT_ID STRING,
              INSERT_TIMESTAMP TIMESTAMP,
              LAST_UPDATE_ID STRING,
              LAST_UPDATE_TIMESTAMP TIMESTAMP,
              RANK INTEGER,
              DUPLICATEFLAG BOOLEAN)
              USING delta
              Location '{}'
              PARTITIONED BY (SMA_STORE)""".format(promoLinkTemp))
except Exception as ex:
  ABC(tempCnt='')
  loggerAtt.error(ex)
  err = ErrorReturn('Error', ex,'CREATE TEMP TABLE')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## UDF, Renaming, Processing and Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### User Defined Functions

# COMMAND ----------

date_func =  udf (lambda x: datetime.datetime.strptime(str(x), '%Y%m%d'), DateType())

dateFuncWithHyp =  udf (lambda x: datetime.datetime.strptime(str(x), '%Y-%m-%d'), DateType())

def statusChange(s):
  if s == 0:
    return 'D'
  elif s == 1:
    return 'C'
  elif s == 2:
    return 'M'
  else:
    return s

statusChangeUDF = udf(statusChange)

def tagSize(s):
  if (s == "0" or s == "1" or s == "2" or s == "3"):
    return s
  else:
    return None

tagSizeUDF = udf(tagSize)

def formatZeros(s):
  if s is not None:
    return format(s, '.2f')
  else:
    return s

formatZerosUDF = udf(formatZeros)   

def formatZeros5F(s):
  if s is not None:
    return format(s, '.5f')
  else:
    return s

formatZeros5FUDF = udf(formatZeros5F)   

#Column renaming functions 
def itemflat_promotable(s):
    return Item_renaming[s]
def change_col_name(s):
    return s in Item_renaming

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Item Transformation

# COMMAND ----------

def ItemTransformation(processed_df,pipelineid):
  
  processed_df=processed_df.withColumn('SMA_LINK_HDR_MAINT_TYPE', statusChangeUDF(col("SMA_LINK_HDR_MAINT_TYPE")))
  invalid_transformation_df=processed_df.filter(((col('SMA_LINK_HDR_MAINT_TYPE') != 'D') & (col("SMA_LINK_HDR_MAINT_TYPE") != 'C') & (col("SMA_LINK_HDR_MAINT_TYPE") != 'M')))
  
  loggerAtt.info(f"No of records with inproper status: {invalid_transformation_df.count()}")
  
  processed_df = processed_df.filter(((col('SMA_LINK_HDR_MAINT_TYPE') == 'D') | (col("SMA_LINK_HDR_MAINT_TYPE") == 'C') | (col("SMA_LINK_HDR_MAINT_TYPE") == 'M')))
  
  loggerAtt.info(f"No of records with proper status: {processed_df.count()}")
#   if processing_file == 'FullItem':
#     processed_df=processed_df.withColumn('SMA_ITM_EFF_DATE',to_date(date_format(current_date(), 'yyyy-MM-dd')))
#   else:
  processed_df=processed_df.withColumn('SMA_ITM_EFF_DATE',to_date(date_format(dateFuncWithHyp(col('SMA_ITM_EFF_DATE')), 'yyyy-MM-dd')))
#     processed_df=processed_df.withColumn('SMA_ITM_EFF_DATE', when((col("SMA_ITM_EFF_DATE") < current_date()), to_date(date_format(current_date(), 'yyyy-MM-dd'))).otherwise(col("SMA_ITM_EFF_DATE")))
  processed_df=processed_df.withColumn('SMA_PRIV_LABEL',when(col('SMA_PRIV_LABEL') =='Y',lit(1)).otherwise(lit(0)).cast(StringType()))
  processed_df=processed_df.withColumn('SMA_TAG_SZ',tagSizeUDF(col('SMA_TAG_SZ')))
  processed_df=processed_df.withColumn('SMA_CONTRIB_QTY',when(col('SMA_CONTRIB_QTY') == 0000.00,lit(None)).otherwise(round(col('SMA_CONTRIB_QTY'))).cast(StringType()))
  processed_df=processed_df.withColumn('SMA_GAS_IND',when(col('SMA_GAS_IND') == 'Y',1).otherwise(0).cast(IntegerType()))
  processed_df=processed_df.withColumn('SMA_SOURCE_METHOD',trim(col("SMA_SOURCE_METHOD")))
  processed_df=processed_df.withColumn('SMA_TARE_PCT',when(col('SMA_TARE_PCT') == 00.00,lit(None)).otherwise(lpad(formatZerosUDF(col('SMA_TARE_PCT')),2,'0')).cast(StringType())) 
  processed_df=processed_df.withColumn('SMA_FIXED_TARE_WGT',when(col('SMA_FIXED_TARE_WGT') == 00.00,lit(None)).otherwise(lpad(formatZerosUDF(col('SMA_FIXED_TARE_WGT')),2,'0')).cast(StringType()))
  processed_df=processed_df.withColumn('SMA_TARE_UOM',when(col('SMA_FIXED_TARE_WGT').isNull(), lit(None)).otherwise(col('SMA_TARE_UOM')).cast(StringType()))
  processed_df=processed_df.withColumn('SMA_RESTR_CODE',when(col('SMA_RESTR_CODE') == '00',lit(None)).otherwise(col('SMA_RESTR_CODE')).cast(StringType()))
  processed_df=processed_df.withColumn('SMA_WIC_SHLF_MIN',when(col('SMA_WIC_SHLF_MIN') == 0,lit(None)).otherwise(lpad(col('SMA_WIC_SHLF_MIN'),2,'0')).cast(StringType()))
  processed_df=processed_df.withColumn('SMA_LINK_UPC',when(col('SMA_LINK_UPC') == '00000000000000',lit(None)).otherwise(col('SMA_LINK_UPC')).cast(LongType()))
  processed_df=processed_df.withColumn('SMA_ACTIVATION_CODE',when(col('SMA_ACTIVATION_CODE') =='0',lit(None)).otherwise(col('SMA_ACTIVATION_CODE')).cast(StringType()))
  processed_df=processed_df.withColumn('SMA_IFPS_CODE',when(col('SMA_IFPS_CODE') == 0,lit(None)).otherwise(lpad(col('SMA_IFPS_CODE'),6,'0')).cast(StringType())) ## Integer or string. Currently it is String in Item Master
  processed_df=processed_df.fillna({'SMA_UPC_OVERRIDE_GRP_NUM':0})
  processed_df=processed_df.withColumn('SMA_UPC_OVERRIDE_GRP_NUM',lpad(col('SMA_UPC_OVERRIDE_GRP_NUM'),2,'0').cast(StringType()))
  
  processed_df=processed_df.fillna({'SMA_TENDER_RESTRICTION_CODE':0})
  processed_df=processed_df.withColumn('SMA_TENDER_RESTRICTION_CODE',lpad(col('SMA_TENDER_RESTRICTION_CODE'),2,'0').cast(StringType()))
  
  processed_df=processed_df.fillna({'SMA_REFUND_RECEIPT_EXCLUSION':0})
  processed_df=processed_df.withColumn('SMA_REFUND_RECEIPT_EXCLUSION',col('SMA_REFUND_RECEIPT_EXCLUSION').cast(StringType()))
  
  processed_df=processed_df.fillna({'SMA_FEE_ITEM_TYPE':0})
  processed_df=processed_df.withColumn('SMA_FEE_ITEM_TYPE',lpad(col('SMA_FEE_ITEM_TYPE'),2,'0').cast(StringType()))
  
  processed_df=processed_df.fillna({'SMA_KOSHER_FLAG': 'N'})
  processed_df=processed_df.withColumn('SMA_LEGACY_ITEM',lpad(col('SMA_LEGACY_ITEM'),12,'0').cast(StringType()))
  
  processed_df=processed_df.fillna({'SMA_GUIDING_STARS': 0})
  processed_df=processed_df.withColumn('SMA_COMP_ID',when(col('SMA_COMP_ID') == '0',lit(None)).otherwise(col('SMA_COMP_ID')).cast(StringType()))

  processed_df=processed_df.withColumn('SMA_COMP_PRICE',when(col('SMA_COMP_PRICE') == 0,lit(None)).otherwise(col('SMA_COMP_PRICE')).cast(StringType()))
  processed_df=processed_df.withColumn('SMA_NON_PRICED_ITEM',trim(col("SMA_NON_PRICED_ITEM")))
  processed_df=processed_df.withColumn('SMA_GLUTEN_FREE',trim(col("SMA_GLUTEN_FREE")))
  processed_df=processed_df.withColumn('SMA_ITEM_SIZE',lpad(formatZerosUDF(round(col("SMA_ITEM_SIZE"), 2)),6,'0'))
  
  processed_df=processed_df.withColumn("SMA_GTIN_NUM",col('SMA_GTIN_NUM').cast(LongType()))
  processed_df=processed_df.withColumn('ITEM_PARENT', col('SMA_GTIN_NUM'))
  processed_df=processed_df.withColumn('GROUP_NO', col('SMA_SUB_DEPT')) ## cHECK GROUP_NO IS NECESSARY
  processed_df=processed_df.withColumn('SMA_BATCH_SERIAL_NBR',concat(lit(str(Date_serial)),lpad(col('SMA_BATCH_SERIAL_NBR'),9,'0')).cast(LongType()))
  processed_df=processed_df.withColumn('SMA_BATCH_SUB_DEPT',col('SMA_BATCH_SUB_DEPT').cast(LongType()))
  processed_df=processed_df.withColumn('SMA_EFF_DOW',lpad(col('SMA_EFF_DOW'),2,'0').cast(StringType()))
  processed_df=processed_df.withColumn('SMA_RETL_VENDOR',lpad(col('SMA_RETL_VENDOR'),6,'0').cast(StringType()))
  processed_df=processed_df.withColumn('SMA_STORE', lpad(col('SMA_STORE'),4,'0').cast(StringType()))
  processed_df=processed_df.withColumn('SMA_DEST_STORE', lpad(col('SMA_DEST_STORE'),4,'0').cast(StringType()))
  processed_df=processed_df.withColumn('SMA_SEGMENT', lpad(col('SMA_SEGMENT'),3,'0').cast(StringType()))
  processed_df=processed_df.withColumn('SMA_MEAS_OF_EACH', lpad(formatZeros5FUDF(col('SMA_MEAS_OF_EACH')),4,'0').cast(StringType()))
  processed_df=processed_df.withColumn('SMA_MEAS_OF_PRICE', col('SMA_MEAS_OF_PRICE').cast(StringType()))
  processed_df=processed_df.withColumn('SMA_WPG', lpad(col('SMA_WPG'),3,'0').cast(StringType()))
  
                                       
  
  
  processed_df=processed_df.withColumn('EFF_TS',lit(None).cast(TimestampType())) # additional
  processed_df=processed_df.withColumn('PROMO_MIN_REQ_QTY',lit(None).cast(StringType()))
  processed_df=processed_df.withColumn('PROMO_LIM_VAL',lit(None).cast(StringType()))
  processed_df=processed_df.withColumn('PROMO_BUY_QTY',lit(None).cast(StringType()))
  processed_df=processed_df.withColumn('PROMO_GET_QTY',lit(None).cast(StringType()))
  processed_df=processed_df.withColumn('HOW_TO_SELL',lit(None).cast(StringType()))
  processed_df=processed_df.withColumn('INNER_PACK_SIZE',lit(None).cast(StringType()))
  processed_df=processed_df.withColumn('BRAND_LOW',lit(None).cast(StringType()))
  processed_df=processed_df.withColumn('MANUFACTURER',lit(None).cast(StringType()))
  processed_df=processed_df.withColumn('AVG_WGT',lit(None).cast(StringType()))
  
  processed_df=processed_df.withColumn('ATC',lit(0).cast(IntegerType()))
  
  processed_df=processed_df.withColumn('SMA_LINK_HDR_COUPON',col('SMA_LINK_HDR_COUPON').cast(LongType()))
  processed_df=processed_df.withColumn('SMA_LINK_HDR_LOCATION', lpad(col('SMA_LINK_HDR_LOCATION'),10,'0').cast(StringType()))
  processed_df=processed_df.withColumn('SMA_LINK_APPLY_TIME', lpad(col('SMA_LINK_APPLY_TIME'),6,'0').cast(StringType()))
  
  processed_df=processed_df.withColumn("LOCATION_TYPE",lit(0).cast(IntegerType()))
  processed_df=processed_df.withColumn("MERCH_TYPE",lit(0).cast(IntegerType()))
  
  processed_df=processed_df.withColumn('SMA_LINK_ITEM_NBR', lpad(col('SMA_LINK_ITEM_NBR'),12,'0').cast(StringType()))
  processed_df=processed_df.withColumn('SMA_LINK_OOPS_ADWK', col('SMA_LINK_OOPS_ADWK').cast(StringType()))
  processed_df=processed_df.withColumn("SMA_LINK_TYPE",col('SMA_LINK_TYPE').cast(StringType()))
  
  processed_df=processed_df.withColumn("SMA_LINK_RCRD_TYPE",col('SMA_LINK_RCRD_TYPE').cast(StringType()))
  processed_df=processed_df.withColumn("SMA_LINK_FAMCD_PROMOCD", col('SMA_LINK_FAMCD_PROMOCD').cast(StringType()))
  processed_df=processed_df.withColumn("SMA_PRICE_CHG_ID",lpad(col('SMA_PRICE_CHG_ID'),15,'0').cast(StringType()))
  
  processed_df=processed_df.withColumn('SELL_UNIT_CHG_IND',lit('Y')) 
  processed_df=processed_df.withColumn('MLT_UNIT_CHG_IND',lit('Y')) 
  processed_df=processed_df.withColumn('PRICECHANGE_STATUS',lit('C')) 
  processed_df=processed_df.withColumn('SELL_RETAIL_CURR',lit('USD')) 
  processed_df=processed_df.withColumn('MLT_UNIT_CUR',lit('USD')) 
  processed_df=processed_df.withColumn('PRICECHANGE_CUSTOM_FIELD_2',lit(None).cast(StringType()))
  processed_df=processed_df.withColumn('PRICECHANGE_CUSTOM_FIELD_3',lit(None).cast(StringType())) 
  processed_df=processed_df.withColumn('SMA_HLTHY_IDEAS',lit(None).cast(StringType())) 
     
  processed_df=processed_df.withColumn("INSERT_ID",lit(pipelineid))
  processed_df=processed_df.withColumn("INSERT_TIMESTAMP",current_timestamp())
  processed_df=processed_df.withColumn("LAST_UPDATE_ID",lit(pipelineid))
  processed_df=processed_df.withColumn("LAST_UPDATE_TIMESTAMP",current_timestamp())
  
  
# Check whether fill na is needed here
  
  #   processed_df=processed_df.withColumn('SMA_COMP_OBSERV_DATE',when(col('SMA_COMP_OBSERV_DATE') == '00000000',lit("0000/00/00")).otherwise(date_format(date_func(col('SMA_COMP_OBSERV_DATE')), 'yyyy/MM/dd')).cast(StringType()))
  #processed_df=processed_df.withColumn('SMA_LINK_END_DATE',when(col('SMA_LINK_END_DATE') == '00000000',col('SMA_LINK_END_DATE')).otherwise(to_date(date_format(date_func(col('SMA_LINK_END_DATE')), 'yyyy-MM-dd'))))
  #processed_df=processed_df.withColumn('SMA_LINK_END_DATE',to_date(date_format(date_func(col('SMA_LINK_END_DATE')), 'yyyy-MM-dd')))
  
  return invalid_transformation_df, processed_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Item Renaming dictionary

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

# MAGIC %md
# MAGIC 
# MAGIC ### Item file reading

# COMMAND ----------

def readFile(file_location, infer_schema, first_row_is_header, delimiter,file_type):
  raw_df = spark.read.format(file_type) \
    .option("mode","PERMISSIVE") \
    .option("header", first_row_is_header) \
    .option("dateFormat", "yyyyMMdd") \
    .option("sep", delimiter) \
    .schema(itemRaw_schema) \
    .load(file_location)
  
  ABC(ReadDataCheck=1)
  RawDataCount = raw_df.count()
  ABC(RawDataCount=RawDataCount)
  loggerAtt.info(f"Count of Records in the File: {RawDataCount}")
  
  return raw_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write Invalid Records

# COMMAND ----------

def writeInvalidRecord(item_invalidRecords, Invalid_RecordsPath, Date):
  ABC(InvalidRecordSaveCheck = 1)
  if item_invalidRecords is not None:
    if item_invalidRecords.count() > 0:
      invalidCount = item_invalidRecords.count()
      loggerAtt.info(f"Count of Invalid Records in the File: {invalidCount}")
      ABC(InvalidRecordCount = invalidCount)
      
      item_invalidRecords.write.mode('Append').format('parquet').save(Invalid_RecordsPath)
      loggerAtt.info('======== Invalid Item Records write operation finished ========')
    else:
      loggerAtt.info('======== No Invalid Item Records ========')
  else:
    loggerAtt.info('======== No Invalid Item Records ========')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ### Promo Linking Function

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/AHOLD/promoLinkingFunc

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ### Price Change Function

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/AHOLD/priceChangeFunc

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ### Promo Linking Main Function

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/AHOLD/promoLinkingMainFunc

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Item Function

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/AHOLD/itemMainFunc

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Main Function Processing

# COMMAND ----------

if __name__ == "__main__":
  ## File reading parameters
  loggerAtt.info('======== Input Item Main file processing initiated ========')
  
  ## Step1: Read Item File
  loggerAtt.info("File Read check")
  try:
    ABC(ReadDataCheck=1)
    item_raw_df = readFile(file_location, infer_schema, first_row_is_header, delimiter, file_type)
  except Exception as ex:
    if 'java.io.FileNotFoundException' in str(ex):
      loggerAtt.error('File does not exists')
    else:
      loggerAtt.error(ex)
    ABC(ReadDataCheck=0)
    ABC(RawDataCount="")
    err = ErrorReturn('Error', ex,'readFile')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
  if item_raw_df is not None:
    loggerAtt.info("ABC Framework processing initiated")
    loggerAtt.info("Renaming columns")
    ## Step 2: Renaming
    try:
      ABC(RenamingCheck=1)
      item_raw_df = quinn.with_some_columns_renamed(itemflat_promotable, change_col_name)(item_raw_df)
    except Exception as ex:
      ABC(RenamingCheck=0)
      err = ErrorReturn('Error', ex,'Renaming columns check Error')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))

    loggerAtt.info("EOF count check")
    ## Step 3: EOF file count 
#     try:
#       ABC(BOFEOFCheck=1)

#       eofDf = item_raw_df.filter(item_raw_df["SMA_DEST_STORE"].rlike("^TRAILER"))

#       loggerAtt.info("EOF file record count is " + str(eofDf.count()))
#       EOFcnt=eofDf.count()
#       ABC(EOFCount=EOFcnt)

#       # If there are multiple header/Footer then the file is invalid
#       if (eofDf.count() != 1):
#         raise Exception('Error in EOF value')

#       fileRecordCount = eofDf.withColumn("SMA_BATCH_SERIAL_NBR",trim(col("SMA_BATCH_SERIAL_NBR"))).select('SMA_BATCH_SERIAL_NBR')
#       fileRecordCount = fileRecordCount.groupBy("SMA_BATCH_SERIAL_NBR").mean().collect()[0]["SMA_BATCH_SERIAL_NBR"]
#       fileRecordCount = re.sub('\W+','', fileRecordCount)
#       fileRecordCount = int(fileRecordCount)

#       actualRecordCount = int(item_raw_df.count() + eofDf.count() - 2)

#       loggerAtt.info("Record Count mentioned in file: " + str(fileRecordCount))
#       loggerAtt.info("Actual no of record in file: " + str(actualRecordCount))

#       if fileRecordCount != actualRecordCount:
#         raise Exception('Error in record count value')

#     except Exception as ex:
#       ABC(BOFEOFCheck=0)
#       ABC(EOFCount=0)
#       err = ErrorReturn('Error', ex,'EOF count check Error')
#       errJson = jsonpickle.encode(err)
#       errJson = json.loads(errJson)
#       dbutils.notebook.exit(Merge(ABCChecks,errJson))

    ## Step 4: Removing Null records
    loggerAtt.info("Removing Null records check")
    try:
      ABC(NullValueCheck=1)
      item_raw_df = item_raw_df.withColumn("SMA_BATCH_SERIAL_NBR",col('SMA_BATCH_SERIAL_NBR').cast(IntegerType()))
      
      item_raw_df = item_raw_df.withColumn("SMA_DEST_STORE",col('SMA_DEST_STORE').cast(IntegerType()))

      item_nullRows = item_raw_df.where(reduce(lambda x, y: x | y, (col(x).isNull() for x in item_raw_df.columns)))

      loggerAtt.info("Dimension of the Null records:("+str(item_nullRows.count())+"," +str(len(item_nullRows.columns))+")")

      ABC(DropNACheck = 1)
      item_raw_df = item_raw_df.na.drop()

      loggerAtt.info("Dimension of the Not null records:("+str(item_raw_df.count())+"," +str(len(item_raw_df.columns))+")")

      ABC(NullValuCount = item_nullRows.count())
    except Exception as ex:
      ABC(NullValueCheck=0)
      ABC(DropNACheck=0)
      ABC(NullValuCount='')
      err = ErrorReturn('Error', ex,'Renaming columns check Error')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))    

    ## Step 5: Removing duplicate record 
#     loggerAtt.info("Removing duplicate record check")
#     try:
#       ABC(DuplicateValueCheck = 1)
#       if (item_raw_df.groupBy('SMA_DEST_STORE','SMA_GTIN_NUM','SMA_ITM_EFF_DATE').count().filter("count > 1").count()) > 0:
#           item_duplicate_records = item_raw_df.groupBy('SMA_DEST_STORE','SMA_GTIN_NUM', 'SMA_ITM_EFF_DATE').count().filter(col('count') > 1)
#           item_duplicate_records = item_duplicate_records.drop(item_duplicate_records['count'])
#           item_duplicate_records = (item_raw_df.join(item_duplicate_records,["SMA_GTIN_NUM", 'SMA_DEST_STORE', 'SMA_ITM_EFF_DATE'], "leftsemi"))
#           DuplicateValueCnt = item_duplicate_records.count()
#           loggerAtt.info("Duplicate Record Count: ("+str(DuplicateValueCnt)+"," +str(len(item_duplicate_records.columns))+")")
#           ABC(DuplicateValueCount=DuplicateValueCnt)
#           item_raw_df = (item_raw_df.join(item_duplicate_records,["SMA_GTIN_NUM", 'SMA_DEST_STORE', 'SMA_ITM_EFF_DATE'], "leftanti"))
#       else:
#           ABC(DuplicateValueCount=0)
#           loggerAtt.info(f"No PROBLEM RECORDS")
    
#     except Exception as ex:
#       ABC(DuplicateValueCheck=0)
#       ABC(DuplicateValueCount='')
#       err = ErrorReturn('Error', ex,'Removing duplicate record Error')
#       errJson = jsonpickle.encode(err)
#       errJson = json.loads(errJson)
#       dbutils.notebook.exit(Merge(ABCChecks,errJson))     

    loggerAtt.info("ABC Framework processing ended")
    ## Step 6: Item Transformation
    try:
      loggerAtt.info("Item transformation check")
      ABC(TransformationCheck = 1)
      invalid_transformed_df, item_raw_df = ItemTransformation(item_raw_df,PipelineID)
      loggerAtt.info("Invalid Transformed records are (" +str(invalid_transformed_df.count())+"," +str(len(invalid_transformed_df.columns))+")")
      loggerAtt.info("Valid Transformed records are " + str(item_raw_df.count()))
    except Exception as ex:
      ABC(TransformationCheck = 0)
      loggerAtt.error(ex)
      err = ErrorReturn('Error', ex,'couponTransformation')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))  

    ## Step 7:Combining Duplicate, null record and invalid item records
    loggerAtt.info("Combining invalid records check")      
    try:
      ABC(itemInvalidUnionCheck = 1)
#       if item_duplicate_records is not None:
#         invalidRecordsList = [item_duplicate_records, item_nullRows,invalid_transformed_df]
#         item_invalidRecords = reduce(DataFrame.unionAll, invalidRecordsList)
#       else:
      invalidRecordsList = [item_nullRows,invalid_transformed_df]
      item_invalidRecords = reduce(DataFrame.unionAll, invalidRecordsList)
    except Exception as ex:
        ABC(itemInvalidUnionCheck = 0)
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'combining invalid records error')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
    
    ## Step 8:Calling all upsert operation
    if item_raw_df.count() > 0:
      if processing_file == "Recall":
        itemMainFunc(item_raw_df, processing_file)
      else: 
        itemMainFunc(item_raw_df, processing_file)
        priceChangeFunc(item_raw_df)
        promoLinkingMain(item_raw_df,Date_serial)
        
    
#     ## Step 9: Write Invalid records to ADLS location
#     loggerAtt.info("Write Invalid Record check")
#     try:
#       Invalid_RecordsPath = Invalid_RecordsPath + "/" +Date+ "/" + "Invalid_Data"
#       writeInvalidRecord(item_invalidRecords, Invalid_RecordsPath, Date)
#     except Exception as ex:
#       ABC(InvalidRecordSaveCheck = 0)
#       ABC(InvalidRecordCount = '')
#       loggerAtt.error(ex)
#       err = ErrorReturn('Error', ex,'writeInvalidRecord')
#       errJson = jsonpickle.encode(err)
#       errJson = json.loads(errJson)
#       dbutils.notebook.exit(Merge(ABCChecks,errJson)) 

# COMMAND ----------

promoLinking(item_raw_df, Invalid_RecordsPath, processing_file)

# COMMAND ----------

temp_table_name = "item_raw_df"
item_raw_df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from (select sma_store, count(*) as count from item_raw_df group by sma_store) order by count desc

# COMMAND ----------

item_raw_df1 = item_raw_df.filter((col('SMA_STORE') == '2593'))

# COMMAND ----------

promoLinking(item_raw_df1, Invalid_RecordsPath, processing_file)

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/tableVersionCapture

# COMMAND ----------

#%run /Centralized_Price_Promo/tableVersionCapture

# Calling function --> updateDeltaVersioning will insert records to Delta table 1) delhaizeCppVersion 2) aholdCppVersion

#DApipelineNames = ['product', 'price', 'producttostore', 'store', 'pos', 'itemMaster', 'altUPC','inventory']
#AUSApipelineNames = ['store','vendor','coupon','item','itemMaster']

### PipelineName will be one from above 2 sets.
### feed == Delhaize or Ahold
### filepath === Is the filepath that we have on our pipeline parameters
### fileName === Is the fileName that we have on our pipeline parameters
#updateDeltaVersioning(feed, pipelineName, pipelineId, filepath, fileName)

updateDeltaVersioning('Ahold', 'item', PipelineID, Filepath, FileName)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## writing log file to ADLS location

# COMMAND ----------

dbutils.fs.mv("file:"+p_logfile, Log_FilesPath+"/"+ custom_logfile_Name + file_date + '.log')
loggerAtt.info('======== Log file is updated at ADLS Location ========')
logging.shutdown()
err = ErrorReturn('Success', '','')
errJson = jsonpickle.encode(err)
errJson = json.loads(errJson)
Merge(ABCChecks,errJson)
dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

