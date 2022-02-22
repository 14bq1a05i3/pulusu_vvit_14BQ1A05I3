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

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = False;
# MAGIC SET TIME ZONE 'America/Halifax';

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##Calling Logger

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/Logging

# COMMAND ----------

custom_logfile_Name ='product_dailymaintainence_customlog'
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
#dbutils.widgets.removeAll()
dbutils.widgets.text("fileName","")
dbutils.widgets.text("filePath","")
dbutils.widgets.text("directory","")
dbutils.widgets.text("Container","")
dbutils.widgets.text("pipelineID","")
dbutils.widgets.text("MountPoint","")
dbutils.widgets.text("deltaPath","")
dbutils.widgets.text("logFilesPath","")
dbutils.widgets.text("invalidRecordsPath","")
dbutils.widgets.text("clientId","")
dbutils.widgets.text("keyVaultName","")
dbutils.widgets.text("itemTempEffDeltaPath","")
dbutils.widgets.text("feedEffDeltaPath","")
dbutils.widgets.text("POSemergencyFlag","")

FileName=dbutils.widgets.get("fileName")
Filepath=dbutils.widgets.get("filePath")
Directory=dbutils.widgets.get("directory")
container=dbutils.widgets.get("Container")
PipelineID=dbutils.widgets.get("pipelineID")
mount_point=dbutils.widgets.get("MountPoint")
ProductDeltaPath=dbutils.widgets.get("deltaPath")
Log_FilesPath=dbutils.widgets.get("logFilesPath")
Invalid_RecordsPath=dbutils.widgets.get("invalidRecordsPath")
Date = datetime.datetime.now(timezone("America/Halifax")).strftime("%Y-%m-%d")
file_location = '/mnt' + '/' + Directory + '/' + Filepath +'/' + FileName 
source= 'abfss://' + Directory + '@' + container + '.dfs.core.windows.net/'
clientId=dbutils.widgets.get("clientId")
keyVaultName=dbutils.widgets.get("keyVaultName")
itemTempEffDeltaPath=dbutils.widgets.get("itemTempEffDeltaPath")
productEffDeltaPath=dbutils.widgets.get("feedEffDeltaPath")
POSemergencyFlag=dbutils.widgets.get("POSemergencyFlag")

loggerAtt.info(f"Date : {Date}")
loggerAtt.info(f"File Location on Mount Point : {file_location}")
loggerAtt.info(f"Source or File Location on Container : {source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Declarations

# COMMAND ----------

file_location = str(file_location)
file_type = "csv"
infer_schema = "false"
first_row_is_header = "false"
delimiter = "|"
product_raw_df = None
product_duplicate_records = None
headerFooterRecord = None
productRecords = None
product_valid_Records = None
PipelineID= str(PipelineID)
folderDate = Date

unifiedProductFields = ["DSS_SUPER_CATEGORY_ID", "DSS_SUB_CATEGORY_ID", "DSS_ORGANIC_ID", "DSS_PRODUCT_SIZE", "DSS_PVT_LBL_FLG", "DSS_PRODUCT_CASE_PACK", "DSS_GROUP_TYPE", "DSS_UOM", "DSS_GUIDING_STAR_RATING", "DSS_AVG_UNIT_WT", "DSS_PRODUCT_DESC", "DSS_PRIMARY_ID", "DSS_PRIMARY_DESC", "DSS_SUB_CATEGORY_DESC", "DSS_CATEGORY_ID", "DSS_CATEGORY_DESC", "DSS_SUPER_CATEGORY_DESC", "DSS_ALL_CATEGORY_ID", "DSS_ALL_CATEGORY_DESC", "DSS_MDSE_PROGRAM_ID", "DSS_MDSE_PROGRAM_DESC", "DSS_PRICE_MASTER_ID", "DSS_PRICE_MASTER_DESC", "DSS_BUYER_ID", "DSS_BUYER_DESC", "DSS_PRICE_SENS_ID", "DSS_PRICE_SENS_SHORT_DESC", "DSS_PRICE_SENS_LONG_DESC", "DSS_MANCODE_ID", "DSS_MANCODE_DESC", "DSS_PRIVATE_BRAND_ID", "DSS_PRIVATE_BRAND_DESC", "DSS_PRODUCT_STATUS_ID", "DSS_PRODUCT_STATUS_DESC", "DSS_PRODUCT_UOM_DESC", "DSS_PRODUCT_PACK_QTY", "DSS_DIRECTOR_ID", "DSS_DIRECTOR_DESC", "DSS_DIRECTOR_GROUP_DESC", "DSS_MAJOR_CATEGORY_ID", "DSS_MAJOR_CATEGORY_DESC", "DSS_PLANOGRAM_ID", "DSS_PLANOGRAM_DESC", "DSS_HEIGHT", "DSS_WIDTH", "DSS_DEPTH", "DSS_BRAND_ID", "DSS_BRAND_DESC", "DSS_LIFO_POOL_ID", "DSS_DATE_UPDATED", "DSS_EQUIVALENT_SIZE_ID", "DSS_EQUIVALENT_SIZE_DESC", "DSS_EQUIV_SIZE", "DSS_SCAN_TYPE_ID", "DSS_SCAN_TYPE_DESC", "DSS_STORE_HANDLING_CODE", "DSS_SHC_DESC", "DSS_GS_RATING_DESC", "DSS_GS_DATE_RATED", "DSS_PRIVATE_LABEL_ID", "DSS_PRIVATE_LABEL_DESC", "DSS_PRODUCT_DIMESION_L_BYTE", "DSS_PRDT_NAME", "DSS_MFR_NAME", "DSS_BRAND_NAME", "DSS_KEYWORD", "DSS_META_DESC", "DSS_META_KEYWORD", "DSS_SRVG_SZ_DSC", "DSS_SRVG_SZ_UOM_DSC", "DSS_SRVG_PER_CTNR", "DSS_NEW_PRDT_FLG", "DSS_OVRLN_DSC", "DSS_ALT_SRVG_SZ_DSC", "DSS_ALT_SRVG_SZ_UOM", "DSS_PRIVATE_BRAND_CD", "DSS_MENU_LBL_FLG", "ALT_UPC_FETCH", "DSS_DC_ITEM_NUMBER", "BANNER_ID"]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Mounting ADLS location

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/Mount_Point_Creation

# COMMAND ----------

try:
  mounting(mount_point, source, clientId, keyVaultName)
  ABC(MountCheck=1)
except Exception as ex:
  # send error message to ADF and send email notification
  ABC(MountCheck=0)
  loggerAtt.error(str(ex))
  err = ErrorReturn('Error', ex,'Mounting')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

#Defining the schema for Product Table
loggerAtt.info('========Schema definition initiated ========')
ProductSchema = StructType([
                           StructField("DSS_PRODUCT_ID",StringType(),False),
                           StructField("DSS_PRODUCT_DESC",StringType(),False),
                           StructField("DSS_PRIMARY_ID",LongType(),False),
                           StructField("DSS_PRIMARY_DESC",StringType(),False),
                           StructField("DSS_SUB_CATEGORY_ID",IntegerType(),False),
                           StructField("DSS_SUB_CATEGORY_DESC",StringType(),False),
                           StructField("DSS_CATEGORY_ID",IntegerType(),False),
                           StructField("DSS_CATEGORY_DESC",StringType(),False),
                           StructField("DSS_SUPER_CATEGORY_ID",IntegerType(),False),
                           StructField("DSS_SUPER_CATEGORY_DESC",StringType(),False),
                           StructField("DSS_ALL_CATEGORY_ID",IntegerType(),False),
                           StructField("DSS_ALL_CATEGORY_DESC",StringType(),False),
                           StructField("DSS_MDSE_PROGRAM_ID",StringType(),True),
                           StructField("DSS_MDSE_PROGRAM_DESC",StringType(),True),
                           StructField("DSS_PRICE_MASTER_ID",StringType(),True),
                           StructField("DSS_PRICE_MASTER_DESC",StringType(),True),
                           StructField("DSS_BUYER_ID",IntegerType(),True),
                           StructField("DSS_BUYER_DESC",StringType(),False),
                           StructField("DSS_PRICE_SENS_ID",IntegerType(),False),
                           StructField("DSS_PRICE_SENS_SHORT_DESC",StringType(),True),
                           StructField("DSS_PRICE_SENS_LONG_DESC",StringType(),True),
                           StructField("DSS_MANCODE_ID",IntegerType(),False),
                           StructField("DSS_MANCODE_DESC",StringType(),False),
                           StructField("DSS_PRIVATE_BRAND_ID",StringType(),True),
                           StructField("DSS_PRIVATE_BRAND_DESC",StringType(),False),
                           StructField("DSS_PRODUCT_STATUS_ID",IntegerType(),False),
                           StructField("DSS_PRODUCT_STATUS_DESC",StringType(),False),
                           StructField("DSS_PRODUCT_SIZE",IntegerType(),False),
                           StructField("DSS_PRODUCT_UOM_ID",IntegerType(),False),
                           StructField("DSS_PRODUCT_UOM_DESC",StringType(),True),
                           StructField("DSS_PRODUCT_PACK_QTY",IntegerType(),False),
                           StructField("DSS_DC_ITEM_NUMBER",IntegerType(),False),
                           StructField("DSS_DIRECTOR_ID",IntegerType(),False),
                           StructField("DSS_DIRECTOR_DESC",StringType(),True),
                           StructField("DSS_DIRECTOR_GROUP_DESC",StringType(),True),
                           StructField("DSS_MAJOR_CATEGORY_ID",IntegerType(),False),
                           StructField("DSS_MAJOR_CATEGORY_DESC",StringType(),True),
                           StructField("DSS_PLANOGRAM_ID",IntegerType(),False),
                           StructField("DSS_PLANOGRAM_DESC",StringType(),True),
                           StructField("DSS_HEIGHT",IntegerType(),False),
                           StructField("DSS_WIDTH",IntegerType(),False),
                           StructField("DSS_DEPTH",IntegerType(),False),
                           StructField("DSS_BRAND_ID",StringType(),True),
                           StructField("DSS_BRAND_DESC",StringType(),False),
                           StructField("DSS_LIFO_POOL_ID",IntegerType(),False),
                           StructField("DSS_GROUP_TYPE",IntegerType(),False),
                           StructField("DSS_DATE_UPDATED",IntegerType(),False),
                           StructField("DSS_DATE_ADDED",IntegerType(),False),
                           StructField("DSS_EQUIVALENT_SIZE_ID",IntegerType(),False),
                           StructField("DSS_EQUIVALENT_SIZE_DESC",StringType(),True),
                           StructField("DSS_EQUIV_SIZE",IntegerType(),False),
                           StructField("DSS_SCAN_TYPE_ID",StringType(),True),
                           StructField("DSS_SCAN_TYPE_DESC",StringType(),True),
                           StructField("DSS_PRODUCT_CASE_PACK",IntegerType(),False),
                           StructField("DSS_STORE_HANDLING_CODE",IntegerType(),False),
                           StructField("DSS_SHC_DESC",StringType(),True),
                           StructField("DSS_GUIDING_STAR_RATING",StringType(),True),
                           StructField("DSS_GS_RATING_DESC",StringType(),False),
                           StructField("DSS_GS_DATE_RATED",StringType(),False),
                           StructField("DSS_PRIVATE_LABEL_ID",IntegerType(),False),
                           StructField("DSS_HBC_ITEM_NUMBER",IntegerType(),False),
                           StructField("DSS_PRIVATE_LABEL_DESC",StringType(),True),
                           StructField("DSS_ORGANIC_ID",IntegerType(),False),
                           StructField("DSS_PRODUCT_DIMESION_L_BYTE",StringType(),True),
                           StructField("DSS_BANNER_ID",StringType(),False),
                           StructField("DSS_PRDT_NAME",StringType(),False),
                           StructField("DSS_MFR_NAME",StringType(),True),
                           StructField("DSS_BRAND_NAME",StringType(),True),
                           StructField("DSS_KEYWORD",StringType(),True),
                           StructField("DSS_META_DESC",StringType(),True),
                           StructField("DSS_META_KEYWORD",StringType(),True),
                           StructField("DSS_PROMO_CODE",StringType(),True),
                           StructField("DSS_SRVG_SZ_DSC",StringType(),True),
                           StructField("DSS_SRVG_SZ_UOM_DSC",StringType(),True),
                           StructField("DSS_SRVG_PER_CTNR",StringType(),True),
                           StructField("DSS_NEW_PRDT_FLG",StringType(),False),
                           StructField("DSS_FSA_FLG",StringType(),False),
                           StructField("DSS_ACTIVE_FLG",StringType(),False),
                           StructField("DSS_OVRLN_DSC",StringType(),True),
                           StructField("DSS_ALT_SRVG_SZ_DSC",StringType(),True),
                           StructField("DSS_ALT_SRVG_SZ_UOM",StringType(),True),
                           StructField("DSS_AVG_UNIT_WT",StringType(),False),
                           StructField("DSS_UOM",StringType(),False),
                           StructField("DSS_PRIVATE_BRAND_CD",StringType(),True),
                           StructField("DSS_MENU_LBL_FLG",StringType(),False),
                           StructField("DSS_PVT_LBL_FLG",StringType(),False)
])
loggerAtt.info('========Schema definition ended ========')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Delta table creation

# COMMAND ----------

def deltaCreator(ProductDeltaPath):
  try:
    ABC(DeltaTableCreateCheck=1)
    spark.sql("""
    CREATE TABLE IF NOT EXISTS productDeltaTable (
                DSS_PRODUCT_ID LONG,
                DSS_PRODUCT_DESC STRING,
                DSS_PRIMARY_ID STRING,
                DSS_PRIMARY_DESC STRING,
                DSS_SUB_CATEGORY_ID STRING,
                DSS_SUB_CATEGORY_DESC STRING,
                DSS_CATEGORY_ID STRING,
                DSS_CATEGORY_DESC STRING,
                DSS_SUPER_CATEGORY_ID STRING,
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
                DSS_PRODUCT_SIZE STRING,
                DSS_PRODUCT_UOM_ID STRING,
                DSS_PRODUCT_UOM_DESC STRING,
                DSS_PRODUCT_PACK_QTY STRING,
                DSS_DC_ITEM_NUMBER STRING,
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
                DSS_DATE_ADDED STRING,
                DSS_EQUIVALENT_SIZE_ID STRING,
                DSS_EQUIVALENT_SIZE_DESC STRING,
                DSS_EQUIV_SIZE STRING,
                DSS_SCAN_TYPE_ID STRING,
                DSS_SCAN_TYPE_DESC STRING,
                DSS_PRODUCT_CASE_PACK STRING,
                DSS_STORE_HANDLING_CODE STRING,
                DSS_SHC_DESC STRING,
                DSS_GUIDING_STAR_RATING STRING,
                DSS_GS_RATING_DESC STRING,
                DSS_GS_DATE_RATED STRING,
                DSS_PRIVATE_LABEL_ID STRING,
                DSS_HBC_ITEM_NUMBER STRING,
                DSS_PRIVATE_LABEL_DESC STRING,
                DSS_ORGANIC_ID STRING,
                DSS_PRODUCT_DIMESION_L_BYTE STRING,
                DSS_BANNER_ID STRING,
                DSS_PRDT_NAME STRING,
                DSS_MFR_NAME STRING,
                DSS_BRAND_NAME STRING,
                DSS_KEYWORD STRING,
                DSS_META_DESC STRING,
                DSS_META_KEYWORD STRING,
                DSS_PROMO_CODE STRING,
                DSS_SRVG_SZ_DSC STRING,
                DSS_SRVG_SZ_UOM_DSC STRING,
                DSS_SRVG_PER_CTNR STRING,
                DSS_NEW_PRDT_FLG STRING,
                DSS_FSA_FLG STRING,
                DSS_ACTIVE_FLG STRING,
                DSS_OVRLN_DSC STRING,
                DSS_ALT_SRVG_SZ_DSC STRING,
                DSS_ALT_SRVG_SZ_UOM STRING,
                DSS_AVG_UNIT_WT STRING,
                DSS_UOM STRING,
                DSS_PRIVATE_BRAND_CD STRING,
                DSS_MENU_LBL_FLG STRING,
                DSS_PVT_LBL_FLG STRING,
                INSERT_ID STRING,
                INSERT_TIMESTAMP TIMESTAMP,
                LAST_UPDATE_ID STRING,
                LAST_UPDATE_TIMESTAMP TIMESTAMP
    )
    USING delta
    Location '{}'
    PARTITIONED BY (DSS_BANNER_ID)
    """.format(ProductDeltaPath))
  except Exception as ex:
    ABC(DeltaTableCreateCheck = 0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'deltaCreator')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  

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
                SCRTX_HDR_DESC STRING,
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

# spark.sql("""DROP TABLE IF EXISTS productTemp;""")
# dbutils.fs.rm('/mnt/delhaize-centralized-price-promo/Product/Outbound/SDM/productDeltaTemp',recurse=True)

# COMMAND ----------

try:
  ABC(DeltaTableCreateCheck = 1)
  loggerAtt.info("productTemp creation")
  spark.sql(""" CREATE TABLE IF NOT EXISTS productTemp(
                SMA_COMM INTEGER,
                SMA_SUB_COMM INTEGER,
                SMA_NAT_ORG_IND STRING,
                SMA_ITEM_SIZE STRING,
                SMA_PRIV_LABEL STRING,
                SMA_VENDOR_PACK INTEGER,
                SMA_ITEM_UOM STRING,
                SMA_ITEM_TYPE STRING,
                SMA_RETL_UOM STRING,
                SMA_RETL_MULT_UOM STRING,
                SMA_GUIDING_STARS INTEGER,
                AVG_WGT STRING,
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
                DSS_PRODUCT_ID LONG,
                DSS_DC_ITEM_NUMBER STRING,
                DSS_BANNER_ID STRING)
              USING delta 
              PARTITIONED BY (DSS_BANNER_ID)
              LOCATION '{}' """.format(productEffDeltaPath))
except Exception as ex:
  ABC(DeltaTableCreateCheck = 0)
  loggerAtt.error(ex)
  err = ErrorReturn('Error', ex,'productEffDeltaPath deltaCreator')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## User Defined Functions

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Product processing

# COMMAND ----------

def typeCheck(s):
  try: 
      int(s)
      return True
  except ValueError:
      return False

typeCheckUDF = udf(typeCheck)

#Column renaming functions 
def itemEff_promotable(s):
    return product_Renaming[s]
def itemEff_change_col_name(s):
    return s in product_Renaming

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Product Transformation

# COMMAND ----------

def productTransformation(processed_df, pipelineid):
  
  loggerAtt.info("Product Transformation function initiated")
  ABC(TransformationCheck=1)
  # Padding 0 to all columns where it is case as integer type
  processed_df=processed_df.withColumn('DSS_PRODUCT_ID', col('DSS_PRODUCT_ID').cast(LongType()))
  processed_df=processed_df.withColumn('DSS_PRIMARY_ID', lpad(col('DSS_PRIMARY_ID'),14,'0'))
  processed_df=processed_df.withColumn('DSS_SUB_CATEGORY_ID', lpad(col('DSS_SUB_CATEGORY_ID'),4,'0'))
  processed_df=processed_df.withColumn('DSS_CATEGORY_ID', lpad(col('DSS_CATEGORY_ID'),4,'0'))
  processed_df=processed_df.withColumn('DSS_SUPER_CATEGORY_ID', lpad(col('DSS_SUPER_CATEGORY_ID'),4,'0'))
  processed_df=processed_df.withColumn('DSS_ALL_CATEGORY_ID', lpad(col('DSS_ALL_CATEGORY_ID'),1,'0'))
  processed_df=processed_df.withColumn('DSS_BUYER_ID', when(col('DSS_BUYER_ID').isNull(), lit(None)).otherwise(lpad(col('DSS_BUYER_ID'),2,'0')))
  processed_df=processed_df.withColumn('DSS_PRICE_SENS_ID', lpad(col('DSS_PRICE_SENS_ID'),1,'0'))
  processed_df=processed_df.withColumn('DSS_MANCODE_ID', lpad(col('DSS_MANCODE_ID'),5,'0'))
  processed_df=processed_df.withColumn('DSS_PRODUCT_STATUS_ID', lpad(col('DSS_PRODUCT_STATUS_ID'),1,'0'))
  processed_df=processed_df.withColumn('DSS_PRODUCT_SIZE', lpad(col('DSS_PRODUCT_SIZE'),5,'0'))
  processed_df=processed_df.withColumn('DSS_PRODUCT_UOM_ID', lpad(col('DSS_PRODUCT_UOM_ID'),2,'0'))
  processed_df=processed_df.withColumn('DSS_PRODUCT_PACK_QTY', lpad(col('DSS_PRODUCT_PACK_QTY'),5,'0'))
  processed_df=processed_df.withColumn('DSS_DC_ITEM_NUMBER', lpad(col('DSS_DC_ITEM_NUMBER'),6,'0'))
  processed_df=processed_df.withColumn('DSS_DIRECTOR_ID', lpad(col('DSS_DIRECTOR_ID'),3,'0'))
  processed_df=processed_df.withColumn('DSS_MAJOR_CATEGORY_ID', lpad(col('DSS_MAJOR_CATEGORY_ID'),2,'0'))
  processed_df=processed_df.withColumn('DSS_PLANOGRAM_ID', lpad(col('DSS_PLANOGRAM_ID'),3,'0'))
  processed_df=processed_df.withColumn('DSS_HEIGHT', lpad(col('DSS_HEIGHT'),5,'0'))
  processed_df=processed_df.withColumn('DSS_WIDTH', lpad(col('DSS_WIDTH'),5,'0'))
  processed_df=processed_df.withColumn('DSS_DEPTH', lpad(col('DSS_DEPTH'),5,'0'))
  processed_df=processed_df.withColumn('DSS_LIFO_POOL_ID', lpad(col('DSS_LIFO_POOL_ID'),2,'0'))
  processed_df=processed_df.withColumn('DSS_GROUP_TYPE', lpad(col('DSS_GROUP_TYPE'),5,'0'))
  processed_df=processed_df.withColumn('DSS_DATE_UPDATED', lpad(col('DSS_DATE_UPDATED'),8,'0'))
  processed_df=processed_df.withColumn('DSS_DATE_ADDED', lpad(col('DSS_DATE_ADDED'),8,'0'))
  processed_df=processed_df.withColumn('DSS_EQUIVALENT_SIZE_ID', lpad(col('DSS_EQUIVALENT_SIZE_ID'),2,'0'))
  processed_df=processed_df.withColumn('DSS_EQUIV_SIZE', lpad(col('DSS_EQUIV_SIZE'),7,'0'))
  processed_df=processed_df.withColumn('DSS_PRODUCT_CASE_PACK', lpad(col('DSS_PRODUCT_CASE_PACK'),5,'0'))
  processed_df=processed_df.withColumn('DSS_STORE_HANDLING_CODE', lpad(col('DSS_STORE_HANDLING_CODE'),3,'0'))
  processed_df=processed_df.withColumn('DSS_PRIVATE_LABEL_ID', lpad(col('DSS_PRIVATE_LABEL_ID'),2,'0'))
  processed_df=processed_df.withColumn('DSS_HBC_ITEM_NUMBER', lpad(col('DSS_HBC_ITEM_NUMBER'),7,'0'))
  processed_df=processed_df.withColumn('DSS_ORGANIC_ID', lpad(col('DSS_ORGANIC_ID'),1,'0'))
  processed_df=processed_df.withColumn('DSS_BANNER_ID', regexp_replace(col("DSS_BANNER_ID"), " ", ""))
  # Adding new columns to dataframe
  processed_df=processed_df.withColumn("INSERT_ID",lit(pipelineid))
  processed_df=processed_df.withColumn("INSERT_TIMESTAMP",current_timestamp())
  processed_df=processed_df.withColumn("LAST_UPDATE_ID",lit(pipelineid))
  processed_df=processed_df.withColumn("LAST_UPDATE_TIMESTAMP",current_timestamp())
  
  loggerAtt.info("Product Transformation function ended")
  return processed_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### UDF

# COMMAND ----------

def flagChange(s):
  if s == 'Y':
    return 1
  elif s == 'N':
    return 0
  else:
    return None

flagChangeUDF = udf(flagChange) 

def formatZeros(s):
  if s is not None:
    return format(s, '.2f')
  else:
    return s

formatZerosUDF = udf(formatZeros) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Transformation for unified item master table

# COMMAND ----------

def itemMasterTransformation(processed_df):
  processed_df=processed_df.withColumn('SMA_COMM', col('SMA_COMM').substr(1,3).cast(IntegerType()))
  processed_df=processed_df.withColumn('SMA_SUB_COMM', col('SMA_SUB_COMM').substr(1,3).cast(IntegerType()))
  processed_df=processed_df.withColumn('SMA_NAT_ORG_IND', col('SMA_NAT_ORG_IND').cast(StringType()))
#   processed_df=processed_df.withColumn('SMA_LEGACY_ITEM', lpad(col('SMA_LEGACY_ITEM'),12,'0'))
  processed_df=processed_df.withColumn('SMA_ITEM_SIZE', col('SMA_ITEM_SIZE').cast(DecimalType()))
  processed_df=processed_df.withColumn('SMA_ITEM_SIZE', (col('SMA_ITEM_SIZE')/100))
#   processed_df=processed_df.withColumn('SMA_ITEM_SIZE', lpad(regexp_replace(format_number(col('SMA_ITEM_SIZE'), 2), ",", ""),4,'0'))
  processed_df=processed_df.withColumn('SMA_ITEM_SIZE', formatZerosUDF(round(col('SMA_ITEM_SIZE'), 2)))
  processed_df=processed_df.withColumn('SMA_ITEM_SIZE', lpad(col('SMA_ITEM_SIZE'),6,'0'))
  processed_df=processed_df.withColumn('SMA_ITEM_TYPE', lpad(col('SMA_ITEM_TYPE'),6,'0'))
  processed_df=processed_df.withColumn('SMA_RETL_UOM', col('SMA_ITEM_UOM'))
  processed_df=processed_df.withColumn('SMA_RETL_MULT_UOM', col('SMA_ITEM_UOM'))
  processed_df=processed_df.withColumn('SMA_GUIDING_STARS', col('SMA_GUIDING_STARS').cast(IntegerType()))
#   processed_df=processed_df.withColumn('AVG_WGT', col('AVG_WGT').cast(FloatType()))
#   processed_df=processed_df.withColumn('AVG_WGT', (col('AVG_WGT')).cast(StringType()))
#   processed_df=processed_df.withColumn('AVG_WGT', regexp_replace(col("AVG_WGT"), " ", ""))
#   processed_df=processed_df.withColumn('AVG_WGT', lpad(col('AVG_WGT'),7,'0'))
  #processed_df=processed_df.withColumn('AVG_WGT', lpad(regexp_replace(format_number(col('AVG_WGT'), 4), ",", ""),9,'0'))
  processed_df=processed_df.withColumn('DSS_AVG_UNIT_WT', col('AVG_WGT'))
  processed_df=processed_df.withColumn('SMA_VENDOR_PACK', col('SMA_VENDOR_PACK').cast(IntegerType()))
  processed_df=processed_df.withColumn('DSS_GROUP_TYPE', col('SMA_ITEM_TYPE'))
#   processed_df=processed_df.withColumn('SMA_FSA_IND', flagChangeUDF(col('SMA_FSA_IND')).cast(IntegerType()))
  return processed_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### ABC Framework Check Function

# COMMAND ----------

def abcFramework(headerFooterRecord, product_duplicate_records, product_raw_df):
  loggerAtt.info("ABC Framework function initiated")
  try:
    ABC(BOFEOFCheck=1)
    
    # Fetching all product id records with non integer value
    headerFooterRecord = product_raw_df.filter(typeCheckUDF(col('DSS_PRODUCT_ID')) == False)
    
    # Fetching EOF and BOF records and checking whether it is of length one each
    eofDf = headerFooterRecord.filter(product_raw_df["DSS_PRODUCT_ID"].rlike("^:EOF"))
    bofDf = headerFooterRecord.filter(product_raw_df["DSS_PRODUCT_ID"].rlike("^:BOF"))
    
    loggerAtt.info("EOF file record count is " + str(eofDf.count()))
    loggerAtt.info("BOF file record count is " + str(bofDf.count()))
    EOFcnt=eofDf.count()
    ABC(EOFCount=EOFcnt)
    # If there are multiple header/Footer then the file is invalid
    if ((eofDf.count() != 1) or (bofDf.count() != 1)):
      raise Exception('Error in EOF or BOF value')
    
    productDf = product_raw_df.filter(typeCheckUDF(col('DSS_PRODUCT_ID')) == True)
    
    fileRecordCount = int(eofDf.select('DSS_PRODUCT_ID').toPandas()['DSS_PRODUCT_ID'][0].split()[1])
    
    actualRecordCount = int(productDf.count() + headerFooterRecord.count() - 2)
    
    loggerAtt.info("Record Count mentioned in file: " + str(fileRecordCount))
    loggerAtt.info("Actual no of record in file: " + str(actualRecordCount))
    
    # Checking to see if the count matched the actual record count
    if actualRecordCount != fileRecordCount:
      raise Exception('Record count mismatch. Actual count ' + str(actualRecordCount) + ', file record count ' + str(fileRecordCount))
     
  except Exception as ex:
    ABC(BOFEOFCheck=0)
    EOFcnt = Null
    ABC(EOFCount=0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'ABC Framework check Error')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
  try:
    # Exception handling of schema records
    product_nullRows = productDf.where(reduce(lambda x, y: x | y, (col(x).isNull() for x in productDf.columns)))
    ABC(NullValueCheck=1)
    ABC(DropNACheck = 1)
    loggerAtt.info("Dimension of the Null records:("+str(product_nullRows.count())+"," +str(len(product_nullRows.columns))+")")
    ABC(NullValuCount = product_nullRows.count())
    product_raw_dfWithNoNull = productDf.na.drop()

    loggerAtt.info("Dimension of the Not null records:("+str(product_raw_dfWithNoNull.count())+"," +str(len(product_raw_dfWithNoNull.columns))+")")

    # removing duplicate record 
    ABC(DuplicateValueCheck = 1)
    if (product_raw_dfWithNoNull.groupBy(product_raw_dfWithNoNull.columns).count().filter("count > 1").count()) > 0:
      loggerAtt.info("Duplicate records exists")
      product_rawdf_withCount = product_raw_dfWithNoNull.groupBy(product_raw_dfWithNoNull.columns).count();
      product_duplicate_records = product_rawdf_withCount.filter("count > 1").drop('count')
      DuplicateValueCnt = product_duplicate_records.count()
      ABC(DuplicateValueCount=DuplicateValueCnt)
      productRecords= product_rawdf_withCount.filter("count == 1").drop('count')
      loggerAtt.info("Duplicate record Exists. No of duplicate records are " + str(product_duplicate_records.count()))
    else:
      loggerAtt.info("No duplicate records")
      ABC(DuplicateValueCount=0)
      productRecords = product_raw_dfWithNoNull
    
    loggerAtt.info("ABC Framework function ended")
    return headerFooterRecord, product_nullRows, product_duplicate_records, productRecords
  except Exception as ex:
    ABC(NullValueCheck=0)
    ABC(DropNACheck = 0)
    ABC(NullValuCount = "")
    ABC(DuplicateValueCheck = 0)
    ABC(DuplicateValueCount='')
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'ABC Framework check Error')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Writing Invalid records

# COMMAND ----------

def writeInvalidRecords(product_invalidRecords, Invalid_RecordsPath):
  try:
    if product_invalidRecords.count() > 0:
      product_invalidRecords.write.mode('Append').format('parquet').save(Invalid_RecordsPath + "/" +Date+ "/" + "Product_Invalid_Data")
      loggerAtt.info('======== Invalid Product Records write operation finished ========')

  except Exception as ex:
    ABC(InvalidRecordSaveCheck = 0)
    loggerAtt.error(str(ex))
    loggerAtt.info('======== Invalid record file write to ADLS location failed ========')
    err = ErrorReturn('Error', ex,'Writeoperationfailed for Invalid Record')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Product Renaming dictionary

# COMMAND ----------

product_Renaming = {
                    "DSS_SUPER_CATEGORY_ID":"SMA_COMM",
                    "DSS_SUB_CATEGORY_ID":"SMA_SUB_COMM",
                    "DSS_ORGANIC_ID":"SMA_NAT_ORG_IND",
                    "DSS_PRODUCT_SIZE":"SMA_ITEM_SIZE",
                    "DSS_PVT_LBL_FLG":"SMA_PRIV_LABEL",
                    "DSS_PRODUCT_CASE_PACK":"SMA_VENDOR_PACK",
                    "DSS_UOM":"SMA_ITEM_UOM",
                    "DSS_GROUP_TYPE":"SMA_ITEM_TYPE",
                    "DSS_GUIDING_STAR_RATING":"SMA_GUIDING_STARS",
                    "DSS_AVG_UNIT_WT":"AVG_WGT",
                    "ALT_UPC_FETCH":"DSS_PRODUCT_ID",
                    "BANNER_ID":"DSS_BANNER_ID"
          }

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Product file reading

# COMMAND ----------

def readFile(file_location, infer_schema, first_row_is_header, delimiter,file_type):
  raw_df = spark.read.format(file_type) \
    .option("mode","PERMISSIVE") \
    .option("header", first_row_is_header) \
    .option("dateFormat", "yyyyMMdd") \
    .option("sep", delimiter) \
    .schema(ProductSchema) \
    .load(file_location)
  
  ABC(ReadDataCheck=1)
  RawDataCount = raw_df.count()
  ABC(RawDataCount=RawDataCount)
  loggerAtt.info("Raw count check initiated")
  loggerAtt.info(f"Count of Records in the File: {RawDataCount}")
  return raw_df


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SQL table functions

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Update/Insert record into function

# COMMAND ----------

def upsertProductRecords(product_valid_Records):
  loggerAtt.info("Merge into Delta table initiated")
  try:
    temp_table_name = "productRecords"
    product_valid_Records.createOrReplaceTempView(temp_table_name)
    
    initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(ProductDeltaPath))
    loggerAtt.info(f"Initial count of records in Delta Table: {initial_recs.head(1)}")
    initial_recs = initial_recs.head(1)
    
    ABC(DeltaTableInitCount=initial_recs[0][0])
    spark.sql('''MERGE INTO delta.`{}` as productDelta
    USING productRecords 
    ON productDelta.DSS_PRODUCT_ID = productRecords.DSS_PRODUCT_ID and
       productDelta.DSS_BANNER_ID = productRecords.DSS_BANNER_ID
    WHEN MATCHED Then 
            Update Set  productDelta.DSS_PRODUCT_DESC=productRecords.DSS_PRODUCT_DESC,
                        productDelta.DSS_PRIMARY_ID=productRecords.DSS_PRIMARY_ID,
                        productDelta.DSS_PRIMARY_DESC=productRecords.DSS_PRIMARY_DESC,
                        productDelta.DSS_SUB_CATEGORY_ID=productRecords.DSS_SUB_CATEGORY_ID,
                        productDelta.DSS_SUB_CATEGORY_DESC=productRecords.DSS_SUB_CATEGORY_DESC,
                        productDelta.DSS_CATEGORY_ID=productRecords.DSS_CATEGORY_ID,
                        productDelta.DSS_CATEGORY_DESC=productRecords.DSS_CATEGORY_DESC,
                        productDelta.DSS_SUPER_CATEGORY_ID=productRecords.DSS_SUPER_CATEGORY_ID,
                        productDelta.DSS_SUPER_CATEGORY_DESC=productRecords.DSS_SUPER_CATEGORY_DESC,
                        productDelta.DSS_ALL_CATEGORY_ID=productRecords.DSS_ALL_CATEGORY_ID,
                        productDelta.DSS_ALL_CATEGORY_DESC=productRecords.DSS_ALL_CATEGORY_DESC,
                        productDelta.DSS_MDSE_PROGRAM_ID=productRecords.DSS_MDSE_PROGRAM_ID,
                        productDelta.DSS_MDSE_PROGRAM_DESC=productRecords.DSS_MDSE_PROGRAM_DESC,
                        productDelta.DSS_PRICE_MASTER_ID=productRecords.DSS_PRICE_MASTER_ID,
                        productDelta.DSS_PRICE_MASTER_DESC=productRecords.DSS_PRICE_MASTER_DESC,
                        productDelta.DSS_BUYER_ID=productRecords.DSS_BUYER_ID,
                        productDelta.DSS_BUYER_DESC=productRecords.DSS_BUYER_DESC,
                        productDelta.DSS_PRICE_SENS_ID=productRecords.DSS_PRICE_SENS_ID,
                        productDelta.DSS_PRICE_SENS_SHORT_DESC=productRecords.DSS_PRICE_SENS_SHORT_DESC,
                        productDelta.DSS_PRICE_SENS_LONG_DESC=productRecords.DSS_PRICE_SENS_LONG_DESC,
                        productDelta.DSS_MANCODE_ID=productRecords.DSS_MANCODE_ID,
                        productDelta.DSS_MANCODE_DESC=productRecords.DSS_MANCODE_DESC,
                        productDelta.DSS_PRIVATE_BRAND_ID=productRecords.DSS_PRIVATE_BRAND_ID,
                        productDelta.DSS_PRIVATE_BRAND_DESC=productRecords.DSS_PRIVATE_BRAND_DESC,
                        productDelta.DSS_PRODUCT_STATUS_ID=productRecords.DSS_PRODUCT_STATUS_ID,
                        productDelta.DSS_PRODUCT_STATUS_DESC=productRecords.DSS_PRODUCT_STATUS_DESC,
                        productDelta.DSS_PRODUCT_SIZE=productRecords.DSS_PRODUCT_SIZE,
                        productDelta.DSS_PRODUCT_UOM_ID=productRecords.DSS_PRODUCT_UOM_ID,
                        productDelta.DSS_PRODUCT_UOM_DESC=productRecords.DSS_PRODUCT_UOM_DESC,
                        productDelta.DSS_PRODUCT_PACK_QTY=productRecords.DSS_PRODUCT_PACK_QTY,
                        productDelta.DSS_DC_ITEM_NUMBER=productRecords.DSS_DC_ITEM_NUMBER,
                        productDelta.DSS_DIRECTOR_ID=productRecords.DSS_DIRECTOR_ID,
                        productDelta.DSS_DIRECTOR_DESC=productRecords.DSS_DIRECTOR_DESC,
                        productDelta.DSS_DIRECTOR_GROUP_DESC=productRecords.DSS_DIRECTOR_GROUP_DESC,
                        productDelta.DSS_MAJOR_CATEGORY_ID=productRecords.DSS_MAJOR_CATEGORY_ID,
                        productDelta.DSS_MAJOR_CATEGORY_DESC=productRecords.DSS_MAJOR_CATEGORY_DESC,
                        productDelta.DSS_PLANOGRAM_ID=productRecords.DSS_PLANOGRAM_ID,
                        productDelta.DSS_PLANOGRAM_DESC=productRecords.DSS_PLANOGRAM_DESC,
                        productDelta.DSS_HEIGHT=productRecords.DSS_HEIGHT,
                        productDelta.DSS_WIDTH=productRecords.DSS_WIDTH,
                        productDelta.DSS_DEPTH=productRecords.DSS_DEPTH,
                        productDelta.DSS_BRAND_ID=productRecords.DSS_BRAND_ID,
                        productDelta.DSS_BRAND_DESC=productRecords.DSS_BRAND_DESC,
                        productDelta.DSS_LIFO_POOL_ID=productRecords.DSS_LIFO_POOL_ID,
                        productDelta.DSS_GROUP_TYPE=productRecords.DSS_GROUP_TYPE,
                        productDelta.DSS_DATE_UPDATED=productRecords.DSS_DATE_UPDATED,
                        productDelta.DSS_DATE_ADDED=productRecords.DSS_DATE_ADDED,
                        productDelta.DSS_EQUIVALENT_SIZE_ID=productRecords.DSS_EQUIVALENT_SIZE_ID,
                        productDelta.DSS_EQUIVALENT_SIZE_DESC=productRecords.DSS_EQUIVALENT_SIZE_DESC,
                        productDelta.DSS_EQUIV_SIZE=productRecords.DSS_EQUIV_SIZE,
                        productDelta.DSS_SCAN_TYPE_ID=productRecords.DSS_SCAN_TYPE_ID,
                        productDelta.DSS_SCAN_TYPE_DESC=productRecords.DSS_SCAN_TYPE_DESC,
                        productDelta.DSS_PRODUCT_CASE_PACK=productRecords.DSS_PRODUCT_CASE_PACK,
                        productDelta.DSS_STORE_HANDLING_CODE=productRecords.DSS_STORE_HANDLING_CODE,
                        productDelta.DSS_SHC_DESC=productRecords.DSS_SHC_DESC,
                        productDelta.DSS_GUIDING_STAR_RATING=productRecords.DSS_GUIDING_STAR_RATING,
                        productDelta.DSS_GS_RATING_DESC=productRecords.DSS_GS_RATING_DESC,
                        productDelta.DSS_GS_DATE_RATED=productRecords.DSS_GS_DATE_RATED,
                        productDelta.DSS_PRIVATE_LABEL_ID=productRecords.DSS_PRIVATE_LABEL_ID,
                        productDelta.DSS_HBC_ITEM_NUMBER=productRecords.DSS_HBC_ITEM_NUMBER,
                        productDelta.DSS_PRIVATE_LABEL_DESC=productRecords.DSS_PRIVATE_LABEL_DESC,
                        productDelta.DSS_ORGANIC_ID=productRecords.DSS_ORGANIC_ID,
                        productDelta.DSS_PRODUCT_DIMESION_L_BYTE=productRecords.DSS_PRODUCT_DIMESION_L_BYTE,
                        productDelta.DSS_PRDT_NAME=productRecords.DSS_PRDT_NAME,
                        productDelta.DSS_MFR_NAME=productRecords.DSS_MFR_NAME,
                        productDelta.DSS_BRAND_NAME=productRecords.DSS_BRAND_NAME,
                        productDelta.DSS_KEYWORD=productRecords.DSS_KEYWORD,
                        productDelta.DSS_META_DESC=productRecords.DSS_META_DESC,
                        productDelta.DSS_META_KEYWORD=productRecords.DSS_META_KEYWORD,
                        productDelta.DSS_PROMO_CODE=productRecords.DSS_PROMO_CODE,
                        productDelta.DSS_SRVG_SZ_DSC=productRecords.DSS_SRVG_SZ_DSC,
                        productDelta.DSS_SRVG_SZ_UOM_DSC=productRecords.DSS_SRVG_SZ_UOM_DSC,
                        productDelta.DSS_SRVG_PER_CTNR=productRecords.DSS_SRVG_PER_CTNR,
                        productDelta.DSS_NEW_PRDT_FLG=productRecords.DSS_NEW_PRDT_FLG,
                        productDelta.DSS_FSA_FLG=productRecords.DSS_FSA_FLG,
                        productDelta.DSS_ACTIVE_FLG=productRecords.DSS_ACTIVE_FLG,
                        productDelta.DSS_OVRLN_DSC=productRecords.DSS_OVRLN_DSC,
                        productDelta.DSS_ALT_SRVG_SZ_DSC=productRecords.DSS_ALT_SRVG_SZ_DSC,
                        productDelta.DSS_ALT_SRVG_SZ_UOM=productRecords.DSS_ALT_SRVG_SZ_UOM,
                        productDelta.DSS_AVG_UNIT_WT=productRecords.DSS_AVG_UNIT_WT,
                        productDelta.DSS_UOM=productRecords.DSS_UOM,
                        productDelta.DSS_PRIVATE_BRAND_CD=productRecords.DSS_PRIVATE_BRAND_CD,
                        productDelta.DSS_MENU_LBL_FLG=productRecords.DSS_MENU_LBL_FLG,
                        productDelta.DSS_PVT_LBL_FLG=productRecords.DSS_PVT_LBL_FLG,
                        productDelta.LAST_UPDATE_ID=productRecords.LAST_UPDATE_ID,
                        productDelta.LAST_UPDATE_TIMESTAMP=productRecords.LAST_UPDATE_TIMESTAMP
                  WHEN NOT MATCHED THEN INSERT * '''.format(ProductDeltaPath))
    appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(ProductDeltaPath))
    loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
    appended_recs = appended_recs.head(1)
    ABC(DeltaTableFinalCount=appended_recs[0][0])
    spark.catalog.dropTempView(temp_table_name)
  except Exception as ex:
    loggerAtt.info("Merge into Delta table failed and throwed error")
    loggerAtt.error(str(ex))
    ABC(DeltaTableInitCount='')
    ABC(DeltaTableFinalCount='')
    err = ErrorReturn('Error', ex,'MergeDeltaTable')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  loggerAtt.info("Merge into Delta table successful")    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Fetching Unified Item records

# COMMAND ----------

def itemMasterRecordsModified(itemTempEffDeltaPath, productEffDeltaPath, ProductDeltaPath, Date, unifiedProductFields):
  loggerAtt.info("itemMasterRecords fetch initiated")
  productEffTemp = spark.sql('''DELETE FROM delta.`{}`'''.format(productEffDeltaPath))
  productDelta = spark.read.format('delta').load(ProductDeltaPath)
  productDelta = productDelta.withColumn('DSS_BANNER_ID', regexp_replace(col("DSS_BANNER_ID"), " ", ""))
  itemTempEffDelta = spark.read.format('delta').load(itemTempEffDeltaPath)

  itemTempEffDelta = spark.sql('''select BANNER_ID, ALT_UPC_FETCH from delta.`{}` group by BANNER_ID, ALT_UPC_FETCH'''.format(itemTempEffDeltaPath))
  itemTempEffDelta = itemTempEffDelta.withColumn('BANNER_ID', regexp_replace(col("BANNER_ID"), " ", ""))
  
  productEffTemp = itemTempEffDelta.join(productDelta, [itemTempEffDelta.ALT_UPC_FETCH == productDelta.DSS_PRODUCT_ID, itemTempEffDelta.BANNER_ID == productDelta.DSS_BANNER_ID], how='left').select([col(xx) for xx in unifiedProductFields])
  
  productEffTemp = quinn.with_some_columns_renamed(itemEff_promotable, itemEff_change_col_name)(productEffTemp)
  productEffTemp = itemMasterTransformation(productEffTemp)
  productEffTemp.write.partitionBy('DSS_BANNER_ID').format('delta').mode('append').save(productEffDeltaPath)
  ABC(UnifiedRecordFeedCount = productEffTemp.count())
  ABC(UnifiedRecordItemCount = itemTempEffDelta.count())
  loggerAtt.info("UnifiedRecordFeedCount:" +str(productEffTemp.count()))
  loggerAtt.info("UnifiedRecordItemCount:" +str(itemTempEffDelta.count()))
  loggerAtt.info("itemMasterRecords fetch successful")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Main file processing

# COMMAND ----------



if __name__ == "__main__":
  
  ## File reading parameters
  loggerAtt.info('======== Input Product file processing initiated ========')
  
  if POSemergencyFlag == 'Y':
    loggerAtt.info('Feed processing happening for POS Emergency feed')
    try:
      ABC(UnifiedRecordFetch = 1)
      itemMasterRecordsModified(itemTempEffDeltaPath, productEffDeltaPath, ProductDeltaPath, Date, unifiedProductFields)
    except Exception as ex:
      ABC(UnifiedRecordFetch = 0)
      ABC(UnifiedRecordFeedCount = "")
      ABC(UnifiedRecordItemCount = "")
      loggerAtt.error(ex)
      err = ErrorReturn('Error', ex,'itemMasterRecordsModified')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))  
  else:
    loggerAtt.info('Feed processing happening for POS Daile/Weekly feed')
    ## Step1: Reading input file
    try:
      product_raw_df = readFile(file_location, infer_schema, first_row_is_header, delimiter, file_type)

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

    if product_raw_df is not None:
      ## Step2: Performing ABC framework check
      headerFooterRecord, product_nullRows, product_duplicate_records, productRecords = abcFramework(headerFooterRecord, product_duplicate_records, product_raw_df)

      ## Step3: Performing Transformation
      try:  
        ABC(RenamingCheck=1)
        if productRecords is not None: 
          product_valid_Records = productTransformation(productRecords, PipelineID)
      except Exception as ex:
        ABC(TransformationCheck = 0)
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'productTransformation')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))

      ## Step4: Combining all duplicate records
      try:
        ## Combining Duplicate, null record
        ABC(InvalidRecordSaveCheck = 1)
        if product_duplicate_records is not None:
          invalidRecordsList = [headerFooterRecord, product_duplicate_records, product_nullRows]
          product_invalidRecords = reduce(DataFrame.unionAll, invalidRecordsList)
        else:
          invalidRecordsList = [headerFooterRecord, product_nullRows]
          product_invalidRecords = reduce(DataFrame.unionAll, invalidRecordsList)
        ABC(InvalidRecordCount = product_invalidRecords.count())
      except Exception as ex:
        ABC(InvalidRecordSaveCheck = 0)
        ABC(InvalidRecordCount = "")
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'Grouping Invalid record function')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))   

      loggerAtt.info("No of valid records: " + str(product_valid_Records.count()))
      loggerAtt.info("No of invalid records: " + str(product_invalidRecords.count()))

      ## Step 5: Delta table creation
      deltaCreator(ProductDeltaPath)

      ## Step 6: Merging records into delta table
      if product_valid_Records is not None:
          upsertProductRecords(product_valid_Records)

      ## Step 7: Writing Invalid records to ADLS location
      if product_invalidRecords is not None:
        writeInvalidRecords(product_invalidRecords, Invalid_RecordsPath)

      ## Step 8: Fetching records for Unified Item 
      try:
        ABC(UnifiedRecordFetch = 1)
        itemMasterRecordsModified(itemTempEffDeltaPath, productEffDeltaPath, ProductDeltaPath, Date, unifiedProductFields)
      except Exception as ex:
        ABC(UnifiedRecordFetch = 0)
        ABC(UnifiedRecordFeedCount = "")
        ABC(UnifiedRecordItemCount = "")
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'itemMasterRecordsModified')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))  

    else:
      loggerAtt.info("Error in input file reading")
    
    
    
loggerAtt.info('======== Input product file processing ended ========')

# COMMAND ----------

temp_table_name = "product_valid_Records"
product_valid_Records.createOrReplaceTempView(temp_table_name)

temp_table_name = "productRecords"
productRecords.createOrReplaceTempView(temp_table_name)
display(product_valid_Records)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select DSS_BANNER_ID, DSS_PRODUCT_ID, DSS_AVG_UNIT_WT from product_valid_Records where DSS_PRODUCT_ID=00000000031180

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select DSS_BANNER_ID, DSS_PRODUCT_ID, DSS_AVG_UNIT_WT from productDeltaTable where DSS_PRODUCT_ID=00000000031180

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