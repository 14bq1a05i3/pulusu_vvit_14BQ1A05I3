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

# MAGIC %sql
# MAGIC 
# MAGIC set spark.sql.adaptive.enabled = true;
# MAGIC set spark.sql.adaptive.coalescePartitions.enabled=true;
# MAGIC set spark.sql.adaptive.skewJoin.enabled=true;
# MAGIC set spark.databricks.delta.optimize.zorder.checkStatsCollection.enabled=true;

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##Calling Logger

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/Logging

# COMMAND ----------

custom_logfile_Name ='item_Mastermaintainence_customlog'
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
dbutils.widgets.text("itemMasterOutboundPath","")
dbutils.widgets.text("logFilesPath","")
dbutils.widgets.text("invalidRecordsPath","")
dbutils.widgets.text("clientId","")
dbutils.widgets.text("keyVaultName","")
dbutils.widgets.text("promoLinkingDeltaPath","")
dbutils.widgets.text("itemMasterOutboundPath","")
dbutils.widgets.text("couponDeltaPath","")
dbutils.widgets.text("promoLinkingMainDeltaPath","")
dbutils.widgets.text("storeDeltaPath","")
dbutils.widgets.text("taxHolidayDeltaPath","")
dbutils.widgets.text("priceChangeDeltaPath","")
dbutils.widgets.text("itemMainDeltaPath","")
dbutils.widgets.text("CurrentFileDate","")

FileName=dbutils.widgets.get("fileName")
Filepath=dbutils.widgets.get("filePath")
inputDirectory=dbutils.widgets.get("directory")
outputDirectory=dbutils.widgets.get("outputDirectory")
container=dbutils.widgets.get("Container")
PipelineID=dbutils.widgets.get("pipelineID")
mount_point=dbutils.widgets.get("MountPoint")
itemMasterDeltaPath=dbutils.widgets.get("deltaPath")
Archivalfilelocation=dbutils.widgets.get("archivalFilePath")
Item_OutboundPath=dbutils.widgets.get("itemMasterOutboundPath")
Log_FilesPath=dbutils.widgets.get("logFilesPath")
Invalid_RecordsPath=dbutils.widgets.get("invalidRecordsPath")
Date = datetime.datetime.now(timezone("America/Halifax")).strftime("%Y-%m-%d")
file_location = '/mnt' + '/' + inputDirectory + '/' + Filepath +'/' + FileName 
clientId=dbutils.widgets.get("clientId")
keyVaultName=dbutils.widgets.get("keyVaultName")
PromotionLinkDeltaPath = dbutils.widgets.get("promoLinkingDeltaPath")
itemMasterOutboundPath = dbutils.widgets.get("itemMasterOutboundPath")
couponDeltaPath = dbutils.widgets.get("couponDeltaPath")

CurrentFileDate = dbutils.widgets.get("CurrentFileDate")
PromotionMainDeltaPath = dbutils.widgets.get("promoLinkingMainDeltaPath")

storeDeltaPath = dbutils.widgets.get("storeDeltaPath")
taxHolidayDeltaPath = dbutils.widgets.get("taxHolidayDeltaPath")

PriceChangeDeltaPath = dbutils.widgets.get("priceChangeDeltaPath")
ItemMainDeltaPath = dbutils.widgets.get("itemMainDeltaPath")


Date_serial = datetime.datetime.now(timezone("America/Halifax")).strftime("%Y%m%d")

promoMainArchival = Archivalfilelocation + "/" +Date+ "/" + "promoMainArchival"

promoLinkArchival = Archivalfilelocation + "/" +Date+ "/" + "promoLinkArchival"

priceChangeArchival = Archivalfilelocation + "/" +Date+ "/" + "priceChangeArchival"

itemMasterArchival = Archivalfilelocation + "/" +Date+ "/" + "itemMasterArchival"

itemMasterArchival = Archivalfilelocation + "/" +Date+ "/" + "itemMasterArchival"

itemMainArchival = Archivalfilelocation + "/" +Date+ "/" + "itemMainArchival"

taxHolidayArchival = Archivalfilelocation + "/" +Date+ "/" + "taxHolidayArchival"

# promoErrorPath = Archivalfilelocation + "/" +Date+ "/" + "promoErrorData"

# productRecallErrorPath = Archivalfilelocation + "/" +Date+ "/" + "productRecallErrorData"

# invalidRecordsPath = Archivalfilelocation + "/" +Date+ "/" + "invalidRecords"



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
item_transformed_df = None
raw_df = None
item_duplicate_records = None
invalid_transformed_df = None
item_productrecall = None
nonItemRecallFile = None
item_nomaintainence = None
recall_error_df =None
PipelineID= str(PipelineID)
currentTimeStamp = datetime.datetime.now().timestamp()

p_filename = "item_dailymaintainence_custom_log"
folderDate = Date

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

date_time_str = CurrentFileDate

date_time_obj = datetime.datetime.strptime(date_time_str, '%Y/%m/%d')

if processing_file != 'COD':
  d = date_time_obj - datetime.timedelta(days=1)
else:
  d = date_time_obj

currentDate = date_time_obj.date().isoformat()
previousDate = d.date().isoformat()

print ("The type of the date is now",  type(date_time_obj))
print ("The date is", currentDate)
print(previousDate)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Delta table creation

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Item Master

# COMMAND ----------

# spark.sql("""DELETE FROM itemMasterAhold;""")
# spark.sql("""DROP TABLE IF EXISTS itemMasterAhold;""")
# dbutils.fs.rm('/mnt/centralized-price-promo/Ahold/Outbound/SDM/Item_Master_delta',recurse=True)

# COMMAND ----------

try:
  ABC(DeltaTableCreateCheck=1)
  spark.sql(""" CREATE TABLE IF NOT EXISTS itemMasterAhold (
                AVG_WGT STRING,
                BTL_DPST_AMT STRING,
                HOW_TO_SELL STRING,
                PERF_DETL_SUB_TYPE STRING,
                SALE_PRICE STRING,
                SALE_QUANTITY STRING,
                SMA_ACTIVATION_CODE STRING,
                SMA_BATCH_SERIAL_NBR LONG,
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
                SMA_LINK_APPLY_DATE DATE,
                SMA_LINK_APPLY_TIME STRING,
                SMA_LINK_CHG_TYPE STRING,
                SMA_LINK_END_DATE DATE,
                SMA_LINK_FAMCD_PROMOCD STRING,
                SMA_LINK_HDR_COUPON LONG,
                SMA_LINK_HDR_LOCATION STRING,
                SMA_LINK_HDR_MAINT_TYPE STRING,
                SMA_LINK_ITEM_NBR STRING,
                SMA_LINK_OOPS_ADWK STRING,
                SMA_LINK_OOPS_FILE_ID STRING,
                SMA_LINK_RCRD_TYPE STRING,
                SMA_LINK_START_DATE DATE,
                SMA_LINK_SYS_DIGIT STRING,
                SMA_LINK_TYPE STRING,
                SMA_LINK_UPC LONG,
                SMA_PL_UPC_REDEF LONG,
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
                SMA_RETL_MULT_UNIT FLOAT,
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
                SMA_ITM_EFF_DATE DATE,
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
  """.format(itemMasterDeltaPath))

  ## Performance
  
  spark.sql("""OPTIMIZE itemMasterAhold ZORDER BY (SMA_DEST_STORE)""")
    
  
except Exception as ex:
  ABC(DeltaTableCreateCheck = 0)
  loggerAtt.error(ex)
  err = ErrorReturn('Error', ex,'deltaCreator itemMasterDeltaPath')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------



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
      EFF_TS STRING,
      INSERT_ID STRING,
      INSERT_TIMESTAMP TIMESTAMP,
      LAST_UPDATE_ID STRING,
      LAST_UPDATE_TIMESTAMP TIMESTAMP
    )
    USING delta
    Location '{}'
    PARTITIONED BY (SMA_STORE)
    """.format(ItemMainDeltaPath))
    
    
      
  ## Performance
  
    spark.sql("""OPTIMIZE itemMainAhold ZORDER BY (SMA_DEST_STORE)""")
    
except Exception as ex:
    ABC(DeltaTableCreateCheck = 0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'deltaCreator')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson)) 

# Need to add in the new delta table
#   SALE_PRICE STRING,
#   SALE_MULT STRING,
#   PERF_DETL_SUB_TYPE STRING,
#   PROMO_START_DATE DATE,
#   PROMO_END_DATE DATE,

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
    
          
  ## Performance
  
    spark.sql("""OPTIMIZE PriceChange ZORDER BY (SMA_PRICE_CHG_ID)""")
    
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
    
   ## Performance
  
    spark.sql("""OPTIMIZE PromotionLink ZORDER BY (SMA_LINK_HDR_COUPON)""")   
    
    
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
    
    
        
   ## Performance
  
    spark.sql("""OPTIMIZE PromotionMain ZORDER BY (SMA_PROMO_LINK_UPC)""")   
    
    
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
# MAGIC ### Tax Holiday

# COMMAND ----------

try:
    ABC(DeltaTableCreateCheck=1)
    spark.sql("""
    CREATE TABLE IF NOT EXISTS tax_holiday (
      DB_UPC  LONG,
      DB_STORE   STRING,
      DB_HOLIDAY_TYPE  STRING,
      DB_HOLIDAY_BEGIN_DATE  DATE,
      DB_HOLIDAY_END_DATE  DATE,
      DB_HOLIDAY_TAX_1  STRING,
      DB_HOLIDAY_TAX_2  STRING,
      DB_HOLIDAY_TAX_3  STRING,
      DB_HOLIDAY_TAX_4  STRING,
      DB_HOLIDAY_TAX_5  STRING,
      DB_HOLIDAY_TAX_6  STRING,
      DB_HOLIDAY_TAX_7  STRING,
      DB_HOLIDAY_TAX_8  STRING,
      DB_WIC_ALT  STRING,
      DB_WIC_IND  STRING,
      DB_SNAP_IND  STRING,
      DB_HIP_IND  STRING,
      DB_GIFT_CARD_RES  STRING,
      DB_LINK_PLU  STRING,
      INSERT_ID STRING,
      INSERT_TIMESTAMP TIMESTAMP,
      LAST_UPDATE_ID STRING,
      LAST_UPDATE_TIMESTAMP TIMESTAMP
    )
    USING delta
    Location '{}'
    PARTITIONED BY (DB_STORE)
    """.format(taxHolidayDeltaPath))
    
            
   ## Performance
  
    spark.sql("""OPTIMIZE tax_holiday ZORDER BY (DB_UPC)""")   
    
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
# MAGIC ## UDF, Renaming, Processing and Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### User Defined Functions

# COMMAND ----------

date_func =  udf (lambda x: datetime.datetime.strptime(str(x), '%Y%m%d'), DateType())

dateFuncWithHyp =  udf (lambda x: datetime.datetime.strptime(str(x), '%Y-%m-%d'), DateType())

def formatZeros(s):
  if s is not None:
    return format(s, '.2f')
  else:
    return s

formatZerosUDF = udf(formatZeros)  

def salePriceCal(PERF_DETL_SUB_TYPE, NUM_TO_BUY_1, MULT_UNIT_RETL, RETL_MULT_UNIT, CHANGE_AMOUNT_PCT):
  if PERF_DETL_SUB_TYPE == 1:
    sale_price = CHANGE_AMOUNT_PCT
  elif PERF_DETL_SUB_TYPE == 3:
    if RETL_MULT_UNIT == 0:
      sale_price = None
    else:
      sale_price = (MULT_UNIT_RETL/RETL_MULT_UNIT) - CHANGE_AMOUNT_PCT
  elif PERF_DETL_SUB_TYPE == 4:
    if RETL_MULT_UNIT == 0:
      sale_price = None
    else:
      sale_value = (MULT_UNIT_RETL/RETL_MULT_UNIT)
      sale_price = sale_value - (sale_value * ((CHANGE_AMOUNT_PCT*10)/1000))
  elif PERF_DETL_SUB_TYPE == 8:
    sale_price = CHANGE_AMOUNT_PCT
  elif PERF_DETL_SUB_TYPE == 9:
    sale_price = 0
  else:
    sale_price = None
  return sale_price
salePriceCalculationUDF = udf(salePriceCal)   

def saleQuantityCalculation(PERF_DETL_SUB_TYPE, NUM_TO_BUY_1):
  if PERF_DETL_SUB_TYPE == 8 or PERF_DETL_SUB_TYPE == 9:
    return NUM_TO_BUY_1
  elif PERF_DETL_SUB_TYPE == 1 or PERF_DETL_SUB_TYPE == 3 or PERF_DETL_SUB_TYPE == 4:
    return 1
  else:
    return None
saleQuantityCalculationUDF  = udf(saleQuantityCalculation)

def formatZeros5F(s):
  if s is not None:
    return format(s, '.5f')
  else:
    return s

formatZeros5FUDF = udf(formatZeros5F)    


def formatZeros4F(s):
  if s is not None:
    return format(s, '.4f')
  else:
    return s

formatZeros4FUDF = udf(formatZeros4F)


#Column renaming functions 
def itemflat_promotable(s):
    return Item_renaming[s]
def change_col_name(s):
    return s in Item_renaming

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Item Renaming dictionary

# COMMAND ----------

Item_renaming = { "DB-STORE": "SMA_STORE"
 }

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Item Main Transformation

# COMMAND ----------

def itemMainTrans(currItemMain):
  
  currItemMain = currItemMain.withColumn('SMA_CHG_TYPE',lpad(col('SMA_CHG_TYPE'),2,'0').cast(StringType()))
  currItemMain = currItemMain.withColumn('SMA_ORIG_CHG_TYPE',lpad(col('SMA_ORIG_CHG_TYPE'),2,'0').cast(StringType()))
  
  return currItemMain

# COMMAND ----------

def addingNUll(itemMain):
  itemMain=itemMain.withColumn('ALTERNATE_UPC',lit(None).cast(StringType()))
#   itemMain=itemMain.withColumn('BTL_DPST_AMT',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_ALL_CATEGORY_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_ALL_CATEGORY_ID',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_ALT_SRVG_SZ_DSC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_ALT_SRVG_SZ_UOM',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_AVG_UNIT_WT',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_BRAND_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_BRAND_ID',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_BRAND_NAME',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_BUYER_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_BUYER_ID',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_CATEGORY_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_CATEGORY_ID',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_DATE_UPDATED',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_DEPTH',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_DIRECTOR_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_DIRECTOR_GROUP_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_DIRECTOR_ID',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_EQUIV_SIZE',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_EQUIVALENT_SIZE_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_EQUIVALENT_SIZE_ID',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_GROUP_TYPE',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_GS_DATE_RATED',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_GS_RATING_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_HEIGHT',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_KEYWORD',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_LIFO_POOL_ID',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_MAJOR_CATEGORY_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_MAJOR_CATEGORY_ID',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_MANCODE_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_MANCODE_ID',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_MDSE_PROGRAM_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_MDSE_PROGRAM_ID',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_MENU_LBL_FLG',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_META_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_META_KEYWORD',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_MFR_NAME',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_NEW_PRDT_FLG',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_OVRLN_DSC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_PLANOGRAM_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_PLANOGRAM_ID',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_PRDT_NAME',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_PRICE_MASTER_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_PRICE_MASTER_ID',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_PRICE_SENS_ID',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_PRICE_SENS_LONG_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_PRICE_SENS_SHORT_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_PRIMARY_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_PRIMARY_ID',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_PRIVATE_BRAND_CD',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_PRIVATE_BRAND_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_PRIVATE_BRAND_ID',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_PRIVATE_LABEL_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_PRIVATE_LABEL_ID',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_PRODUCT_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_PRODUCT_DIMESION_L_BYTE',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_PRODUCT_PACK_QTY',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_PRODUCT_STATUS_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_PRODUCT_STATUS_ID',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_PRODUCT_UOM_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_SCAN_TYPE_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_SCAN_TYPE_ID',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_SHC_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_SRVG_PER_CTNR',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_SRVG_SZ_DSC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_SRVG_SZ_UOM_DSC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_STORE_HANDLING_CODE',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_SUB_CATEGORY_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_SUPER_CATEGORY_DESC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('DSS_WIDTH',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('INVENTORY_CODE',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('INVENTORY_INTMD_CATEGORY',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('INVENTORY_MAJOR_CATEGORY',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('INVENTORY_QTY_ON_HAND',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('INVENTORY_SUPER_CATEGORY',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('PERF_DETL_SUB_TYPE',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('PRD2STORE_AGE_CODE',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('PRD2STORE_AGE_FLAG',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('PRD2STORE_BANNER_ID',lit('AHOLD').cast(StringType()))
  itemMain=itemMain.withColumn('PRD2STORE_BUY_QTY',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('PRD2STORE_LMT_QTY',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('PRD2STORE_PROMO_CODE',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('PRD2STORE_SALE_END_DT',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('PRD2STORE_SALE_STRT_DT',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('PRD2STORE_SPLN_AD_CD',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('PRD2STORE_SWAP_SAVE_UPC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('PRD2STORE_TAG_TYP_CD',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('PRD2STORE_WINE_VALUE_FLAG',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('PRICE_END_DATE',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('PRICE_MLT_QUANTITY',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('PRICE_PROMO_RTL_PRC',lit(None).cast(FloatType()))
  itemMain=itemMain.withColumn('PRICE_PROMO_UNIT_PRICE',lit(None).cast(FloatType()))
  itemMain=itemMain.withColumn('PRICE_PROMOTIONAL_QUANTITY',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('PRICE_START_DATE',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('PRICE_UNIT_PRICE',lit(None).cast(FloatType()))
  itemMain=itemMain.withColumn('PRICE_UOM_CD',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('RTX_TYPE',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SALE_PRICE',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('SALE_QUANTITY',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('SCRTX_DET_BLK_GRP',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SCRTX_DET_CENTRAL_ITEM',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SCRTX_DET_COMP_PRC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('SCRTX_DET_COMP_QTY',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SCRTX_DET_COMP_TYPE',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SCRTX_DET_DEA_GRP',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SCRTX_DET_FREQ_SHOP_TYPE',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SCRTX_DET_FREQ_SHOP_VAL',lit(None).cast(FloatType()))
  itemMain=itemMain.withColumn('SCRTX_DET_INTRNL_ID',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SCRTX_DET_MAN_PRC_FG',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SCRTX_DET_MIX_MATCH_CD',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SCRTX_DET_NG_ENTRY_FG',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SCRTX_DET_NON_MDSE_ID',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SCRTX_DET_NON_RX_HEALTH_FG',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SCRTX_DET_OP_CODE',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SCRTX_DET_PLU_BTCH_NBR',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SCRTX_DET_QTY_RQRD_FG',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SCRTX_DET_RCPT_DESCR',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('SCRTX_DET_RSTRCSALE_BRCD_FG',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SCRTX_DET_RX_FG',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SCRTX_DET_SLS_RESTRICT_GRP',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SCRTX_DET_STR_CPN_FG',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SCRTX_DET_VEN_CPN_FG',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SCRTX_DET_WIC_CVV_FG',lit(None).cast(IntegerType()))
  itemMain=itemMain.withColumn('SMA_ITM_EFF_DATE',lit(None).cast(DateType()))
  itemMain=itemMain.withColumn('SMA_LINK_APPLY_DATE',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('SMA_LINK_APPLY_TIME',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('SMA_LINK_CHG_TYPE',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('SMA_LINK_END_DATE',lit(None).cast(DateType()))
  itemMain=itemMain.withColumn('SMA_LINK_FAMCD_PROMOCD',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('SMA_LINK_HDR_COUPON',lit(None).cast(LongType()))
  itemMain=itemMain.withColumn('SMA_LINK_HDR_LOCATION',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('SMA_LINK_HDR_MAINT_TYPE',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('SMA_LINK_ITEM_NBR',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('SMA_LINK_OOPS_ADWK',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('SMA_LINK_OOPS_FILE_ID',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('SMA_LINK_RCRD_TYPE',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('SMA_LINK_START_DATE',lit(None).cast(DateType()))
  itemMain=itemMain.withColumn('SMA_LINK_SYS_DIGIT',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('SMA_LINK_TYPE',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('SMA_PL_UPC_REDEF',lit(None).cast(LongType()))
  itemMain=itemMain.withColumn('SMA_PRICE_CHG_ID',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('SMA_RETL_PRC_ENTRY_CHG',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('SMA_RETL_UOM',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('SMA_SELL_RETL',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('TPRX001_AD_TYP_CD',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('TPRX001_CPN_SRC_CD',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('TPRX001_ITEM_NBR',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('TPRX001_ITEM_PROMO_FLG',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('TPRX001_ITEM_SRC_CD',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('TPRX001_LOYAL_CRD_FLG',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('TPRX001_MDSE_AUTH_FLG',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('TPRX001_MIX_MTCH_FLG',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('TPRX001_PRC_STRAT_CD',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('TPRX001_PROMO_DSC',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('TPRX001_PROMO_TYP_CD',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('TPRX001_RTL_PRC_EFF_DT',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('TPRX001_SBT_FLG',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('TPRX001_SBT_VEND_ID',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('TPRX001_SBT_VEND_NET_CST',lit(None).cast(DecimalType(8,3)))
  itemMain=itemMain.withColumn('TPRX001_SCAN_AUTH_FLG',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('TPRX001_SCAN_DAUTH_DT',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('TPRX001_SCAN_PRVWK_RTL_MLT',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('TPRX001_SCAN_PRVWK_RTL_PRC',lit(None).cast(DecimalType(7,2)))
  itemMain=itemMain.withColumn('TPRX001_SCANPRVDAY_RTL_MLT',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('TPRX001_SCANPRVDAY_RTL_PRC',lit(None).cast(DecimalType(7,2)))
  itemMain=itemMain.withColumn('TPRX001_STORE_SID_NBR',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('TPRX001_TAG_PRV_WK_RTL_MLT',lit(None).cast(StringType()))
  itemMain=itemMain.withColumn('TPRX001_TAG_PRV_WK_RTL_PRC',lit(None).cast(DecimalType(7,2)))
  
  return itemMain

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Price Change Transformation

# COMMAND ----------

def priceChangeTrans(currPriceChange):
  
  currPriceChange = currPriceChange.withColumn('SMA_SELL_RETL', lpad(formatZeros4FUDF(col('SMA_SELL_RETL')),10,'0').cast(StringType()))
    
  currPriceChange = currPriceChange.withColumn("LAST_UPDATE_ID", lit(PipelineID))

  currPriceChange = currPriceChange.withColumn("LAST_UPDATE_TIMESTAMP", lit(currentTimeStamp).cast(TimestampType()))
  
  return currPriceChange
  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Promo Link Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Promo Link Transformation scenario 2

# COMMAND ----------

def promoLinkTransSce2(currpromoLink):
  
  currpromoLink = currpromoLink.withColumn('SMA_LINK_CHG_TYPE',lpad(col('SMA_LINK_CHG_TYPE'),2,'0').cast(StringType()))
  currpromoLink = currpromoLink.withColumn('SMA_CHG_TYPE',lpad(col('SMA_CHG_TYPE'),2,'0').cast(StringType()))
  
  currpromoLink = currpromoLink.withColumn('PERF_DETL_SUB_TYPE',lit(None).cast(StringType()))
  currpromoLink = currpromoLink.withColumn('SALE_PRICE',lit(None).cast(StringType()))
  currpromoLink = currpromoLink.withColumn('SALE_QUANTITY',lit(None).cast(StringType()))
  return currpromoLink

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Promo Link Transformation scenario 1

# COMMAND ----------

def promoLinkTransSce1(currpromoLink):
  
  currpromoLink = currpromoLink.withColumn('SMA_CHG_TYPE',lpad(col('SMA_CHG_TYPE'),2,'0').cast(StringType()))
  currpromoLink = currpromoLink.withColumn('SMA_LINK_HDR_COUPON',lit(None).cast(LongType()))
  currpromoLink = currpromoLink.withColumn('SMA_LINK_HDR_LOCATION',lit(None).cast(StringType()))
  currpromoLink = currpromoLink.withColumn('SMA_LINK_START_DATE',lit(None).cast(DateType()))
  currpromoLink = currpromoLink.withColumn('SMA_LINK_RCRD_TYPE',lit(None).cast(StringType()))
  currpromoLink = currpromoLink.withColumn('SMA_LINK_END_DATE',lit(None).cast(DateType()))
  currpromoLink = currpromoLink.withColumn('SMA_LINK_CHG_TYPE',lit(None).cast(StringType()))
  currpromoLink = currpromoLink.withColumn('SMA_LINK_HDR_MAINT_TYPE',lit(None).cast(StringType()))
  currpromoLink = currpromoLink.withColumn('SMA_LINK_ITEM_NBR',lit(None).cast(StringType()))
  currpromoLink = currpromoLink.withColumn('SMA_LINK_OOPS_ADWK',lit(None).cast(StringType()))
  currpromoLink = currpromoLink.withColumn('SMA_LINK_OOPS_FILE_ID',lit(None).cast(StringType()))
  currpromoLink = currpromoLink.withColumn('SMA_LINK_TYPE',lit(None).cast(StringType()))
  currpromoLink = currpromoLink.withColumn('SMA_LINK_SYS_DIGIT',lit(None).cast(StringType()))
  currpromoLink = currpromoLink.withColumn('SMA_LINK_FAMCD_PROMOCD',lit(None).cast(StringType()))
  currpromoLink = currpromoLink.withColumn('SMA_LINK_APPLY_DATE',lit(None).cast(StringType()))
  currpromoLink = currpromoLink.withColumn('SMA_LINK_APPLY_TIME',lit(None).cast(StringType()))
  currpromoLink = currpromoLink.withColumn('PERF_DETL_SUB_TYPE',lit(None).cast(StringType()))
  currpromoLink = currpromoLink.withColumn('SALE_PRICE',lit(None).cast(StringType()))
  currpromoLink = currpromoLink.withColumn('SALE_QUANTITY',lit(None).cast(StringType()))
  
  return currpromoLink

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Sale Price Calculation

# COMMAND ----------

def salePriceCalculation(currPromo):
  
  currPromo = currPromo.withColumn('SMA_MULT_UNIT_RETL', round(col('SMA_MULT_UNIT_RETL').cast(DoubleType()), 2))
  currPromo = currPromo.withColumn('SMA_RETL_MULT_UNIT', round(col('SMA_RETL_MULT_UNIT').cast(DoubleType()), 2))
  currPromo = currPromo.withColumn('CHANGE_AMOUNT_PCT', round(col('CHANGE_AMOUNT_PCT').cast(DoubleType()), 2))
  
  currPromo = currPromo.withColumn("SALE_PRICE", salePriceCalculationUDF(col('PERF_DETL_SUB_TYPE'), col('NUM_TO_BUY_1'), col('SMA_MULT_UNIT_RETL'), col('SMA_RETL_MULT_UNIT'), col('CHANGE_AMOUNT_PCT')))
    
  currPromo = currPromo.withColumn("SALE_PRICE", floor(col('SALE_PRICE') * 100)/100)
  
  currPromo = currPromo.withColumn('SMA_MULT_UNIT_RETL', col('SMA_MULT_UNIT_RETL').cast(FloatType()))
  currPromo = currPromo.withColumn('SMA_RETL_MULT_UNIT', col('SMA_RETL_MULT_UNIT').cast(FloatType()))
  currPromo = currPromo.withColumn('CHANGE_AMOUNT_PCT', col('CHANGE_AMOUNT_PCT').cast(FloatType()))
  currPromo = currPromo.withColumn('SALE_PRICE', col('SALE_PRICE').cast(FloatType()))
  
  
  currPromo = currPromo.withColumn("SALE_PRICE", when(col('SALE_PRICE').isNull(), lit(None)).otherwise(lpad(formatZerosUDF(round((col('SALE_PRICE')), 2)),10,'0')).cast(StringType()))
  
  currPromo = currPromo.withColumn("SALE_QUANTITY", saleQuantityCalculationUDF(col('PERF_DETL_SUB_TYPE'), col('NUM_TO_BUY_1')))
  
  currPromo = currPromo.withColumn("CHANGE_AMOUNT_PCT", col('CHANGE_AMOUNT_PCT').cast(FloatType()))
  currPromo = currPromo.drop(col('NUM_TO_BUY_1'))
  
  currPromo = currPromo.drop(col('CHANGE_AMOUNT_PCT'))
  
  currPromo = currPromo.withColumn("LAST_UPDATE_ID", lit(PipelineID))

  currPromo = currPromo.withColumn("LAST_UPDATE_TIMESTAMP", lit(currentTimeStamp).cast(TimestampType()))
  
  return currPromo

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Item Master Output Transformation

# COMMAND ----------

def itemMasterOutputTransformation(itemMasterOutputDf):
  
  itemMasterOutputDf = itemMasterOutputDf.withColumn("SMA_LINK_APPLY_DATE",date_format(col("SMA_LINK_APPLY_DATE"), 'yyyy/MM/dd').cast(StringType()))
  itemMasterOutputDf = itemMasterOutputDf.withColumn("SMA_LINK_END_DATE",date_format(col("SMA_LINK_END_DATE"), 'yyyy/MM/dd').cast(StringType()))
  itemMasterOutputDf = itemMasterOutputDf.withColumn("SMA_LINK_START_DATE",date_format(col("SMA_LINK_START_DATE"), 'yyyy/MM/dd').cast(StringType()))
  itemMasterOutputDf = itemMasterOutputDf.withColumn("SMA_ITM_EFF_DATE",date_format(col("SMA_ITM_EFF_DATE"), 'yyyy/MM/dd').cast(StringType()))
  itemMasterOutputDf = itemMasterOutputDf.withColumn('SMA_LINK_UPC', lpad(col('SMA_LINK_UPC'),14,'0'))
  itemMasterOutputDf = itemMasterOutputDf.withColumn('SMA_PL_UPC_REDEF', lpad(col('SMA_PL_UPC_REDEF'),14,'0'))
  itemMasterOutputDf = itemMasterOutputDf.withColumn('SMA_VEND_COUPON_FAM1', lpad(col('SMA_VEND_COUPON_FAM1'),3,'0').cast(StringType()))
  itemMasterOutputDf = itemMasterOutputDf.withColumn('SMA_VEND_COUPON_FAM2', lpad(col('SMA_VEND_COUPON_FAM2'),4,'0').cast(StringType()))
  itemMasterOutputDf = itemMasterOutputDf.withColumn("SMA_RETL_MULT_UNIT", lpad(col("SMA_RETL_MULT_UNIT").cast(IntegerType()),10,'0'))
  itemMasterOutputDf = itemMasterOutputDf.withColumn("SMA_MULT_UNIT_RETL", lpad(regexp_replace(format_number(col("SMA_MULT_UNIT_RETL"), 2), ",", ""),10,'0'))
  itemMasterOutputDf = itemMasterOutputDf.withColumn('SMA_LINK_HDR_COUPON', lpad(col('SMA_LINK_HDR_COUPON'),14,'0'))
  itemMasterOutputDf = itemMasterOutputDf.withColumn('SMA_GTIN_NUM', lpad(col('SMA_GTIN_NUM'),14,'0'))
  itemMasterOutputDf = itemMasterOutputDf.withColumn('CHAIN_ID', lit('AHOLD'))
  itemMasterOutputDf = itemMasterOutputDf.withColumn('DSS_DC_ITEM_NUMBER', lit(None).cast(StringType()))
  
  itemMasterOutputDf = itemMasterOutputDf.select('AVG_WGT',
                'BTL_DPST_AMT',
                'HOW_TO_SELL',
                'PERF_DETL_SUB_TYPE',
                'SALE_PRICE',
                'SALE_QUANTITY',
                'SMA_ACTIVATION_CODE',
                'SMA_BATCH_SERIAL_NBR',
                'SMA_BATCH_SUB_DEPT',
                'SMA_BOTTLE_DEPOSIT_IND',
                'SMA_CHG_TYPE',
                'SMA_COMM',
                'SMA_COMP_ID',
                'SMA_COMP_OBSERV_DATE',
                'SMA_COMP_PRICE',
                'SMA_CONTRIB_QTY',
                'SMA_CONTRIB_QTY_UOM',
                'SMA_CPN_MLTPY_IND',
                'SMA_DATA_TYPE',
                'SMA_DEMANDTEC_PROMO_ID',
                'SMA_DEST_STORE',
                'SMA_DISCOUNTABLE_IND',
                'SMA_EFF_DOW',
                'SMA_EMERGY_UPDATE_IND',
                'SMA_FEE_ITEM_TYPE',
                'SMA_FEE_TYPE',
                'SMA_FIXED_TARE_WGT',
                'SMA_FOOD_STAMP_IND',
                'SMA_FSA_IND',
                'SMA_FUEL_PROD_CATEG',
                'SMA_GAS_IND',
                'SMA_GLUTEN_FREE',
                'SMA_GTIN_NUM',
                'SMA_GUIDING_STARS',
                'SMA_HIP_IND',
                'SMA_HLTHY_IDEAS',
                'SMA_IFPS_CODE',
                'SMA_IN_STORE_TAG_PRINT',
                'SMA_ITEM_BRAND',
                'SMA_ITEM_DESC',
                'SMA_ITEM_MVMT',
                'SMA_ITEM_POG_COMM',
                'SMA_ITEM_SIZE' ,
                'SMA_ITEM_STATUS',
                'SMA_ITEM_TYPE',
                'SMA_ITEM_UOM',
                'SMA_KOSHER_FLAG',
                'SMA_LEGACY_ITEM',
                'SMA_LINK_APPLY_DATE',
                'SMA_LINK_APPLY_TIME',
                'SMA_LINK_CHG_TYPE',
                'SMA_LINK_END_DATE',
                'SMA_LINK_FAMCD_PROMOCD',
                'SMA_LINK_HDR_COUPON',
                'SMA_LINK_HDR_LOCATION',
                'SMA_LINK_HDR_MAINT_TYPE',
                'SMA_LINK_ITEM_NBR',
                'SMA_LINK_OOPS_ADWK',
                'SMA_LINK_OOPS_FILE_ID',
                'SMA_LINK_RCRD_TYPE',
                'SMA_LINK_START_DATE',
                'SMA_LINK_SYS_DIGIT',
                'SMA_LINK_TYPE' ,
                'SMA_LINK_UPC',
                'SMA_PL_UPC_REDEF',
                'SMA_MANUAL_PRICE_ENTRY',
                'SMA_MEAS_OF_EACH',
                'SMA_MEAS_OF_EACH_WIP',
                'SMA_MEAS_OF_PRICE',
                'SMA_MKT_AREA',
                'SMA_MULT_UNIT_RETL',
                'SMA_NAT_ORG_IND',
                'SMA_NEW_ITMUPC',
                'SMA_NON_PRICED_ITEM',
                'SMA_ORIG_CHG_TYPE',
                'SMA_ORIG_LIN',
                'SMA_ORIG_VENDOR',
                'SMA_POINTS_ELIGIBLE',
                'SMA_POS_REQUIRED',
                'SMA_POS_SYSTEM',
                'SMA_PRICE_CHG_ID',
                'SMA_PRIME_UPC',
                'SMA_PRIV_LABEL',
                'SMA_QTY_KEY_OPTIONS',
                'SMA_RECALL_FLAG',
                'SMA_RECV_TYPE',
                'SMA_REFUND_RECEIPT_EXCLUSION',
                'SMA_RESTR_CODE',
                'SMA_RETL_CHG_TYPE',
                'SMA_RETL_MULT_UNIT',
                'SMA_RETL_MULT_UOM',
                'SMA_RETL_PRC_ENTRY_CHG',
                'SMA_RETL_UOM',
                'SMA_RETL_VENDOR',
                'SMA_SBW_IND',
                'SMA_SEGMENT',
                'SMA_SELL_RETL',
                'SMA_SHELF_TAG_REQ',
                'SMA_SMR_EFF_DATE',
                'SMA_SOURCE_METHOD',
                'SMA_SOURCE_WHSE',
                'SMA_STATUS_DATE',
                'SMA_STORE',
                'SMA_STORE_DIV',
                'SMA_STORE_STATE',
                'SMA_STORE_ZONE',
                'SMA_SUB_COMM',
                'SMA_SUB_DEPT',
                'SMA_TAG_REQUIRED',
                'SMA_TAG_SZ',
                'SMA_TARE_PCT',
                'SMA_TARE_UOM',
                'SMA_TAX_1',
                'SMA_TAX_2',
                'SMA_TAX_3',
                'SMA_TAX_4',
                'SMA_TAX_5',
                'SMA_TAX_6',
                'SMA_TAX_7',
                'SMA_TAX_8',
                'SMA_TAX_CATEG',
                'SMA_TAX_CHG_IND',
                'SMA_TENDER_RESTRICTION_CODE',
                'SMA_TIBCO_DATE',
                'SMA_TRANSMIT_DATE',
                'SMA_UNIT_PRICE_CODE',
                'SMA_UOM_OF_PRICE',
                'SMA_UPC_DESC',
                'SMA_UPC_OVERRIDE_GRP_NUM',
                'SMA_VEND_COUPON_FAM1',
                'SMA_VEND_COUPON_FAM2',
                'SMA_VEND_NUM',
                'SMA_VENDOR_PACK',
                'SMA_WIC_ALT',
                'SMA_WIC_IND',
                'SMA_WIC_SHLF_MIN',
                'SMA_WPG',
                'SMA_ITM_EFF_DATE',
                'DSS_PRODUCT_DESC',
                'DSS_PRIMARY_ID',
                'DSS_PRIMARY_DESC',
                'DSS_SUB_CATEGORY_DESC',
                'DSS_CATEGORY_ID',
                'DSS_CATEGORY_DESC',
                'DSS_SUPER_CATEGORY_DESC',
                'DSS_ALL_CATEGORY_ID',
                'DSS_ALL_CATEGORY_DESC',
                'DSS_MDSE_PROGRAM_ID',
                'DSS_MDSE_PROGRAM_DESC',
                'DSS_PRICE_MASTER_ID',
                'DSS_PRICE_MASTER_DESC',
                'DSS_BUYER_ID',
                'DSS_BUYER_DESC',
                'DSS_PRICE_SENS_ID',
                'DSS_PRICE_SENS_SHORT_DESC',
                'DSS_PRICE_SENS_LONG_DESC',
                'DSS_MANCODE_ID',
                'DSS_MANCODE_DESC',
                'DSS_PRIVATE_BRAND_ID',
                'DSS_PRIVATE_BRAND_DESC',
                'DSS_PRODUCT_STATUS_ID',
                'DSS_PRODUCT_STATUS_DESC',
                'DSS_PRODUCT_UOM_DESC',
                'DSS_PRODUCT_PACK_QTY',
                'DSS_DIRECTOR_ID',
                'DSS_DIRECTOR_DESC',
                'DSS_DIRECTOR_GROUP_DESC',
                'DSS_MAJOR_CATEGORY_ID',
                'DSS_MAJOR_CATEGORY_DESC',
                'DSS_PLANOGRAM_ID',
                'DSS_PLANOGRAM_DESC',
                'DSS_HEIGHT',
                'DSS_WIDTH',
                'DSS_DEPTH',
                'DSS_BRAND_ID',
                'DSS_BRAND_DESC',
                'DSS_LIFO_POOL_ID',
                'DSS_GROUP_TYPE',
                'DSS_DATE_UPDATED',
                'DSS_EQUIVALENT_SIZE_ID',
                'DSS_EQUIVALENT_SIZE_DESC',
                'DSS_EQUIV_SIZE',
                'DSS_SCAN_TYPE_ID',
                'DSS_SCAN_TYPE_DESC',
                'DSS_STORE_HANDLING_CODE',
                'DSS_SHC_DESC',
                'DSS_GS_RATING_DESC',
                'DSS_GS_DATE_RATED',
                'DSS_PRIVATE_LABEL_ID',
                'DSS_PRIVATE_LABEL_DESC',
                'DSS_PRODUCT_DIMESION_L_BYTE',
                'DSS_PRDT_NAME',
                'DSS_MFR_NAME',
                'DSS_BRAND_NAME',
                'DSS_KEYWORD',
                'DSS_META_DESC',
                'DSS_META_KEYWORD',
                'DSS_SRVG_SZ_DSC',
                'DSS_SRVG_SZ_UOM_DSC',
                'DSS_SRVG_PER_CTNR',
                'DSS_NEW_PRDT_FLG',
                'DSS_OVRLN_DSC',
                'DSS_ALT_SRVG_SZ_DSC',
                'DSS_ALT_SRVG_SZ_UOM',
                'DSS_AVG_UNIT_WT',
                'DSS_PRIVATE_BRAND_CD',
                'DSS_MENU_LBL_FLG',
                'DSS_DC_ITEM_NUMBER',                                
                'PRD2STORE_BANNER_ID',
                'PRD2STORE_PROMO_CODE',
                'PRD2STORE_SPLN_AD_CD',
                'PRD2STORE_TAG_TYP_CD',
                'PRD2STORE_WINE_VALUE_FLAG',
                'PRD2STORE_BUY_QTY',
                'PRD2STORE_LMT_QTY',
                'PRD2STORE_SALE_STRT_DT',
                'PRD2STORE_SALE_END_DT',
                'PRD2STORE_AGE_FLAG',
                'PRD2STORE_AGE_CODE',
                'PRD2STORE_SWAP_SAVE_UPC',
                'SCRTX_DET_FREQ_SHOP_TYPE',
                'SCRTX_DET_FREQ_SHOP_VAL',
                'SCRTX_DET_QTY_RQRD_FG',
                'SCRTX_DET_MIX_MATCH_CD',
                'SCRTX_DET_CENTRAL_ITEM',
                'SCRTX_DET_SLS_RESTRICT_GRP',
                'RTX_TYPE',
                'SCRTX_DET_PLU_BTCH_NBR',
                'SCRTX_DET_OP_CODE',
                'SCRTX_DET_RCPT_DESCR',
                'SCRTX_DET_NON_MDSE_ID',
                'SCRTX_DET_NG_ENTRY_FG',
                'SCRTX_DET_STR_CPN_FG',
                'SCRTX_DET_VEN_CPN_FG',
                'SCRTX_DET_MAN_PRC_FG',
                'SCRTX_DET_INTRNL_ID',
                'SCRTX_DET_DEA_GRP',
                'SCRTX_DET_COMP_TYPE',
                'SCRTX_DET_COMP_PRC',
                'SCRTX_DET_COMP_QTY',
                'SCRTX_DET_BLK_GRP',
                'SCRTX_DET_RSTRCSALE_BRCD_FG',
                'SCRTX_DET_NON_RX_HEALTH_FG',
                'SCRTX_DET_RX_FG',
                'SCRTX_DET_WIC_CVV_FG',
                'PRICE_UNIT_PRICE',
                'PRICE_UOM_CD',
                'PRICE_MLT_QUANTITY',
                'PRICE_PROMO_RTL_PRC',
                'PRICE_PROMO_UNIT_PRICE',
                'PRICE_PROMOTIONAL_QUANTITY',
                'PRICE_START_DATE',
                'PRICE_END_DATE',
                'TPRX001_STORE_SID_NBR',
                'TPRX001_ITEM_NBR',
                'TPRX001_ITEM_SRC_CD',
                'TPRX001_CPN_SRC_CD',
                'TPRX001_RTL_PRC_EFF_DT',
                'TPRX001_ITEM_PROMO_FLG',
                'TPRX001_PROMO_TYP_CD',
                'TPRX001_AD_TYP_CD',
                'TPRX001_PROMO_DSC',
                'TPRX001_MIX_MTCH_FLG',
                'TPRX001_PRC_STRAT_CD',
                'TPRX001_LOYAL_CRD_FLG',
                'TPRX001_SCAN_AUTH_FLG',
                'TPRX001_MDSE_AUTH_FLG',
                'TPRX001_SBT_FLG',
                'TPRX001_SBT_VEND_ID',
                'TPRX001_SBT_VEND_NET_CST',
                'TPRX001_SCAN_DAUTH_DT',
                'TPRX001_SCAN_PRVWK_RTL_MLT',
                'TPRX001_SCAN_PRVWK_RTL_PRC',
                'TPRX001_SCANPRVDAY_RTL_MLT',
                'TPRX001_SCANPRVDAY_RTL_PRC',
                'TPRX001_TAG_PRV_WK_RTL_MLT',
                'TPRX001_TAG_PRV_WK_RTL_PRC',
                'ALTERNATE_UPC',
                'INVENTORY_CODE',
                'INVENTORY_QTY_ON_HAND',
                'INVENTORY_SUPER_CATEGORY',
                'INVENTORY_MAJOR_CATEGORY',
                'INVENTORY_INTMD_CATEGORY',
                'INSERT_ID',
                'INSERT_TIMESTAMP',
                'LAST_UPDATE_ID',
                'LAST_UPDATE_TIMESTAMP',
                'EFF_TS',
                'CHAIN_ID'                               )
  
  return itemMasterOutputDf

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Checking promo record not present in priceChange update

# COMMAND ----------

def checkMissingPromoRec(currPromo, priceChgWithPromo):
  try:
    loggerAtt.info("checking missing PromoRec")
    ABC(mergePromoPriceUpdateCheck = 1)

    if priceChgWithPromo != None and currPromo != None and priceChgWithPromo.count() > 0 and currPromo.count() > 0:
      temp_table_name1 = "currPromo"
      currPromo.createOrReplaceTempView(temp_table_name1)

      temp_table_name2 = "priceChgWithPromo"
      priceChgWithPromo.createOrReplaceTempView(temp_table_name2)

      priceChgWithPromo = spark.sql('''select priceChgWithPromo.* 
                                        from priceChgWithPromo 
                                        left anti join currPromo
                                        on priceChgWithPromo.SMA_PROMO_LINK_UPC = currPromo.SMA_PROMO_LINK_UPC 
                                          and priceChgWithPromo.SMA_STORE = currPromo.SMA_STORE''')

      if priceChgWithPromo != None:
        loggerAtt.info(f"No of price change items not in promo update: {priceChgWithPromo.count()}")
        if priceChgWithPromo.count() > 0:
          currPromoList = [priceChgWithPromo, currPromo]
          currPromo = reduce(DataFrame.unionAll, currPromoList)          
      spark.catalog.dropTempView(temp_table_name2)
      spark.catalog.dropTempView(temp_table_name1)
      return currPromo
    elif priceChgWithPromo != None and priceChgWithPromo.count() > 0:
      return priceChgWithPromo
    elif currPromo != None and currPromo.count() > 0:
      return currPromo
    else:
      return currPromo
  except Exception as ex:
    ABC(mergePromoPriceUpdateCheck = 0)
    loggerAtt.error(ex)
    ABC(currItemMainRec = '')
    err = ErrorReturn('Error', ex,'checkMissingPromoRec')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson)) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SQL Functions

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Item Main - Fetch all current day modified records

# COMMAND ----------

def currItemMain():
  currItemMain = None
  try:
    ABC(itemMainSetUp = 1)
    
#     currItemMain = spark.sql('''select * from delta.`{}` where DATE(LAST_UPDATE_TIMESTAMP) between '2021-09-16' and '{}' '''.format(ItemMainDeltaPath, currentDate))
    
    currItemMain = spark.sql('''select * from delta.`{}` where DATE(LAST_UPDATE_TIMESTAMP) between '{}' and '{}' '''.format(ItemMainDeltaPath, previousDate, currentDate))
    
    if processing_file == 'COD':
      currItemMain = currItemMain.filter((col('LAST_UPDATE_ID')== PipelineID))
    elif processing_file == 'Recall':
      currItemMain = currItemMain.filter((col('LAST_UPDATE_ID')== PipelineID))
    elif processing_file == 'FullItem':
      currItemMain = currItemMain.filter((col('LAST_UPDATE_ID')== PipelineID))
      
    currItemMainCount = currItemMain.count()
    loggerAtt.info(f"No of current day attribute chg rec: {currItemMainCount}")
    ABC(currItemMainRec = currItemMainCount)
  except Exception as ex:
    ABC(itemMainSetUp = 0)
    loggerAtt.error(ex)
    ABC(currItemMainRec = '')
    err = ErrorReturn('Error', ex,'itemMainFetch')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
    
  if currItemMain != None:
    if currItemMainCount > 0:
      try:
        ABC(itemMainTransCheck = 1)
        currItemMain = addingNUll(currItemMain)
  
        currItemMain = itemMainTrans(currItemMain)
  
        loggerAtt.info("No of records for Update/Insert: "+str(currItemMain.count())+"," +str(len(currItemMain.columns)))
      except Exception as ex:
        ABC(itemMainTransCheck = 0)
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'itemMainTrans')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
      
      try:
        ABC(btlTransCheck = 1)
        
        currItemMain = fetchBtlAmt(currItemMain)
  
        loggerAtt.info("Bottle Deposit amount fetch done")
      except Exception as ex:
        ABC(btlTransCheck = 0)
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'btl Transformation')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
      
      try:
        ABC(wltTransCheck = 1)
        
        currItemMain = fetchWicInd(currItemMain)
  
        loggerAtt.info("Store - WIC fetch done")
      except Exception as ex:
        ABC(wltTransCheck = 0)
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'store wlt Transformation')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
      
      try:
        ABC(DeltaTableCreateCheck = 1)
        
        currItemMain = currItemMain.withColumn("LAST_UPDATE_ID", lit(PipelineID))
        currItemMain = currItemMain.withColumn("INSERT_ID", lit(PipelineID))

        currItemMain = currItemMain.withColumn("LAST_UPDATE_TIMESTAMP", lit(currentTimeStamp).cast(TimestampType()))
  
        currItemMain = currItemMain.withColumn("INSERT_TIMESTAMP", lit(currentTimeStamp).cast(TimestampType()))
        
        temp_table_name = "itemsInItemTableProcessing"

        currItemMain.createOrReplaceTempView(temp_table_name)

        initial_recs = spark.sql("""SELECT * from delta.`{}`;""".format(itemMasterDeltaPath))
        initialCount = initial_recs.count()
        loggerAtt.info("No of records for in item table: "+str(initialCount)+"," +str(len(initial_recs.columns)))
        initial_recs = initialCount
        ABC(ItemMasterInitCount=initialCount)
        
        
        spark.sql('''MERGE INTO delta.`{}` as Item 
                      USING itemsInItemTableProcessing 
                      ON Item.SMA_GTIN_NUM = itemsInItemTableProcessing.SMA_GTIN_NUM and Item.SMA_STORE= itemsInItemTableProcessing.SMA_STORE
                      WHEN MATCHED Then 
                        Update Set  Item.SMA_DEST_STORE = itemsInItemTableProcessing.SMA_DEST_STORE,
                                    Item.BTL_DPST_AMT = itemsInItemTableProcessing.BTL_DPST_AMT,
                                    Item.SMA_SUB_DEPT = itemsInItemTableProcessing.SMA_SUB_DEPT,
                                    Item.SMA_COMM = itemsInItemTableProcessing.SMA_COMM,
                                    Item.SMA_SUB_COMM = itemsInItemTableProcessing.SMA_SUB_COMM,
                                    Item.SMA_DATA_TYPE = itemsInItemTableProcessing.SMA_DATA_TYPE,
                                    Item.SMA_DEMANDTEC_PROMO_ID = itemsInItemTableProcessing.SMA_DEMANDTEC_PROMO_ID,
                                    Item.SMA_BOTTLE_DEPOSIT_IND = itemsInItemTableProcessing.SMA_BOTTLE_DEPOSIT_IND,
                                    Item.SMA_POS_SYSTEM = itemsInItemTableProcessing.SMA_POS_SYSTEM,
                                    Item.SMA_SEGMENT = itemsInItemTableProcessing.SMA_SEGMENT,
                                    Item.SMA_ITEM_DESC = itemsInItemTableProcessing.SMA_ITEM_DESC,
                                    Item.SMA_ITEM_MVMT = itemsInItemTableProcessing.SMA_ITEM_MVMT,
                                    Item.SMA_ITEM_POG_COMM = itemsInItemTableProcessing.SMA_ITEM_POG_COMM,
                                    Item.SMA_UPC_DESC = itemsInItemTableProcessing.SMA_UPC_DESC,
                                    Item.SMA_SBW_IND = itemsInItemTableProcessing.SMA_SBW_IND,
                                    Item.SMA_PRIME_UPC = itemsInItemTableProcessing.SMA_PRIME_UPC,
                                    Item.SMA_ITEM_SIZE = itemsInItemTableProcessing.SMA_ITEM_SIZE,
                                    Item.SMA_ITEM_UOM = itemsInItemTableProcessing.SMA_ITEM_UOM,
                                    Item.SMA_ITEM_BRAND = itemsInItemTableProcessing.SMA_ITEM_BRAND,
                                    Item.SMA_ITEM_TYPE = itemsInItemTableProcessing.SMA_ITEM_TYPE,
                                    Item.SMA_VEND_COUPON_FAM1 = itemsInItemTableProcessing.SMA_VEND_COUPON_FAM1,
                                    Item.SMA_VEND_COUPON_FAM2 = itemsInItemTableProcessing.SMA_VEND_COUPON_FAM2,
                                    Item.SMA_FSA_IND = itemsInItemTableProcessing.SMA_FSA_IND,
                                    Item.SMA_NAT_ORG_IND = itemsInItemTableProcessing.SMA_NAT_ORG_IND,
                                    Item.SMA_NEW_ITMUPC = itemsInItemTableProcessing.SMA_NEW_ITMUPC,
                                    Item.SMA_PRIV_LABEL = itemsInItemTableProcessing.SMA_PRIV_LABEL,
                                    Item.SMA_TAG_SZ = itemsInItemTableProcessing.SMA_TAG_SZ,
                                    Item.SMA_FUEL_PROD_CATEG = itemsInItemTableProcessing.SMA_FUEL_PROD_CATEG,
                                    Item.SMA_CONTRIB_QTY = itemsInItemTableProcessing.SMA_CONTRIB_QTY,
                                    Item.SMA_CONTRIB_QTY_UOM = itemsInItemTableProcessing.SMA_CONTRIB_QTY_UOM,
                                    Item.SMA_GAS_IND = itemsInItemTableProcessing.SMA_GAS_IND,
                                    Item.SMA_ITEM_STATUS = itemsInItemTableProcessing.SMA_ITEM_STATUS,
                                    Item.SMA_STATUS_DATE = itemsInItemTableProcessing.SMA_STATUS_DATE,
                                    Item.SMA_VEND_NUM = itemsInItemTableProcessing.SMA_VEND_NUM,
                                    Item.SMA_SOURCE_METHOD = itemsInItemTableProcessing.SMA_SOURCE_METHOD,
                                    Item.SMA_SOURCE_WHSE = itemsInItemTableProcessing.SMA_SOURCE_WHSE,
                                    Item.SMA_RECV_TYPE = itemsInItemTableProcessing.SMA_RECV_TYPE,
                                    Item.SMA_MEAS_OF_EACH = itemsInItemTableProcessing.SMA_MEAS_OF_EACH,
                                    Item.SMA_MEAS_OF_EACH_WIP = itemsInItemTableProcessing.SMA_MEAS_OF_EACH_WIP,
                                    Item.SMA_MEAS_OF_PRICE = itemsInItemTableProcessing.SMA_MEAS_OF_PRICE,
                                    Item.SMA_MKT_AREA = itemsInItemTableProcessing.SMA_MKT_AREA,
                                    Item.SMA_UOM_OF_PRICE = itemsInItemTableProcessing.SMA_UOM_OF_PRICE,
                                    Item.SMA_VENDOR_PACK = itemsInItemTableProcessing.SMA_VENDOR_PACK,
                                    Item.SMA_SHELF_TAG_REQ = itemsInItemTableProcessing.SMA_SHELF_TAG_REQ,
                                    Item.SMA_QTY_KEY_OPTIONS = itemsInItemTableProcessing.SMA_QTY_KEY_OPTIONS,
                                    Item.SMA_MANUAL_PRICE_ENTRY = itemsInItemTableProcessing.SMA_MANUAL_PRICE_ENTRY,
                                    Item.SMA_FOOD_STAMP_IND = itemsInItemTableProcessing.SMA_FOOD_STAMP_IND,
                                    Item.SMA_HIP_IND = itemsInItemTableProcessing.SMA_HIP_IND,
                                    Item.SMA_WIC_IND = itemsInItemTableProcessing.SMA_WIC_IND,
                                    Item.SMA_TARE_PCT = itemsInItemTableProcessing.SMA_TARE_PCT,
                                    Item.SMA_FIXED_TARE_WGT = itemsInItemTableProcessing.SMA_FIXED_TARE_WGT,
                                    Item.SMA_TARE_UOM = itemsInItemTableProcessing.SMA_TARE_UOM,
                                    Item.SMA_POINTS_ELIGIBLE = itemsInItemTableProcessing.SMA_POINTS_ELIGIBLE,
                                    Item.SMA_RECALL_FLAG = itemsInItemTableProcessing.SMA_RECALL_FLAG,
                                    Item.SMA_TAX_CHG_IND = itemsInItemTableProcessing.SMA_TAX_CHG_IND,
                                    Item.SMA_RESTR_CODE = itemsInItemTableProcessing.SMA_RESTR_CODE,
                                    Item.SMA_WIC_SHLF_MIN = itemsInItemTableProcessing.SMA_WIC_SHLF_MIN,
                                    Item.SMA_WIC_ALT = itemsInItemTableProcessing.SMA_WIC_ALT,
                                    Item.SMA_TAX_1 = itemsInItemTableProcessing.SMA_TAX_1,
                                    Item.SMA_TAX_2 = itemsInItemTableProcessing.SMA_TAX_2,
                                    Item.SMA_TAX_3 = itemsInItemTableProcessing.SMA_TAX_3,
                                    Item.SMA_TAX_4 = itemsInItemTableProcessing.SMA_TAX_4,
                                    Item.SMA_TAX_5 = itemsInItemTableProcessing.SMA_TAX_5,
                                    Item.SMA_TAX_6 = itemsInItemTableProcessing.SMA_TAX_6,
                                    Item.SMA_TAX_7 = itemsInItemTableProcessing.SMA_TAX_7,
                                    Item.SMA_TAX_8 = itemsInItemTableProcessing.SMA_TAX_8,
                                    Item.SMA_DISCOUNTABLE_IND = itemsInItemTableProcessing.SMA_DISCOUNTABLE_IND,
                                    Item.SMA_CPN_MLTPY_IND = itemsInItemTableProcessing.SMA_CPN_MLTPY_IND,
                                    Item.SMA_LINK_UPC = itemsInItemTableProcessing.SMA_LINK_UPC,
                                    Item.SMA_ACTIVATION_CODE = itemsInItemTableProcessing.SMA_ACTIVATION_CODE,
                                    Item.SMA_GLUTEN_FREE = itemsInItemTableProcessing.SMA_GLUTEN_FREE,
                                    Item.SMA_NON_PRICED_ITEM = itemsInItemTableProcessing.SMA_NON_PRICED_ITEM,
                                    Item.SMA_ORIG_CHG_TYPE = itemsInItemTableProcessing.SMA_ORIG_CHG_TYPE,
                                    Item.SMA_ORIG_LIN = itemsInItemTableProcessing.SMA_ORIG_LIN,
                                    Item.SMA_ORIG_VENDOR = itemsInItemTableProcessing.SMA_ORIG_VENDOR,
                                    Item.SMA_IFPS_CODE = itemsInItemTableProcessing.SMA_IFPS_CODE,
                                    Item.SMA_IN_STORE_TAG_PRINT = itemsInItemTableProcessing.SMA_IN_STORE_TAG_PRINT,
                                    Item.SMA_BATCH_SERIAL_NBR = itemsInItemTableProcessing.SMA_BATCH_SERIAL_NBR,
                                    Item.SMA_BATCH_SUB_DEPT = itemsInItemTableProcessing.SMA_BATCH_SUB_DEPT,
                                    Item.SMA_EFF_DOW = itemsInItemTableProcessing.SMA_EFF_DOW,
                                    Item.SMA_SMR_EFF_DATE = itemsInItemTableProcessing.SMA_SMR_EFF_DATE,
                                    Item.SMA_STORE_DIV = itemsInItemTableProcessing.SMA_STORE_DIV,
                                    Item.SMA_STORE_STATE = itemsInItemTableProcessing.SMA_STORE_STATE,
                                    Item.SMA_STORE_ZONE = itemsInItemTableProcessing.SMA_STORE_ZONE,
                                    Item.SMA_TAX_CATEG = itemsInItemTableProcessing.SMA_TAX_CATEG,
                                    Item.SMA_TIBCO_DATE = itemsInItemTableProcessing.SMA_TIBCO_DATE,
                                    Item.SMA_TRANSMIT_DATE = itemsInItemTableProcessing.SMA_TRANSMIT_DATE,
                                    Item.SMA_UNIT_PRICE_CODE = itemsInItemTableProcessing.SMA_UNIT_PRICE_CODE,
                                    Item.SMA_WPG = itemsInItemTableProcessing.SMA_WPG,
                                    Item.SMA_EMERGY_UPDATE_IND = itemsInItemTableProcessing.SMA_EMERGY_UPDATE_IND,
                                    Item.SMA_CHG_TYPE = itemsInItemTableProcessing.SMA_CHG_TYPE,
                                    Item.SMA_RETL_VENDOR = itemsInItemTableProcessing.SMA_RETL_VENDOR,
                                    Item.SMA_POS_REQUIRED = itemsInItemTableProcessing.SMA_POS_REQUIRED,
                                    Item.SMA_TAG_REQUIRED = itemsInItemTableProcessing.SMA_TAG_REQUIRED,
                                    Item.SMA_UPC_OVERRIDE_GRP_NUM = itemsInItemTableProcessing.SMA_UPC_OVERRIDE_GRP_NUM,
                                    Item.SMA_TENDER_RESTRICTION_CODE = itemsInItemTableProcessing.SMA_TENDER_RESTRICTION_CODE,
                                    Item.SMA_REFUND_RECEIPT_EXCLUSION = itemsInItemTableProcessing.SMA_REFUND_RECEIPT_EXCLUSION,
                                    Item.SMA_FEE_ITEM_TYPE = itemsInItemTableProcessing.SMA_FEE_ITEM_TYPE,
                                    Item.SMA_FEE_TYPE = itemsInItemTableProcessing.SMA_FEE_TYPE,
                                    Item.SMA_KOSHER_FLAG = itemsInItemTableProcessing.SMA_KOSHER_FLAG,
                                    Item.SMA_LEGACY_ITEM = itemsInItemTableProcessing.SMA_LEGACY_ITEM,
                                    Item.SMA_GUIDING_STARS = itemsInItemTableProcessing.SMA_GUIDING_STARS,
                                    Item.SMA_COMP_ID = itemsInItemTableProcessing.SMA_COMP_ID,
                                    Item.SMA_COMP_OBSERV_DATE = itemsInItemTableProcessing.SMA_COMP_OBSERV_DATE,
                                    Item.SMA_COMP_PRICE = itemsInItemTableProcessing.SMA_COMP_PRICE,
                                    Item.SMA_RETL_CHG_TYPE = itemsInItemTableProcessing.SMA_RETL_CHG_TYPE,
                                    Item.HOW_TO_SELL = itemsInItemTableProcessing.HOW_TO_SELL,
                                    Item.AVG_WGT = itemsInItemTableProcessing.AVG_WGT,
                                    Item.EFF_TS = itemsInItemTableProcessing.EFF_TS,
                                    Item.LAST_UPDATE_ID = itemsInItemTableProcessing.LAST_UPDATE_ID,
                                    Item.LAST_UPDATE_TIMESTAMP = itemsInItemTableProcessing.LAST_UPDATE_TIMESTAMP
                        WHEN NOT MATCHED THEN INSERT * '''.format(itemMasterDeltaPath))
  
        appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(itemMasterDeltaPath))
        loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
        appended_recs = appended_recs.head(1)
        ABC(ItemMasterFinalCount=appended_recs[0][0])

        spark.catalog.dropTempView(temp_table_name)
        loggerAtt.info("Merge into Item Master Delta table successful")
      except Exception as ex:
        ABC(DeltaTableCreateCheck = 0)
        ABC(ItemMasterFinalCount='')
        ABC(ItemMasterInitCount='')
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'itemMainUpdate/Insert')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson)) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Bottle Deposit Amount Fetch

# COMMAND ----------

def fetchBtlAmt(currItemMain):
  
  btlData = spark.sql('''select SMA_STORE as STORE, SMA_GTIN_NUM as GTIN_NUM, SMA_MULT_UNIT_RETL as BTL_DPST_AMT from delta.`{}`'''.format(itemMasterDeltaPath))
  
  currItemMain = currItemMain.join(btlData, [btlData.GTIN_NUM == currItemMain.SMA_LINK_UPC, btlData.STORE == currItemMain.SMA_STORE], how='left').select([col(xx) for xx in currItemMain.columns] + ['BTL_DPST_AMT'])
    
  currItemMain = currItemMain.withColumn("BTL_DPST_AMT", when(col('BTL_DPST_AMT').isNull(), lit(None)).otherwise(lpad(formatZerosUDF(round((col('BTL_DPST_AMT')), 2)),10,'0')).cast(StringType()))
  
  return currItemMain



def btlAmtUpdate():
  btlData = None
  try:
    ABC(btlItemFetchCheck = 1)
    btlData = spark.sql('''select SMA_STORE as STORE, SMA_GTIN_NUM as GTIN_NUM, SMA_MULT_UNIT_RETL as BTL_DPST_AMT from delta.`{}` where LENGTH(SMA_GTIN_NUM)=5 and LAST_UPDATE_ID='{}' '''.format(itemMasterDeltaPath, PipelineID))
    
    if btlData != None:
      loggerAtt.info(f"No of 5 digit Gtin Num : {btlData.count()}")
      if btlData.count() > 0:

        itemMain = spark.sql('''select SMA_GTIN_NUM, SMA_LINK_UPC, SMA_STORE from delta.`{}`'''.format(itemMasterDeltaPath))

        btlData = btlData.join(itemMain, [btlData.GTIN_NUM == itemMain.SMA_LINK_UPC, btlData.STORE == itemMain.SMA_STORE], how='inner').select(['SMA_GTIN_NUM', 'SMA_STORE', 'BTL_DPST_AMT'])

        loggerAtt.info(f"No of item update for BTL AMT: {btlData.count()}")
  except Exception as ex:
    ABC(btlItemFetchCheck = 0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'btlItemFetchCheck')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
  
  if btlData != None:
    if btlData.count() > 0:
      try:
        ABC(btlAmtUpdate = 1)
        
        btlData = btlData.withColumn("LAST_UPDATE_ID", lit(PipelineID))
        btlData = btlData.withColumn("LAST_UPDATE_TIMESTAMP", lit(currentTimeStamp).cast(TimestampType()))
        
        temp_table_name = "btlProcessing"

        btlData.createOrReplaceTempView(temp_table_name)

        spark.sql(''' MERGE INTO delta.`{}` as Item 
                      USING btlProcessing 
                      ON Item.SMA_GTIN_NUM = btlProcessing.SMA_GTIN_NUM and Item.SMA_STORE= btlProcessing.SMA_STORE
                      WHEN MATCHED Then 
                        Update Set  Item.BTL_DPST_AMT = btlProcessing.BTL_DPST_AMT,
                        Item.LAST_UPDATE_ID = btlProcessing.LAST_UPDATE_ID,
                        Item.LAST_UPDATE_TIMESTAMP = btlProcessing.LAST_UPDATE_TIMESTAMP'''.format(itemMasterDeltaPath))

        spark.catalog.dropTempView(temp_table_name)
        loggerAtt.info("Merge into Item Master Delta table successful")
      except Exception as ex:
        ABC(btlAmtUpdate = 0)
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'btlAmtUpdate')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson)) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Store Join - WIC_IND and WIC_ALT

# COMMAND ----------

def fetchWicInd(currItemMain):
  
  storeData = spark.sql('''select DEST_STORE, BANNER, WIC_ELIGIBLE_IND from delta.`{}`'''.format(storeDeltaPath))
  
  currItemMain = currItemMain.join(storeData, [storeData.DEST_STORE == currItemMain.SMA_DEST_STORE, storeData.BANNER == currItemMain.PRD2STORE_BANNER_ID], how='left').select([col(xx) for xx in currItemMain.columns] + ['WIC_ELIGIBLE_IND'])
  
  currItemMain = currItemMain.withColumn("SMA_WIC_IND", when((col('WIC_ELIGIBLE_IND') == 'Y'), col('SMA_WIC_IND')).otherwise(col('WIC_ELIGIBLE_IND')).cast(StringType()))
  
  currItemMain = currItemMain.withColumn("SMA_WIC_ALT", when((col('WIC_ELIGIBLE_IND') == 'Y'), col('SMA_WIC_ALT')).otherwise(col('WIC_ELIGIBLE_IND')).cast(StringType()))
  
  currItemMain = currItemMain.drop(col('WIC_ELIGIBLE_IND'))
  
  return currItemMain


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Price Change - Fetch all current day effective records

# COMMAND ----------

def  currPriceChange():
  loggerAtt.info("Current day processing on Price Change table")
  currPriceChange = None
  try:
    ABC(priceChangeSetUp = 1)
    if processing_file == 'FullItem':
      currPriceChange = spark.sql('''select SMA_PRICE_CHG_ID, 
                                        SMA_GTIN_NUM, 
                                        SMA_STORE, 
                                        SMA_ITM_EFF_DATE, 
                                        SMA_SELL_RETL, 
                                        SMA_RETL_UOM, 
                                        SMA_RETL_MULT_UNIT, 
                                        SMA_MULT_UNIT_RETL, 
                                        SMA_RETL_MULT_UOM, 
                                        SMA_BATCH_SERIAL_NBR, 
                                        SMA_RETL_PRC_ENTRY_CHG, 
                                        INSERT_ID, 
                                        INSERT_TIMESTAMP 
                                      from (select *, ROW_NUMBER() OVER(
                                                      PARTITION BY SMA_STORE, SMA_GTIN_NUM ORDER BY INSERT_TIMESTAMP DESC) as row_number
                                      from delta.`{}`) where  row_number=1 and
                                                              SMA_ITM_EFF_DATE = '{}' '''.format(PriceChangeDeltaPath, previousDate))
    else:
      currPriceChange = spark.sql('''select SMA_PRICE_CHG_ID, 
                                        SMA_GTIN_NUM, 
                                        SMA_STORE, 
                                        SMA_ITM_EFF_DATE, 
                                        SMA_SELL_RETL, 
                                        SMA_RETL_UOM, 
                                        SMA_RETL_MULT_UNIT, 
                                        SMA_MULT_UNIT_RETL, 
                                        SMA_RETL_MULT_UOM, 
                                        SMA_BATCH_SERIAL_NBR, 
                                        SMA_RETL_PRC_ENTRY_CHG, 
                                        INSERT_ID, 
                                        INSERT_TIMESTAMP 
                                      from (select *, ROW_NUMBER() OVER(
                                                      PARTITION BY SMA_STORE, SMA_GTIN_NUM ORDER BY INSERT_TIMESTAMP DESC) as row_number
                                      from delta.`{}`) where  row_number=1 and
                                                              SMA_ITM_EFF_DATE = '{}' '''.format(PriceChangeDeltaPath, currentDate))
    
#     currPriceChange = spark.sql('''select first_value(SMA_PRICE_CHG_ID) as SMA_PRICE_CHG_ID,
#                                           SMA_GTIN_NUM,
#                                           SMA_STORE, 
#                                           first_value(SMA_ITM_EFF_DATE) as SMA_ITM_EFF_DATE,
#                                           first_value(SMA_SELL_RETL) as SMA_SELL_RETL,
#                                           first_value(SMA_RETL_UOM) as SMA_RETL_UOM,
#                                           first_value(SMA_RETL_MULT_UNIT) as SMA_RETL_MULT_UNIT,
#                                           first_value(SMA_MULT_UNIT_RETL) as SMA_MULT_UNIT_RETL,
#                                           first_value(SMA_RETL_MULT_UOM) as SMA_RETL_MULT_UOM,
#                                           first_value(SMA_BATCH_SERIAL_NBR) as SMA_BATCH_SERIAL_NBR,
#                                           first_value(SMA_RETL_PRC_ENTRY_CHG) as SMA_RETL_PRC_ENTRY_CHG,
#                                           first_value(INSERT_ID) as INSERT_ID,
#                                           first_value(INSERT_TIMESTAMP) as INSERT_TIMESTAMP
#                                     from delta.`{}` 
#                                     where SMA_ITM_EFF_DATE = '{}'
#                                     group by SMA_STORE, SMA_GTIN_NUM
#                                     order by INSERT_TIMESTAMP desc'''.format(PriceChangeDeltaPath, previousDate))
    
    if processing_file == 'COD':
      currPriceChange = currPriceChange.filter((col('INSERT_ID')== PipelineID))
    elif processing_file == 'FullItem':
      currPriceChange = currPriceChange.filter((col('INSERT_ID')== PipelineID))
    currPriceChangeCount = currPriceChange.count()
    loggerAtt.info(f"No of current day attribute chg rec: {currPriceChangeCount}")
    ABC(currPriceChangeRec = currPriceChangeCount)
  except Exception as ex:
    ABC(priceChangeSetUp = 0)
    loggerAtt.error(ex)
    ABC(currPriceChangeRec = '')
    err = ErrorReturn('Error', ex,'price change Fetch')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
  
  if currPriceChange != None:
    if currPriceChange.count() > 0:
      try:
        ABC(currPriceChangeTransCheck = 1)
        
        currPriceChange = priceChangeTrans(currPriceChange)
  
        loggerAtt.info("No of records for Update: "+str(currPriceChange.count())+"," +str(len(currPriceChange.columns)))
      except Exception as ex:
        ABC(currPriceChangeTransCheck = 0)
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'price change trans')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
      
      try:
        ABC(DeltaTableCreateCheck = 1)
        
        currPriceChange = currPriceChange.withColumn("LAST_UPDATE_ID", lit(PipelineID))
        
        currPriceChange = currPriceChange.withColumn("LAST_UPDATE_TIMESTAMP", lit(currentTimeStamp).cast(TimestampType()))
        
        temp_table_name = "priceChangeTemp"

        currPriceChange.createOrReplaceTempView(temp_table_name)

        initial_recs = spark.sql("""SELECT * from delta.`{}`;""".format(itemMasterDeltaPath))
        initialCount = initial_recs.count()
        loggerAtt.info("No of records in item table: "+str(initialCount)+"," +str(len(initial_recs.columns)))
        initial_recs = initialCount
        ABC(priceChangeInitCount=initialCount)

        spark.sql('''MERGE INTO delta.`{}` as Item 
            USING priceChangeTemp 
            ON Item.SMA_GTIN_NUM = priceChangeTemp.SMA_GTIN_NUM and Item.SMA_STORE= priceChangeTemp.SMA_STORE
            WHEN MATCHED Then 
                    Update Set  Item.SMA_PRICE_CHG_ID = priceChangeTemp.SMA_PRICE_CHG_ID,
                                Item.SMA_ITM_EFF_DATE = priceChangeTemp.SMA_ITM_EFF_DATE,
                                Item.SMA_SELL_RETL = priceChangeTemp.SMA_SELL_RETL,
                                Item.SMA_RETL_UOM = priceChangeTemp.SMA_RETL_UOM,
                                Item.SMA_RETL_MULT_UNIT = priceChangeTemp.SMA_RETL_MULT_UNIT,
                                Item.SMA_MULT_UNIT_RETL = priceChangeTemp.SMA_MULT_UNIT_RETL,
                                Item.SMA_RETL_MULT_UOM = priceChangeTemp.SMA_RETL_MULT_UOM,
                                Item.SMA_BATCH_SERIAL_NBR = priceChangeTemp.SMA_BATCH_SERIAL_NBR,
                                Item.SMA_RETL_PRC_ENTRY_CHG = priceChangeTemp.SMA_RETL_PRC_ENTRY_CHG,
                                Item.LAST_UPDATE_ID = priceChangeTemp.LAST_UPDATE_ID,
                                Item.LAST_UPDATE_TIMESTAMP = priceChangeTemp.LAST_UPDATE_TIMESTAMP'''.format(itemMasterDeltaPath))

        appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(itemMasterDeltaPath))
        loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
        appended_recs = appended_recs.head(1)
        ABC(priceChangeFinalCount=appended_recs[0][0])

        spark.catalog.dropTempView(temp_table_name)
        loggerAtt.info("Merge into Price Change Delta table successful")
      except Exception as ex:
        ABC(DeltaTableCreateCheck = 0)
        ABC(priceChangeInitCount='')
        ABC(priceChangeFinalCount='')
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'price Change update')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson)) 

  return currPriceChange

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Price Change Promo fetch

# COMMAND ----------

def priceChangeNotInPromo(priceChgRec):
  try:
    ABC(priceChgPromoFetchCheck = 1)
    if priceChgRec != None:
      if priceChgRec.count() > 0:
        temp_table_name = 'priceChgRec'
        priceChgRec.createOrReplaceTempView(temp_table_name)
        
        loggerAtt.info(f"No of price Change update: {priceChgRec.count()}")
        
        priceChgRec = spark.sql('''select SMA_LINK_HDR_COUPON, 
                                          SMA_LINK_HDR_LOCATION, 
                                          priceChgRec.SMA_STORE, 
                                          SMA_CHG_TYPE, 
                                          SMA_LINK_START_DATE, 
                                          SMA_LINK_RCRD_TYPE, 
                                          SMA_LINK_END_DATE, 
                                          priceChgRec.SMA_ITM_EFF_DATE, 
                                          SMA_LINK_CHG_TYPE, 
                                          SMA_PL_UPC_REDEF as SMA_PROMO_LINK_UPC, 
                                          SMA_LINK_HDR_MAINT_TYPE, 
                                          SMA_LINK_ITEM_NBR, 
                                          SMA_LINK_OOPS_ADWK, 
                                          SMA_LINK_OOPS_FILE_ID, 
                                          SMA_LINK_TYPE,
                                          SMA_LINK_SYS_DIGIT,
                                          SMA_LINK_FAMCD_PROMOCD,
                                          priceChgRec.SMA_BATCH_SERIAL_NBR,
                                          SMA_LINK_APPLY_DATE,
                                          SMA_LINK_APPLY_TIME,
                                          itemMasterTemp.INSERT_ID,
                                          itemMasterTemp.INSERT_TIMESTAMP,
                                          priceChgRec.LAST_UPDATE_ID,
                                          priceChgRec.LAST_UPDATE_TIMESTAMP,
                                          PERF_DETL_SUB_TYPE,
                                          SALE_PRICE,
                                          SALE_QUANTITY,
                                          priceChgRec.SMA_MULT_UNIT_RETL as SMA_MULT_UNIT_RETL,
                                          priceChgRec.SMA_RETL_MULT_UNIT as SMA_RETL_MULT_UNIT
                                    from delta.`{}` as itemMasterTemp
                                    join priceChgRec
                                    ON  priceChgRec.SMA_GTIN_NUM = itemMasterTemp.SMA_PL_UPC_REDEF and 
                                        priceChgRec.SMA_STORE = itemMasterTemp.SMA_STORE
                                    where SMA_LINK_HDR_COUPON > 0'''.format(itemMasterDeltaPath))
        
        loggerAtt.info(f"No of price Change update that has promo: {priceChgRec.count()}")
        spark.catalog.dropTempView(temp_table_name)
    return priceChgRec
  except Exception as ex:
        ABC(priceChgPromoFetchCheck = 0)
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'price change that has promo records')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson)) 



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Promo Linking Dialy - Fetch all current day effective records

# COMMAND ----------

def currPromoRecDaily():
  
  loggerAtt.info("Current day processing of Promo records")
  
  currPromoMain = None
  currPromoSce1 = None
  currPromoSce1Count = 0
  currPromoSce2 = None
  currPromoSce2Count = 0
  currentPromoCount = 0
  
  try:
    ABC(promoMainSetUp = 1)
    
#     currPromoMain = spark.sql('''select SMA_PROMO_LINK_UPC, 
#                                         SMA_STORE, 
#                                         first_value(SMA_ITM_EFF_DATE) as SMA_ITM_EFF_DATE,
#                                         first_value(SMA_LINK_HDR_COUPON) as SMA_LINK_HDR_COUPON,
#                                         first_value(SMA_LINK_HDR_LOCATION) as SMA_LINK_HDR_LOCATION,
#                                         first_value(SMA_LINK_APPLY_DATE) as SMA_LINK_APPLY_DATE,
#                                         first_value(SMA_LINK_APPLY_TIME) as SMA_LINK_APPLY_TIME,
#                                         first_value(SMA_LINK_END_DATE) as SMA_LINK_END_DATE,
#                                         first_value(SMA_LINK_CHG_TYPE) as SMA_LINK_CHG_TYPE,
#                                         first_value(SMA_CHG_TYPE) as SMA_CHG_TYPE,
#                                         first_value(SMA_BATCH_SERIAL_NBR) as SMA_BATCH_SERIAL_NBR,
#                                         first_value(INSERT_ID) as INSERT_ID,
#                                         first_value(INSERT_TIMESTAMP) as INSERT_TIMESTAMP
#                                       from delta.`{}` where SMA_ITM_EFF_DATE = '{}' and SMA_LINK_END_DATE >= current_date()
#                                       group by SMA_PROMO_LINK_UPC, SMA_STORE
#                                       order by INSERT_TIMESTAMP desc'''.format(PromotionMainDeltaPath, previousDate))
    if processing_file == 'Delta':
      currPromoMain = spark.sql('''select SMA_PROMO_LINK_UPC, 
                                      SMA_STORE, 
                                      SMA_ITM_EFF_DATE,
                                      SMA_LINK_HDR_COUPON,
                                      SMA_LINK_HDR_LOCATION,
                                      SMA_LINK_APPLY_DATE,
                                      SMA_LINK_APPLY_TIME,
                                      SMA_LINK_END_DATE,
                                      SMA_LINK_CHG_TYPE,
                                      SMA_CHG_TYPE,
                                      SMA_BATCH_SERIAL_NBR,
                                      INSERT_ID,
                                      INSERT_TIMESTAMP
                               from (select *, 
                                            ROW_NUMBER() OVER(PARTITION BY SMA_PROMO_LINK_UPC, SMA_STORE ORDER BY INSERT_TIMESTAMP DESC) as row_number 
                                     from delta.`{}`) 
                               where row_number=1 and SMA_ITM_EFF_DATE = '{}' and SMA_LINK_END_DATE >= '{}' '''.format(PromotionMainDeltaPath, currentDate, currentDate))
    
    else:
      currPromoMain = spark.sql('''select SMA_PROMO_LINK_UPC, 
                                      SMA_STORE, 
                                      SMA_ITM_EFF_DATE,
                                      SMA_LINK_HDR_COUPON,
                                      SMA_LINK_HDR_LOCATION,
                                      SMA_LINK_APPLY_DATE,
                                      SMA_LINK_APPLY_TIME,
                                      SMA_LINK_END_DATE,
                                      SMA_LINK_CHG_TYPE,
                                      SMA_CHG_TYPE,
                                      SMA_BATCH_SERIAL_NBR,
                                      INSERT_ID,
                                      INSERT_TIMESTAMP
                               from (select *, 
                                            ROW_NUMBER() OVER(PARTITION BY SMA_PROMO_LINK_UPC, SMA_STORE ORDER BY INSERT_TIMESTAMP DESC) as row_number 
                                     from delta.`{}`) 
                               where row_number=1 and SMA_ITM_EFF_DATE = '{}' and SMA_LINK_END_DATE >= '{}' '''.format(PromotionMainDeltaPath, previousDate, currentDate))
    
    if processing_file == 'FullItem':
      currPromoMain = currPromoMain.filter((col('INSERT_ID')== PipelineID))
    currPromoMainCount = currPromoMain.count()
    loggerAtt.info(f"No of current day attribute chg rec: {currPromoMainCount}")
    ABC(currPromoMainRec = currPromoMainCount)
  except Exception as ex:
    ABC(promoMainSetUp = 0)
    loggerAtt.error(ex)
    ABC(currPromoMainRec = '')
    err = ErrorReturn('Error', ex,'promo Main Fetch')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
  
  
  if currPromoMain != None:
    if currPromoMain.count() > 0:
      try:
        temp_table_name = "promoMainTemp"
        
        currPromoMain.createOrReplaceTempView(temp_table_name)
        
        if processing_file == 'Delta':
          currPromoMain = spark.sql('''Select * from (SELECT promoLink.SMA_LINK_HDR_COUPON as SMA_LINK_HDR_COUPON,
                                      promoLink.SMA_LINK_HDR_LOCATION as SMA_LINK_HDR_LOCATION,
                                      promoMainTemp.SMA_STORE as SMA_STORE,
                                      promoLink.SMA_CHG_TYPE as SMA_CHG_TYPE,
                                      promoLink.SMA_LINK_START_DATE as SMA_LINK_START_DATE,
                                      promoLink.SMA_LINK_RCRD_TYPE as SMA_LINK_RCRD_TYPE,
                                      promoLink.SMA_LINK_END_DATE as SMA_LINK_END_DATE,
                                      promoLink.SMA_ITM_EFF_DATE as SMA_ITM_EFF_DATE,
                                      promoLink.SMA_LINK_CHG_TYPE as SMA_LINK_CHG_TYPE,
                                      promoMainTemp.SMA_PROMO_LINK_UPC as SMA_PROMO_LINK_UPC, 
                                      promoLink.SMA_LINK_HDR_MAINT_TYPE as SMA_LINK_HDR_MAINT_TYPE,
                                      promoLink.SMA_LINK_ITEM_NBR as SMA_LINK_ITEM_NBR,
                                      promoLink.SMA_LINK_OOPS_ADWK as SMA_LINK_OOPS_ADWK,
                                      promoLink.SMA_LINK_OOPS_FILE_ID as SMA_LINK_OOPS_FILE_ID,
                                      promoLink.SMA_LINK_TYPE as SMA_LINK_TYPE,
                                      promoLink.SMA_LINK_SYS_DIGIT as SMA_LINK_SYS_DIGIT,
                                      promoLink.SMA_LINK_FAMCD_PROMOCD as SMA_LINK_FAMCD_PROMOCD,
                                      promoLink.SMA_BATCH_SERIAL_NBR as SMA_BATCH_SERIAL_NBR,
                                      promoMainTemp.SMA_LINK_APPLY_DATE as SMA_LINK_APPLY_DATE,
                                      promoMainTemp.SMA_LINK_APPLY_TIME as SMA_LINK_APPLY_TIME,
                                      promoLink.INSERT_ID as INSERT_ID,
                                      promoLink.INSERT_TIMESTAMP as INSERT_TIMESTAMP,
                                      promoLink.LAST_UPDATE_ID as LAST_UPDATE_ID,
                                      promoLink.LAST_UPDATE_TIMESTAMP as LAST_UPDATE_TIMESTAMP,
                                      ROW_NUMBER() OVER(PARTITION BY promoMainTemp.SMA_STORE, promoMainTemp.SMA_PROMO_LINK_UPC 
                                                        ORDER BY promoLink.SMA_LINK_START_DATE DESC, promoLink.INSERT_TIMESTAMP DESC) as row_number
                               FROM promoMainTemp 
                               LEFT JOIN (SELECT * FROM delta.`{}` 
                                         WHERE '{}' BETWEEN SMA_LINK_START_DATE AND SMA_LINK_END_DATE 
                                               and SMA_LINK_END_DATE >= '{}') as promoLink
                               ON promoLink.SMA_PROMO_LINK_UPC = promoMainTemp.SMA_PROMO_LINK_UPC 
                                  AND promoLink.SMA_STORE = promoMainTemp.SMA_STORE) 
                               Where row_number=1'''.format(PromotionLinkDeltaPath, currentDate, currentDate))
        else:
          currPromoMain = spark.sql('''Select * from (SELECT promoLink.SMA_LINK_HDR_COUPON as SMA_LINK_HDR_COUPON,
                                      promoLink.SMA_LINK_HDR_LOCATION as SMA_LINK_HDR_LOCATION,
                                      promoMainTemp.SMA_STORE as SMA_STORE,
                                      promoLink.SMA_CHG_TYPE as SMA_CHG_TYPE,
                                      promoLink.SMA_LINK_START_DATE as SMA_LINK_START_DATE,
                                      promoLink.SMA_LINK_RCRD_TYPE as SMA_LINK_RCRD_TYPE,
                                      promoLink.SMA_LINK_END_DATE as SMA_LINK_END_DATE,
                                      promoLink.SMA_ITM_EFF_DATE as SMA_ITM_EFF_DATE,
                                      promoLink.SMA_LINK_CHG_TYPE as SMA_LINK_CHG_TYPE,
                                      promoMainTemp.SMA_PROMO_LINK_UPC as SMA_PROMO_LINK_UPC, 
                                      promoLink.SMA_LINK_HDR_MAINT_TYPE as SMA_LINK_HDR_MAINT_TYPE,
                                      promoLink.SMA_LINK_ITEM_NBR as SMA_LINK_ITEM_NBR,
                                      promoLink.SMA_LINK_OOPS_ADWK as SMA_LINK_OOPS_ADWK,
                                      promoLink.SMA_LINK_OOPS_FILE_ID as SMA_LINK_OOPS_FILE_ID,
                                      promoLink.SMA_LINK_TYPE as SMA_LINK_TYPE,
                                      promoLink.SMA_LINK_SYS_DIGIT as SMA_LINK_SYS_DIGIT,
                                      promoLink.SMA_LINK_FAMCD_PROMOCD as SMA_LINK_FAMCD_PROMOCD,
                                      promoLink.SMA_BATCH_SERIAL_NBR as SMA_BATCH_SERIAL_NBR,
                                      promoMainTemp.SMA_LINK_APPLY_DATE as SMA_LINK_APPLY_DATE,
                                      promoMainTemp.SMA_LINK_APPLY_TIME as SMA_LINK_APPLY_TIME,
                                      promoLink.INSERT_ID as INSERT_ID,
                                      promoLink.INSERT_TIMESTAMP as INSERT_TIMESTAMP,
                                      promoLink.LAST_UPDATE_ID as LAST_UPDATE_ID,
                                      promoLink.LAST_UPDATE_TIMESTAMP as LAST_UPDATE_TIMESTAMP,
                                      ROW_NUMBER() OVER(PARTITION BY promoMainTemp.SMA_STORE, promoMainTemp.SMA_PROMO_LINK_UPC 
                                                        ORDER BY promoLink.SMA_LINK_START_DATE DESC, promoLink.INSERT_TIMESTAMP DESC) as row_number
                               FROM promoMainTemp 
                               LEFT JOIN (SELECT * FROM delta.`{}` 
                                         WHERE '{}' BETWEEN SMA_LINK_START_DATE AND SMA_LINK_END_DATE 
                                               and SMA_LINK_END_DATE >= '{}') as promoLink
                               ON promoLink.SMA_PROMO_LINK_UPC = promoMainTemp.SMA_PROMO_LINK_UPC 
                                  AND promoLink.SMA_STORE = promoMainTemp.SMA_STORE) 
                               Where row_number=1'''.format(PromotionLinkDeltaPath, previousDate, currentDate))
        currPromoMain = currPromoMain.drop(col('row_number'))
        
#         currPromoMain = spark.sql('''SELECT first_value(promoLink.SMA_LINK_HDR_COUPON) as SMA_LINK_HDR_COUPON,
#                                         first_value(promoLink.SMA_LINK_HDR_LOCATION) as SMA_LINK_HDR_LOCATION,
#                                         promoMainTemp.SMA_STORE as SMA_STORE,
#                                         first_value(promoLink.SMA_CHG_TYPE) as SMA_CHG_TYPE,
#                                         first_value(promoLink.SMA_LINK_START_DATE) as SMA_LINK_START_DATE,
#                                         first_value(promoLink.SMA_LINK_RCRD_TYPE) as SMA_LINK_RCRD_TYPE,
#                                         first_value(promoLink.SMA_LINK_END_DATE) as SMA_LINK_END_DATE,
#                                         first_value(promoLink.SMA_ITM_EFF_DATE) as SMA_ITM_EFF_DATE,
#                                         first_value(promoLink.SMA_LINK_CHG_TYPE) as SMA_LINK_CHG_TYPE,
#                                         promoMainTemp.SMA_PROMO_LINK_UPC as SMA_PROMO_LINK_UPC, 
#                                         first_value(promoLink.SMA_LINK_HDR_MAINT_TYPE) as SMA_LINK_HDR_MAINT_TYPE,
#                                         first_value(promoLink.SMA_LINK_ITEM_NBR) as SMA_LINK_ITEM_NBR,
#                                         first_value(promoLink.SMA_LINK_OOPS_ADWK) as SMA_LINK_OOPS_ADWK,
#                                         first_value(promoLink.SMA_LINK_OOPS_FILE_ID) as SMA_LINK_OOPS_FILE_ID,
#                                         first_value(promoLink.SMA_LINK_TYPE) as SMA_LINK_TYPE,
#                                         first_value(promoLink.SMA_LINK_SYS_DIGIT) as SMA_LINK_SYS_DIGIT,
#                                         first_value(promoLink.SMA_LINK_FAMCD_PROMOCD) as SMA_LINK_FAMCD_PROMOCD,
#                                         first_value(promoLink.SMA_BATCH_SERIAL_NBR) as SMA_BATCH_SERIAL_NBR,
#                                         first_value(promoMainTemp.SMA_LINK_APPLY_DATE) as SMA_LINK_APPLY_DATE,
#                                         first_value(promoMainTemp.SMA_LINK_APPLY_TIME) as SMA_LINK_APPLY_TIME,
#                                         first_value(promoLink.INSERT_ID) as INSERT_ID,
#                                         first_value(promoLink.INSERT_TIMESTAMP) as INSERT_TIMESTAMP,
#                                         first_value(promoLink.LAST_UPDATE_ID) as LAST_UPDATE_ID,
#                                         first_value(promoLink.LAST_UPDATE_TIMESTAMP) as LAST_UPDATE_TIMESTAMP
#                                       FROM promoMainTemp 
#                                         LEFT JOIN (SELECT * FROM delta.`{}` WHERE '{}' BETWEEN SMA_LINK_START_DATE 
#                                                                                                   AND SMA_LINK_END_DATE and SMA_LINK_END_DATE >= current_date()) as promoLink
#                                       ON promoLink.SMA_PROMO_LINK_UPC = promoMainTemp.SMA_PROMO_LINK_UPC 
#                                         AND promoLink.SMA_STORE = promoMainTemp.SMA_STORE 
#                                       GROUP BY promoMainTemp.SMA_PROMO_LINK_UPC, promoMainTemp.SMA_STORE
#                                       ORDER BY SMA_LINK_START_DATE DESC, INSERT_TIMESTAMP DESC'''.format(PromotionLinkDeltaPath, previousDate))
        if processing_file == 'FullItem':
          currPromoMain = currPromoMain.filter((col('LAST_UPDATE_ID')== PipelineID))
        currPromoMainCount = currPromoMain.count()
        loggerAtt.info(f"No of current day records after Promo Link Join: {currPromoMainCount}")
        ABC(currPromoRec = currPromoMainCount)
        spark.catalog.dropTempView(temp_table_name)       
      except Exception as ex:
        ABC(promoMainSetUp = 0)
        loggerAtt.error(ex)
        ABC(currPromoRec = '')
        err = ErrorReturn('Error', ex,'promo Main Join Promo Link')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
      
      try:
        currPromoSce1 = currPromoMain.filter((col('SMA_LINK_HDR_COUPON').isNull()))
        
        loggerAtt.info(f"No of current day promo main records not present in promo linking: {currPromoSce1.count()}")
        
      except Exception as ex:
        ABC(promoMainSetUp = 0)
        loggerAtt.error(ex)
        ABC(currPromoRec = '')
        err = ErrorReturn('Error', ex,'promo sce 1')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
      try:
        if currPromoSce1 != None:
          if currPromoSce1.count() != None:
            currPromoSce1 = promoLinkTransSce1(currPromoSce1)
            
            currPromoSce1Count = currPromoSce1.count()
      except Exception as ex:
        ABC(promoMainSetUp = 0)
        loggerAtt.error(ex)
        ABC(currPromoRec = '')
        err = ErrorReturn('Error', ex,'promo sc1 trans')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
      try:
        currPromoSce2 = currPromoMain.filter((col('SMA_LINK_HDR_COUPON').isNotNull()))
        
        loggerAtt.info(f"No of current day promo main records present in promo linking: {currPromoSce2.count()}")
        
      except Exception as ex:
        ABC(promoMainSetUp = 0)
        loggerAtt.error(ex)
        ABC(currPromoRec = '')
        err = ErrorReturn('Error', ex,'promo sc2')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))  
      
      try:
        if currPromoSce2 != None:
          if currPromoSce2.count() != None:
            currPromoSce2 = promoLinkTransSce2(currPromoSce2)
            currPromoSce2Count = currPromoSce2.count()
      except Exception as ex:
        ABC(promoMainSetUp = 0)
        loggerAtt.error(ex)
        ABC(currPromoRec = '')
        err = ErrorReturn('Error', ex,'promo sc2 trans')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
      try:
        if ((currPromoSce1Count > 0) and (currPromoSce2Count > 0)):
          currPromoMainList = [currPromoSce2, currPromoSce1]
          currPromoMain = reduce(DataFrame.unionAll, currPromoMainList)
        elif (currPromoSce2Count > 0):
          currPromoMain = currPromoSce2
        elif (currPromoSce1Count > 0):
          currPromoMain = currPromoSce1
      except Exception as ex:
        ABC(promoMainSetUp = 0)
        loggerAtt.error(ex)
        ABC(currPromoRec = '')
        err = ErrorReturn('Error', ex,'promo join sc1 and sc2')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
      try:
        if currPromoMain != None:
          if currPromoMain.count() != None:
            
            currItemMaster = spark.sql('''select SMA_STORE as STORE, SMA_GTIN_NUM as GTIN_NUM, SMA_MULT_UNIT_RETL, SMA_RETL_MULT_UNIT from delta.`{}`'''.format(itemMasterDeltaPath))
            
            currPromoMain = currPromoMain.join(currItemMaster, [currItemMaster.GTIN_NUM == currPromoMain.SMA_PROMO_LINK_UPC, currItemMaster.STORE == currPromoMain.SMA_STORE], how='inner').select([col(xx) for xx in currPromoMain.columns] + ['SMA_MULT_UNIT_RETL', 'SMA_RETL_MULT_UNIT'])
            
            loggerAtt.info(f"No of promo records after amount fetch from item master: {currPromoMain.count()}")
      except Exception as ex:
        ABC(promoMainSetUp = 0)
        loggerAtt.error(ex)
        ABC(currPromoRec = '')
        err = ErrorReturn('Error', ex,'Amount fetch from Item Master')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
  return currPromoMain

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Promo Linking Intraday - Fetch all current day effective records

# COMMAND ----------

def currPromoRecIOD():
  
  loggerAtt.info("Current day processing of Promo records")
  
  currPromoMain = None
  currPromoSce1 = None
  currPromoSce1Count = 0
  currPromoSce2 = None
  currPromoSce2Count = 0
  currentPromoCount = 0
  
  try:
    ABC(promoMainSetUp = 1)
    
#     currPromoMain = spark.sql('''select SMA_PROMO_LINK_UPC, 
#                                         SMA_STORE, 
#                                         first_value(SMA_ITM_EFF_DATE) as SMA_ITM_EFF_DATE,
#                                         first_value(SMA_LINK_HDR_COUPON) as SMA_LINK_HDR_COUPON,
#                                         first_value(SMA_LINK_HDR_LOCATION) as SMA_LINK_HDR_LOCATION,
#                                         first_value(SMA_LINK_APPLY_DATE) as SMA_LINK_APPLY_DATE,
#                                         first_value(SMA_LINK_APPLY_TIME) as SMA_LINK_APPLY_TIME,
#                                         first_value(SMA_LINK_END_DATE) as SMA_LINK_END_DATE,
#                                         first_value(SMA_LINK_CHG_TYPE) as SMA_LINK_CHG_TYPE,
#                                         first_value(SMA_CHG_TYPE) as SMA_CHG_TYPE,
#                                         first_value(SMA_BATCH_SERIAL_NBR) as SMA_BATCH_SERIAL_NBR,
#                                         first_value(INSERT_ID) as INSERT_ID,
#                                         first_value(INSERT_TIMESTAMP) as INSERT_TIMESTAMP
#                                       from delta.`{}` where SMA_ITM_EFF_DATE = '{}' and INSERT_ID='{}' and SMA_LINK_END_DATE >= current_date()
#                                       group by SMA_PROMO_LINK_UPC, SMA_STORE
#                                       order by INSERT_TIMESTAMP desc'''.format(PromotionMainDeltaPath, previousDate, PipelineID))
    
    currPromoMain = spark.sql('''select SMA_PROMO_LINK_UPC, 
                                    SMA_STORE, 
                                    SMA_ITM_EFF_DATE,
                                    SMA_LINK_HDR_COUPON,
                                    SMA_LINK_HDR_LOCATION,
                                    SMA_LINK_APPLY_DATE,
                                    SMA_LINK_APPLY_TIME,
                                    SMA_LINK_END_DATE,
                                    SMA_LINK_CHG_TYPE,
                                    SMA_CHG_TYPE,
                                    SMA_BATCH_SERIAL_NBR,
                                    INSERT_ID,
                                    INSERT_TIMESTAMP
                             from (select *, 
                                          ROW_NUMBER() OVER(PARTITION BY SMA_PROMO_LINK_UPC, SMA_STORE ORDER BY INSERT_TIMESTAMP DESC) as row_number 
                                   from delta.`{}`) 
                             where row_number=1 and SMA_ITM_EFF_DATE = '{}' and INSERT_ID='{}' and SMA_LINK_END_DATE >= current_date() '''.format(PromotionMainDeltaPath, previousDate, PipelineID))
    
    currPromoMainCount = currPromoMain.count()
    loggerAtt.info(f"No of current day attribute chg rec: {currPromoMainCount}")
    ABC(currPromoMainRec = currPromoMainCount)
  except Exception as ex:
    ABC(promoMainSetUp = 0)
    loggerAtt.error(ex)
    ABC(currPromoMainRec = '')
    err = ErrorReturn('Error', ex,'promo Main Fetch')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
  
  
  if currPromoMain != None:
    if currPromoMain.count() > 0:
      try:
        temp_table_name = "promoMainTemp"
        
        currPromoMain.createOrReplaceTempView(temp_table_name)
        
        currPromoMain = spark.sql('''Select * from (SELECT promoLink.SMA_LINK_HDR_COUPON as SMA_LINK_HDR_COUPON,
                                    promoLink.SMA_LINK_HDR_LOCATION as SMA_LINK_HDR_LOCATION,
                                    promoMainTemp.SMA_STORE as SMA_STORE,
                                    promoLink.SMA_CHG_TYPE as SMA_CHG_TYPE,
                                    promoLink.SMA_LINK_START_DATE as SMA_LINK_START_DATE,
                                    promoLink.SMA_LINK_RCRD_TYPE as SMA_LINK_RCRD_TYPE,
                                    promoLink.SMA_LINK_END_DATE as SMA_LINK_END_DATE,
                                    promoLink.SMA_ITM_EFF_DATE as SMA_ITM_EFF_DATE,
                                    promoLink.SMA_LINK_CHG_TYPE as SMA_LINK_CHG_TYPE,
                                    promoMainTemp.SMA_PROMO_LINK_UPC as SMA_PROMO_LINK_UPC, 
                                    promoLink.SMA_LINK_HDR_MAINT_TYPE as SMA_LINK_HDR_MAINT_TYPE,
                                    promoLink.SMA_LINK_ITEM_NBR as SMA_LINK_ITEM_NBR,
                                    promoLink.SMA_LINK_OOPS_ADWK as SMA_LINK_OOPS_ADWK,
                                    promoLink.SMA_LINK_OOPS_FILE_ID as SMA_LINK_OOPS_FILE_ID,
                                    promoLink.SMA_LINK_TYPE as SMA_LINK_TYPE,
                                    promoLink.SMA_LINK_SYS_DIGIT as SMA_LINK_SYS_DIGIT,
                                    promoLink.SMA_LINK_FAMCD_PROMOCD as SMA_LINK_FAMCD_PROMOCD,
                                    promoLink.SMA_BATCH_SERIAL_NBR as SMA_BATCH_SERIAL_NBR,
                                    promoMainTemp.SMA_LINK_APPLY_DATE as SMA_LINK_APPLY_DATE,
                                    promoMainTemp.SMA_LINK_APPLY_TIME as SMA_LINK_APPLY_TIME,
                                    promoLink.INSERT_ID as INSERT_ID,
                                    promoLink.INSERT_TIMESTAMP as INSERT_TIMESTAMP,
                                    promoLink.LAST_UPDATE_ID as LAST_UPDATE_ID,
                                    promoLink.LAST_UPDATE_TIMESTAMP as LAST_UPDATE_TIMESTAMP,
                                    ROW_NUMBER() OVER(PARTITION BY promoMainTemp.SMA_STORE, promoMainTemp.SMA_PROMO_LINK_UPC 
                                                      ORDER BY promoLink.SMA_LINK_START_DATE DESC, promoLink.INSERT_TIMESTAMP DESC) as row_number
                             FROM promoMainTemp 
                             LEFT JOIN (SELECT * FROM delta.`{}` 
                                       WHERE '{}' BETWEEN SMA_LINK_START_DATE AND SMA_LINK_END_DATE 
                                             and SMA_LINK_END_DATE >= current_date()
                                             and LAST_UPDATE_ID='{}') as promoLink
                             ON promoLink.SMA_PROMO_LINK_UPC = promoMainTemp.SMA_PROMO_LINK_UPC 
                                AND promoLink.SMA_STORE = promoMainTemp.SMA_STORE) 
                             Where row_number=1'''.format(PromotionLinkDeltaPath, previousDate, PipelineID))
        currPromoMain = currPromoMain.drop(col('row_number'))
        
#         currPromoMain = spark.sql('''SELECT first_value(promoLink.SMA_LINK_HDR_COUPON) as SMA_LINK_HDR_COUPON,
#                                         first_value(promoLink.SMA_LINK_HDR_LOCATION) as SMA_LINK_HDR_LOCATION,
#                                         promoMainTemp.SMA_STORE as SMA_STORE,
#                                         first_value(promoLink.SMA_CHG_TYPE) as SMA_CHG_TYPE,
#                                         first_value(promoLink.SMA_LINK_START_DATE) as SMA_LINK_START_DATE,
#                                         first_value(promoLink.SMA_LINK_RCRD_TYPE) as SMA_LINK_RCRD_TYPE,
#                                         first_value(promoLink.SMA_LINK_END_DATE) as SMA_LINK_END_DATE,
#                                         first_value(promoLink.SMA_ITM_EFF_DATE) as SMA_ITM_EFF_DATE,
#                                         first_value(promoLink.SMA_LINK_CHG_TYPE) as SMA_LINK_CHG_TYPE,
#                                         promoMainTemp.SMA_PROMO_LINK_UPC as SMA_PROMO_LINK_UPC, 
#                                         first_value(promoLink.SMA_LINK_HDR_MAINT_TYPE) as SMA_LINK_HDR_MAINT_TYPE,
#                                         first_value(promoLink.SMA_LINK_ITEM_NBR) as SMA_LINK_ITEM_NBR,
#                                         first_value(promoLink.SMA_LINK_OOPS_ADWK) as SMA_LINK_OOPS_ADWK,
#                                         first_value(promoLink.SMA_LINK_OOPS_FILE_ID) as SMA_LINK_OOPS_FILE_ID,
#                                         first_value(promoLink.SMA_LINK_TYPE) as SMA_LINK_TYPE,
#                                         first_value(promoLink.SMA_LINK_SYS_DIGIT) as SMA_LINK_SYS_DIGIT,
#                                         first_value(promoLink.SMA_LINK_FAMCD_PROMOCD) as SMA_LINK_FAMCD_PROMOCD,
#                                         first_value(promoLink.SMA_BATCH_SERIAL_NBR) as SMA_BATCH_SERIAL_NBR,
#                                         first_value(promoMainTemp.SMA_LINK_APPLY_DATE) as SMA_LINK_APPLY_DATE,
#                                         first_value(promoMainTemp.SMA_LINK_APPLY_TIME) as SMA_LINK_APPLY_TIME,
#                                         first_value(promoLink.INSERT_ID) as INSERT_ID,
#                                         first_value(promoLink.INSERT_TIMESTAMP) as INSERT_TIMESTAMP,
#                                         first_value(promoLink.LAST_UPDATE_ID) as LAST_UPDATE_ID,
#                                         first_value(promoLink.LAST_UPDATE_TIMESTAMP) as LAST_UPDATE_TIMESTAMP
#                                       FROM promoMainTemp 
#                                         LEFT JOIN (SELECT * FROM delta.`{}` WHERE '{}' BETWEEN SMA_LINK_START_DATE 
#                                                                                                   AND SMA_LINK_END_DATE  and LAST_UPDATE_ID='{}' and SMA_LINK_END_DATE >= current_date()) as promoLink
#                                       ON promoLink.SMA_PROMO_LINK_UPC = promoMainTemp.SMA_PROMO_LINK_UPC 
#                                         AND promoLink.SMA_STORE = promoMainTemp.SMA_STORE 
#                                       GROUP BY promoMainTemp.SMA_PROMO_LINK_UPC, promoMainTemp.SMA_STORE
#                                       ORDER BY SMA_LINK_START_DATE DESC, INSERT_TIMESTAMP DESC'''.format(PromotionLinkDeltaPath, previousDate, PipelineID))

        currPromoMainCount = currPromoMain.count()
        loggerAtt.info(f"No of current day records after Promo Link Join: {currPromoMainCount}")
        ABC(currPromoRec = currPromoMainCount)
        spark.catalog.dropTempView(temp_table_name)       
      except Exception as ex:
        ABC(promoMainSetUp = 0)
        loggerAtt.error(ex)
        ABC(currPromoRec = '')
        err = ErrorReturn('Error', ex,'promo Main Join Promo Link')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
      
      try:
        currPromoSce1 = currPromoMain.filter((col('SMA_LINK_HDR_COUPON').isNull()))
        
        loggerAtt.info(f"No of current day promo main records not present in promo linking: {currPromoSce1.count()}")
        
      except Exception as ex:
        ABC(promoMainSetUp = 0)
        loggerAtt.error(ex)
        ABC(currPromoRec = '')
        err = ErrorReturn('Error', ex,'promo sce 1')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
      try:
        if currPromoSce1 != None:
          if currPromoSce1.count() != None:
            currPromoSce1 = promoLinkTransSce1(currPromoSce1)
            
            currPromoSce1Count = currPromoSce1.count()
      except Exception as ex:
        ABC(promoMainSetUp = 0)
        loggerAtt.error(ex)
        ABC(currPromoRec = '')
        err = ErrorReturn('Error', ex,'promo sc1 trans')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
      try:
        currPromoSce2 = currPromoMain.filter((col('SMA_LINK_HDR_COUPON').isNotNull()))
        
        loggerAtt.info(f"No of current day promo main records present in promo linking: {currPromoSce2.count()}")
        
      except Exception as ex:
        ABC(promoMainSetUp = 0)
        loggerAtt.error(ex)
        ABC(currPromoRec = '')
        err = ErrorReturn('Error', ex,'promo sc2')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))  
      
      try:
        if currPromoSce2 != None:
          if currPromoSce2.count() != None:
            currPromoSce2 = promoLinkTransSce2(currPromoSce2)
            currPromoSce2Count = currPromoSce2.count()
      except Exception as ex:
        ABC(promoMainSetUp = 0)
        loggerAtt.error(ex)
        ABC(currPromoRec = '')
        err = ErrorReturn('Error', ex,'promo sc2 trans')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
      try:
        if ((currPromoSce1Count > 0) and (currPromoSce2Count > 0)):
          currPromoMainList = [currPromoSce2, currPromoSce1]
          currPromoMain = reduce(DataFrame.unionAll, currPromoMainList)
        elif (currPromoSce2Count > 0):
          currPromoMain = currPromoSce2
        elif (currPromoSce1Count > 0):
          currPromoMain = currPromoSce1
      except Exception as ex:
        ABC(promoMainSetUp = 0)
        loggerAtt.error(ex)
        ABC(currPromoRec = '')
        err = ErrorReturn('Error', ex,'promo join sc1 and sc2')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
      try:
        if currPromoMain != None:
          if currPromoMain.count() != None:
            
            currItemMaster = spark.sql('''select SMA_STORE as STORE, SMA_GTIN_NUM as GTIN_NUM, SMA_MULT_UNIT_RETL, SMA_RETL_MULT_UNIT from delta.`{}`'''.format(itemMasterDeltaPath))
            
            currPromoMain = currPromoMain.join(currItemMaster, [currItemMaster.GTIN_NUM == currPromoMain.SMA_PROMO_LINK_UPC, currItemMaster.STORE == currPromoMain.SMA_STORE], how='inner').select([col(xx) for xx in currPromoMain.columns] + ['SMA_MULT_UNIT_RETL', 'SMA_RETL_MULT_UNIT'])
            
            loggerAtt.info(f"No of promo records after amount fetch from item master: {currPromoMain.count()}")
      except Exception as ex:
        ABC(promoMainSetUp = 0)
        loggerAtt.error(ex)
        ABC(currPromoRec = '')
        err = ErrorReturn('Error', ex,'Amount fetch from Item Master')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
  return currPromoMain

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Update promo details in item master

# COMMAND ----------

def salePriceCalFunc(currPromo):
  currPromoSc1 = None
  try:
    ABC(currPromoCouponFetchCheck = 1)
    if currPromo != None:
      if currPromo.count() > 0:
        
        currPromoSc1 = currPromo.filter((col('SMA_LINK_HDR_COUPON').isNull()))
        
        loggerAtt.info(f"No of currPromo records with null coupon no: {currPromoSc1.count()}")
        
        currPromo = currPromo.filter((col('SMA_LINK_HDR_COUPON').isNotNull()))
        
        temp_table_name = 'currPromo'
        currPromo.createOrReplaceTempView(temp_table_name)
        
        loggerAtt.info(f"No of currPromo records before coupon join: {currPromo.count()}")
        
        currPromo = spark.sql('''select SMA_LINK_HDR_COUPON, 
                                        SMA_LINK_HDR_LOCATION, 
                                        SMA_STORE, 
                                        SMA_CHG_TYPE, 
                                        SMA_LINK_START_DATE, 
                                        SMA_LINK_RCRD_TYPE, 
                                        SMA_LINK_END_DATE, 
                                        SMA_ITM_EFF_DATE, 
                                        SMA_LINK_CHG_TYPE, 
                                        SMA_PROMO_LINK_UPC, 
                                        SMA_LINK_HDR_MAINT_TYPE, 
                                        SMA_LINK_ITEM_NBR, 
                                        SMA_LINK_OOPS_ADWK, 
                                        SMA_LINK_OOPS_FILE_ID, 
                                        SMA_LINK_TYPE,
                                        SMA_LINK_SYS_DIGIT,
                                        SMA_LINK_FAMCD_PROMOCD,
                                        SMA_BATCH_SERIAL_NBR,
                                        SMA_LINK_APPLY_DATE,
                                        SMA_LINK_APPLY_TIME,
                                        currPromo.INSERT_ID as INSERT_ID,
                                        currPromo.INSERT_TIMESTAMP as INSERT_TIMESTAMP,
                                        currPromo.LAST_UPDATE_ID as LAST_UPDATE_ID,
                                        currPromo.LAST_UPDATE_TIMESTAMP as LAST_UPDATE_TIMESTAMP,
                                        couponRec.PERF_DETL_SUB_TYPE as PERF_DETL_SUB_TYPE,
                                        SALE_PRICE,
                                        currPromo.SALE_QUANTITY as SALE_QUANTITY,
                                        SMA_RETL_MULT_UNIT,
                                        SMA_MULT_UNIT_RETL,
                                        NUM_TO_BUY_1,
                                        CHANGE_AMOUNT_PCT
                                  from (select * from delta.`{}` where '{}' BETWEEN START_DATE AND END_DATE) as couponRec
                                  join currPromo
                                  ON  currPromo.SMA_LINK_HDR_COUPON = couponRec.COUPON_NO and 
                                      currPromo.SMA_STORE = couponRec.LOCATION'''.format(couponDeltaPath, currentDate))
        
        loggerAtt.info(f"No of currPromo records after coupon join: {currPromo.count()}")
        spark.catalog.dropTempView(temp_table_name)
  except Exception as ex:
    ABC(currPromoCouponFetchCheck = 0)
    loggerAtt.error(ex)
    ABC(currPromoRec = '')
    err = ErrorReturn('Error', ex,'promo rec coupon value fetch')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
  try:
    ABC(salePriceTransCheck = 1)
    if currPromo != None:
      if currPromo.count() > 0:
        currPromo = salePriceCalculation(currPromo)
  except Exception as ex:
    ABC(salePriceTransCheck = 0)
    loggerAtt.error(ex)
    ABC(currPromoRec = '')
    err = ErrorReturn('Error', ex,'Sale Price Calculation')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
  try:
    if currPromo != None:
      if currPromo.count() > 0:
        if currPromoSc1 != None:
          if currPromoSc1.count() > 0:
            currPromoList = [currPromo, currPromoSc1]
            currPromo = reduce(DataFrame.unionAll, currPromoList)
            
  except Exception as ex:
    ABC(promoMainSetUp = 0)
    loggerAtt.error(ex)
    ABC(currPromoRec = '')
    err = ErrorReturn('Error', ex,'promo sc1 union')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
  try:
    if currPromo != None:
      if currPromo.count() > 0:
        ABC(promoRecUpdateCheck = 1)
        currPromo = currPromo.withColumn("LAST_UPDATE_ID", lit(PipelineID))
        currPromo = currPromo.withColumn("LAST_UPDATE_TIMESTAMP", lit(currentTimeStamp).cast(TimestampType()))
        temp_table_name = "currPromo"

        currPromo.createOrReplaceTempView(temp_table_name)
        
        loggerAtt.info(f"No of currPromo records updating item master: {currPromo.count()}")

        spark.sql('''MERGE INTO delta.`{}` as Item 
            USING currPromo 
            ON Item.SMA_GTIN_NUM = currPromo.SMA_PROMO_LINK_UPC and Item.SMA_STORE= currPromo.SMA_STORE
            WHEN MATCHED Then 
                    Update Set  Item.SMA_LINK_HDR_COUPON = currPromo.SMA_LINK_HDR_COUPON, 
                                Item.SMA_LINK_HDR_LOCATION = currPromo.SMA_LINK_HDR_LOCATION, 
                                Item.SMA_CHG_TYPE = currPromo.SMA_CHG_TYPE, 
                                Item.SMA_LINK_START_DATE = currPromo.SMA_LINK_START_DATE, 
                                Item.SMA_LINK_RCRD_TYPE = currPromo.SMA_LINK_RCRD_TYPE, 
                                Item.SMA_LINK_END_DATE = currPromo.SMA_LINK_END_DATE, 
                                Item.SMA_ITM_EFF_DATE = currPromo.SMA_ITM_EFF_DATE, 
                                Item.SMA_LINK_CHG_TYPE = currPromo.SMA_LINK_CHG_TYPE, 
                                Item.SMA_PL_UPC_REDEF = currPromo.SMA_PROMO_LINK_UPC, 
                                Item.SMA_LINK_HDR_MAINT_TYPE = currPromo.SMA_LINK_HDR_MAINT_TYPE, 
                                Item.SMA_LINK_ITEM_NBR = currPromo.SMA_LINK_ITEM_NBR, 
                                Item.SMA_LINK_OOPS_ADWK = currPromo.SMA_LINK_OOPS_ADWK, 
                                Item.SMA_LINK_OOPS_FILE_ID = currPromo.SMA_LINK_OOPS_FILE_ID, 
                                Item.SMA_LINK_TYPE = currPromo.SMA_LINK_TYPE, 
                                Item.SMA_LINK_SYS_DIGIT = currPromo.SMA_LINK_SYS_DIGIT, 
                                Item.SMA_LINK_FAMCD_PROMOCD = currPromo.SMA_LINK_FAMCD_PROMOCD, 
                                Item.SMA_BATCH_SERIAL_NBR = currPromo.SMA_BATCH_SERIAL_NBR, 
                                Item.SMA_LINK_APPLY_DATE = currPromo.SMA_LINK_APPLY_DATE, 
                                Item.LAST_UPDATE_ID = currPromo.LAST_UPDATE_ID, 
                                Item.LAST_UPDATE_TIMESTAMP = currPromo.LAST_UPDATE_TIMESTAMP, 
                                Item.PERF_DETL_SUB_TYPE = currPromo.PERF_DETL_SUB_TYPE, 
                                Item.SALE_PRICE = currPromo.SALE_PRICE,
                                Item.SALE_QUANTITY = currPromo.SALE_QUANTITY'''.format(itemMasterDeltaPath))
        
        spark.catalog.dropTempView(temp_table_name)
        loggerAtt.info("Merge into item master with promo record successful")
  except Exception as ex:
    ABC(promoRecUpdateCheck = 0)
    loggerAtt.error(ex)
    ABC(currPromoRec = '')
    err = ErrorReturn('Error', ex,'promo rec update')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  return currPromo

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Fetch all coupon changes for that current day

# COMMAND ----------

def fetchCouponChange():
  currCoupon = None
  try:
    ABC(currCouponFetchCheck=1)
    currCoupon = spark.sql('''select  LOCATION, 
                                      couponRec.PERF_DETL_SUB_TYPE as PERF_DETL_SUB_TYPE,
                                      COUPON_NO,
                                      SMA_STORE,
                                      SMA_GTIN_NUM,
                                      SMA_RETL_MULT_UNIT,
                                      SMA_MULT_UNIT_RETL,
                                      NUM_TO_BUY_1,
                                      CHANGE_AMOUNT_PCT,
                                      todayUpdateItemMaster.LAST_UPDATE_TIMESTAMP as ItemMasterLAST_UPDATE_TIMESTAMP,
                                      couponRec.LAST_UPDATE_TIMESTAMP as couponRecLAST_UPDATE_TIMESTAMP
                                    from (select * from delta.`{}` where DATE(LAST_UPDATE_TIMESTAMP) between '{}' and '{}' and LAST_UPDATE_ID='{}') as couponRec
                                    join delta.`{}` as todayUpdateItemMaster
                                    ON  todayUpdateItemMaster.SMA_LINK_HDR_COUPON = couponRec.COUPON_NO and 
                                        todayUpdateItemMaster.SMA_STORE = couponRec.LOCATION
                                    WHERE DATE(todayUpdateItemMaster.LAST_UPDATE_TIMESTAMP) < '{}' 
                                        or  couponRec.LAST_UPDATE_TIMESTAMP > todayUpdateItemMaster.LAST_UPDATE_TIMESTAMP'''.format(couponDeltaPath, previousDate, currentDate, PipelineID, itemMasterDeltaPath, currentDate))
    ABC(currCouponCount='')
  except Exception as ex:
    loggerAtt.error(ex)
    ABC(currCouponFetchCheck=0)
    ABC(currCouponCount='')
    err = ErrorReturn('Error', ex,'currCoupon fetch')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  if currCoupon != None:
    if currCoupon.count() > 0:
      try:
        ABC(currCouponCount=currCoupon.count())
        
        currCoupon = currCoupon.withColumn('SMA_MULT_UNIT_RETL', round(col('SMA_MULT_UNIT_RETL').cast(DoubleType()), 2))
        currCoupon = currCoupon.withColumn('SMA_RETL_MULT_UNIT', round(col('SMA_RETL_MULT_UNIT').cast(DoubleType()), 2))
        currCoupon = currCoupon.withColumn('CHANGE_AMOUNT_PCT', round(col('CHANGE_AMOUNT_PCT').cast(DoubleType()), 2))
        
        loggerAtt.info("No of coupon changes not in item master: "+str(currCoupon.count()))
        currCoupon = currCoupon.withColumn("SALE_PRICE", salePriceCalculationUDF(col('PERF_DETL_SUB_TYPE'), col('NUM_TO_BUY_1'), col('SMA_MULT_UNIT_RETL'), col('SMA_RETL_MULT_UNIT'), col('CHANGE_AMOUNT_PCT')))
        
        currCoupon = currCoupon.withColumn("SALE_PRICE", floor(col('SALE_PRICE') * 100)/100)
        
        currCoupon = currCoupon.withColumn('SMA_MULT_UNIT_RETL', col('SMA_MULT_UNIT_RETL').cast(FloatType()))
        currCoupon = currCoupon.withColumn('SMA_RETL_MULT_UNIT', col('SMA_RETL_MULT_UNIT').cast(FloatType()))
        currCoupon = currCoupon.withColumn('CHANGE_AMOUNT_PCT', col('CHANGE_AMOUNT_PCT').cast(FloatType()))
        currCoupon = currCoupon.withColumn('SALE_PRICE', col('SALE_PRICE').cast(FloatType()))

        currCoupon = currCoupon.withColumn("SALE_PRICE", when(col('SALE_PRICE').isNull(), lit(None)).otherwise(lpad(formatZerosUDF(round((col('SALE_PRICE')), 2)),10,'0')).cast(StringType()))

        currCoupon = currCoupon.withColumn("SALE_QUANTITY", saleQuantityCalculationUDF(col('PERF_DETL_SUB_TYPE'), col('NUM_TO_BUY_1')))

#         currCoupon = currCoupon.withColumn("LAST_UPDATE_ID", lit(PipelineID))

#         currCoupon = currCoupon.withColumn("LAST_UPDATE_TIMESTAMP", lit(currentTimeStamp).cast(TimestampType()))
      except Exception as ex:
        loggerAtt.error(ex)
        ABC(currCouponCount='')
        err = ErrorReturn('Error', ex,'currCoupon transformation')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
      try:
        
        currCoupon = currCoupon.withColumn("LAST_UPDATE_ID", lit(PipelineID))
        currCoupon = currCoupon.withColumn("LAST_UPDATE_TIMESTAMP", lit(currentTimeStamp).cast(TimestampType()))
        
        temp_table_name = "currCoupon"

        currCoupon.createOrReplaceTempView(temp_table_name)
        
        spark.sql('''MERGE INTO delta.`{}` as Item 
            USING currCoupon 
            ON Item.SMA_GTIN_NUM = currCoupon.SMA_GTIN_NUM and Item.SMA_STORE= currCoupon.SMA_STORE
            WHEN MATCHED Then 
                    Update Set  Item.LAST_UPDATE_ID = currCoupon.LAST_UPDATE_ID, 
                                Item.LAST_UPDATE_TIMESTAMP = currCoupon.LAST_UPDATE_TIMESTAMP, 
                                Item.PERF_DETL_SUB_TYPE = currCoupon.PERF_DETL_SUB_TYPE, 
                                Item.SALE_PRICE = currCoupon.SALE_PRICE,
                                Item.SALE_QUANTITY = currCoupon.SALE_QUANTITY'''.format(itemMasterDeltaPath))
        spark.catalog.dropTempView(temp_table_name)
      except Exception as ex:
        loggerAtt.error(ex)
        ABC(currCouponCount='')
        err = ErrorReturn('Error', ex,'currCoupon update')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Archival

# COMMAND ----------

def archiveTaxTable():
  
  loggerAtt.info("archiveTaxHolidayTable")
  try:
    initial_recs = spark.sql("""SELECT * from delta.`{}`;""".format(taxHolidayDeltaPath))
    initialCount = initial_recs.count()
    loggerAtt.info("No of records in PromotionLink table: "+str(initialCount)+"," +str(len(initial_recs.columns)))
    initial_recs = initialCount
    ABC(taxTblInitCount=initialCount)
    
    taxArchival_df = spark.sql("""SELECT * from delta.`{}` as taxHoliday where taxHoliday.DB_HOLIDAY_END_DATE < '{}' ;""".format(taxHolidayDeltaPath, currentDate))
    taxArchivalCount = taxArchival_df.count()
    loggerAtt.info(f"No of tax holiday records to get deleted: {taxArchivalCount}")
    ABC(taxTblArchivalCount=taxArchivalCount)
    taxArchival_df.write.mode('Append').format('parquet').save(taxHolidayArchival)
    
    if taxArchivalCount > 0:
      spark.sql('''DELETE from delta.`{}` as taxHoliday where taxHoliday.DB_HOLIDAY_END_DATE < '{}' '''.format(taxHolidayDeltaPath, currentDate))

    appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(taxHolidayDeltaPath))
    loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
    appended_recs = appended_recs.head(1)
    ABC(taxTblFinalCount=appended_recs[0][0])
  except Exception as ex:
    loggerAtt.error(ex)
    ABC(taxTblInitCount='')
    ABC(taxTblFinalCount='')
    ABC(taxTblArchivalCount='')
    err = ErrorReturn('Error', ex,'taxHolidayArc')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  pass

# COMMAND ----------

def archivePriceTable():
  loggerAtt.info("archivePriceTable")
  try:
    initial_recs = spark.sql("""SELECT * from delta.`{}`;""".format(PriceChangeDeltaPath))
    initialCount = initial_recs.count()
    loggerAtt.info("No of records in Price Change table: "+str(initialCount)+"," +str(len(initial_recs.columns)))
    initial_recs = initialCount
    ABC(priceChangeArcTblInitCount=initialCount)
    
    priceChgArchival_df = spark.sql("""select * from (select  SMA_STORE, 
                                                              SMA_GTIN_NUM, 
                                                              SMA_ITM_EFF_DATE, 
                                                              INSERT_TIMESTAMP, 
                                                              DENSE_RANK() OVER (PARTITION BY SMA_STORE, SMA_GTIN_NUM ORDER BY SMA_ITM_EFF_DATE desc, INSERT_TIMESTAMP desc) AS Rank  
                                                      from delta.`{}` 
                                                      where SMA_ITM_EFF_DATE < '{}' 
                                                      ORDER BY SMA_STORE, SMA_GTIN_NUM) 
                                                  where Rank > 2;""".format(PriceChangeDeltaPath, currentDate))
    priceChgArchivalCount = priceChgArchival_df.count()
    loggerAtt.info(f"No of Price Change records to get deleted: {priceChgArchivalCount}")
    ABC(priceChgArchivalCount=priceChgArchivalCount)
    priceChgArchival_df.write.mode('Append').format('parquet').save(priceChangeArchival)
    
    temp_table_name = "priceChgArchival_df"
    priceChgArchival_df.createOrReplaceTempView(temp_table_name)
    
    spark.sql('''MERGE into delta.`{}` as PriceChangeTmp
                  using priceChgArchival_df
                  on PriceChangeTmp.SMA_STORE = priceChgArchival_df.SMA_STORE
                   and PriceChangeTmp.SMA_GTIN_NUM = priceChgArchival_df.SMA_GTIN_NUM
                   and PriceChangeTmp.SMA_ITM_EFF_DATE = priceChgArchival_df.SMA_ITM_EFF_DATE
                   and PriceChangeTmp.INSERT_TIMESTAMP = priceChgArchival_df.INSERT_TIMESTAMP
                  when matched then delete'''.format(PriceChangeDeltaPath))

    appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(PriceChangeDeltaPath))
    loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
    appended_recs = appended_recs.head(1)
    ABC(priceChangeArcTblFinalCount=appended_recs[0][0])
    
    spark.catalog.dropTempView(temp_table_name)
  except Exception as ex:
    loggerAtt.error(ex)
    ABC(priceChangeArcTblInitCount='')
    ABC(priceChangeArcTblFinalCount='')
    err = ErrorReturn('Error', ex,'priceChangeArc based on Rank')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

def archivePromotionTable():
  loggerAtt.info("archivePromotionTable")
  try:
    initial_recs = spark.sql("""SELECT * from delta.`{}`;""".format(PromotionLinkDeltaPath))
    initialCount = initial_recs.count()
    loggerAtt.info("No of records in PromotionLink table: "+str(initialCount)+"," +str(len(initial_recs.columns)))
    initial_recs = initialCount
    ABC(promotionLinkArcTblInitCount=initialCount)
    
    promoLinkArchival_df = spark.sql("""SELECT * from delta.`{}` as PromotionLink where PromotionLink.SMA_LINK_END_DATE < '{}' ;""".format(PromotionLinkDeltaPath, currentDate))
    promoLinkArchivalCount = promoLinkArchival_df.count()
    loggerAtt.info(f"No of Promo Link records to get deleted: {promoLinkArchivalCount}")
    ABC(promoLinkArchivalCount=promoLinkArchivalCount)
    promoLinkArchival_df.write.mode('Append').format('parquet').save(promoLinkArchival)

    spark.sql('''DELETE from delta.`{}` as PromotionLink where PromotionLink.SMA_LINK_END_DATE < '{}' '''.format(PromotionLinkDeltaPath, currentDate))

    appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(PromotionLinkDeltaPath))
    loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
    appended_recs = appended_recs.head(1)
    ABC(promotionLinkArcTblFinalCount=appended_recs[0][0])
  except Exception as ex:
    loggerAtt.error(ex)
    ABC(promotionLinkArcTblInitCount='')
    ABC(promotionLinkArcTblFinalCount='')
    err = ErrorReturn('Error', ex,'promotionLinkArc')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
  try:
    initial_recs = spark.sql("""SELECT * from delta.`{}`;""".format(PromotionMainDeltaPath))
    initialCount = initial_recs.count()
    loggerAtt.info("No of records in PromotionMain table: "+str(initialCount)+"," +str(len(initial_recs.columns)))
    initial_recs = initialCount
    ABC(promotionMainArcTblInitCount=initialCount)
    
    promoMainArchival_df = spark.sql("""SELECT * from delta.`{}` as PromotionMain where PromotionMain.SMA_LINK_END_DATE < '{}' ;""".format(PromotionMainDeltaPath, currentDate))
    promoMainArchivalCount = promoMainArchival_df.count()
    loggerAtt.info(f"No of Promo Main records to get deleted: {promoMainArchivalCount}")
    ABC(promoMainArchivalCount=promoMainArchivalCount)
    promoMainArchival_df.write.mode('Append').format('parquet').save(promoMainArchival)

    spark.sql('''DELETE from delta.`{}` as PromotionMain where PromotionMain.SMA_LINK_END_DATE < '{}' '''.format(PromotionMainDeltaPath, currentDate))

    appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(PromotionMainDeltaPath))
    loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
    appended_recs = appended_recs.head(1)
    ABC(promotionMainArcTblFinalCount=appended_recs[0][0])
  except Exception as ex:
    loggerAtt.error(ex)
    ABC(promotionMainArcTblInitCount='')
    ABC(promotionMainArcTblFinalCount='')
    err = ErrorReturn('Error', ex,'promotionMainArc')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
  try:
    itemMasterLinkArchival_df = spark.sql("""SELECT * from delta.`{}` where SMA_LINK_END_DATE < '{}' ;""".format(itemMasterDeltaPath, currentDate))
    itemMasterLinkArchivalCount = itemMasterLinkArchival_df.count()
    loggerAtt.info(f"No of Item Master records with link end date less then current date: {itemMasterLinkArchivalCount}")
    ABC(itemMasterLinkArchivalCount=itemMasterLinkArchivalCount)
    
    itemMasterLinkArchival_df = promoLinkTransSce1(itemMasterLinkArchival_df)
    
    itemMasterLinkArchival_df = itemMasterLinkArchival_df.withColumn("SMA_PL_UPC_REDEF", lit(None))
    itemMasterLinkArchival_df = itemMasterLinkArchival_df.withColumn("LAST_UPDATE_ID", lit(PipelineID))
    itemMasterLinkArchival_df = itemMasterLinkArchival_df.withColumn("LAST_UPDATE_TIMESTAMP", lit(currentTimeStamp).cast(TimestampType()))
    
    temp_table_name = "itemMasterLinkArchival_df"
    itemMasterLinkArchival_df.createOrReplaceTempView(temp_table_name)
    
    spark.sql('''MERGE into delta.`{}` as itemMasterTmp
                  using itemMasterLinkArchival_df
                  on itemMasterTmp.SMA_STORE = itemMasterLinkArchival_df.SMA_STORE
                   and itemMasterTmp.SMA_GTIN_NUM = itemMasterLinkArchival_df.SMA_GTIN_NUM
                  when matched then
                  Update Set  itemMasterTmp.SMA_LINK_HDR_COUPON = itemMasterLinkArchival_df.SMA_LINK_HDR_COUPON, 
                                itemMasterTmp.SMA_LINK_HDR_LOCATION = itemMasterLinkArchival_df.SMA_LINK_HDR_LOCATION, 
                                itemMasterTmp.SMA_CHG_TYPE = itemMasterLinkArchival_df.SMA_CHG_TYPE, 
                                itemMasterTmp.SMA_LINK_START_DATE = itemMasterLinkArchival_df.SMA_LINK_START_DATE, 
                                itemMasterTmp.SMA_LINK_RCRD_TYPE = itemMasterLinkArchival_df.SMA_LINK_RCRD_TYPE, 
                                itemMasterTmp.SMA_LINK_END_DATE = itemMasterLinkArchival_df.SMA_LINK_END_DATE, 
                                itemMasterTmp.SMA_LINK_CHG_TYPE = itemMasterLinkArchival_df.SMA_LINK_CHG_TYPE, 
                                itemMasterTmp.SMA_PL_UPC_REDEF = itemMasterLinkArchival_df.SMA_PL_UPC_REDEF, 
                                itemMasterTmp.SMA_LINK_HDR_MAINT_TYPE = itemMasterLinkArchival_df.SMA_LINK_HDR_MAINT_TYPE, 
                                itemMasterTmp.SMA_LINK_ITEM_NBR = itemMasterLinkArchival_df.SMA_LINK_ITEM_NBR, 
                                itemMasterTmp.SMA_LINK_OOPS_ADWK = itemMasterLinkArchival_df.SMA_LINK_OOPS_ADWK, 
                                itemMasterTmp.SMA_LINK_OOPS_FILE_ID = itemMasterLinkArchival_df.SMA_LINK_OOPS_FILE_ID, 
                                itemMasterTmp.SMA_LINK_TYPE = itemMasterLinkArchival_df.SMA_LINK_TYPE, 
                                itemMasterTmp.SMA_LINK_SYS_DIGIT = itemMasterLinkArchival_df.SMA_LINK_SYS_DIGIT, 
                                itemMasterTmp.SMA_LINK_FAMCD_PROMOCD = itemMasterLinkArchival_df.SMA_LINK_FAMCD_PROMOCD, 
                                itemMasterTmp.SMA_LINK_APPLY_DATE = itemMasterLinkArchival_df.SMA_LINK_APPLY_DATE, 
                                itemMasterTmp.SMA_LINK_APPLY_TIME = itemMasterLinkArchival_df.SMA_LINK_APPLY_TIME, 
                                itemMasterTmp.LAST_UPDATE_ID = itemMasterLinkArchival_df.LAST_UPDATE_ID, 
                                itemMasterTmp.LAST_UPDATE_TIMESTAMP = itemMasterLinkArchival_df.LAST_UPDATE_TIMESTAMP, 
                                itemMasterTmp.PERF_DETL_SUB_TYPE = itemMasterLinkArchival_df.PERF_DETL_SUB_TYPE, 
                                itemMasterTmp.SALE_PRICE = itemMasterLinkArchival_df.SALE_PRICE,
                                itemMasterTmp.SALE_QUANTITY = itemMasterLinkArchival_df.SALE_QUANTITY'''.format(itemMasterDeltaPath))
    
    spark.catalog.dropTempView(temp_table_name)
  except Exception as ex:
    loggerAtt.error(ex)
    ABC(promotionMainArcTblInitCount='')
    ABC(promotionMainArcTblFinalCount='')
    err = ErrorReturn('Error', ex,'promotionMainArc')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  

# COMMAND ----------

def archivalItemWithStatusX():
  loggerAtt.info("archivalItemWithStatusX")
  try:
    ABC(archivalItemWithStatusXCheck = 1)
    itemMainWithStatusC = spark.sql(''' UPDATE delta.`{}` SET SMA_ITEM_STATUS = 'D', LAST_UPDATE_TIMESTAMP=current_timestamp(), LAST_UPDATE_ID='{}'
                                        WHERE SMA_ITEM_STATUS = 'X' and  DATE(LAST_UPDATE_TIMESTAMP) <= date_sub('{}' , 14)'''.format(ItemMainDeltaPath, PipelineID, currentDate))
  
  except Exception as ex:
    ABC(archivalItemWithStatusXCheck = 0)
    loggerAtt.error(ex)
    ABC(currPromoRec = '')
    err = ErrorReturn('Error', ex,'itemMainWithStatusX update')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

def archivalItemWithStatusD():
  loggerAtt.info("archivalItemWithStatusD")
  try:
    ABC(archivalItemWithStatusDCheck = 1)
    
    itemMainWithStatusD = spark.sql('''select * from delta.`{}` where SMA_ITEM_STATUS = 'D' '''.format(ItemMainDeltaPath))
    
    loggerAtt.info("No of records in Item Main table with status D: "+str(itemMainWithStatusD.count()))
    ABC(itemMainWithStatusDCount=itemMainWithStatusD.count())
    
    temp_table_name = "itemMainWithStatusD"
    itemMainWithStatusD.createOrReplaceTempView(temp_table_name)
  
  except Exception as ex:
    ABC(archivalItemWithStatusDCheck = 0)
    ABC(itemMainWithStatusDCount='')
    loggerAtt.error(ex)
    ABC(currPromoRec = '')
    err = ErrorReturn('Error', ex,'itemMainWithStatusD fetch')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
  try:
    initial_recs = spark.sql("""SELECT * from delta.`{}`;""".format(PriceChangeDeltaPath))
    initialCount = initial_recs.count()
    loggerAtt.info("No of records in PriceChange table: "+str(initialCount)+"," +str(len(initial_recs.columns)))
    initial_recs = initialCount
    ABC(priceChangeArcTblInitCount=initialCount)
    
    priceChangeArchival_df = spark.sql("""SELECT priceChange.* from delta.`{}` as priceChange
                                          inner join itemMainWithStatusD
                                          on priceChange.SMA_STORE = itemMainWithStatusD.SMA_STORE
                                           and  priceChange.SMA_GTIN_NUM = itemMainWithStatusD.SMA_GTIN_NUM""".format(PriceChangeDeltaPath))
    priceChangeArchivalCount = priceChangeArchival_df.count()
    loggerAtt.info(f"No of Price Change records to get deleted: {priceChangeArchivalCount}")
    ABC(priceChangeArchivalCount=priceChangeArchivalCount)
    priceChangeArchival_df.write.mode('Append').format('parquet').save(priceChangeArchival)

    spark.sql('''MERGE into delta.`{}` as priceChange
                  using itemMainWithStatusD
                  on priceChange.SMA_STORE = itemMainWithStatusD.SMA_STORE
                   and  priceChange.SMA_GTIN_NUM = itemMainWithStatusD.SMA_GTIN_NUM
                  when matched then delete'''.format(PriceChangeDeltaPath))

    appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(PriceChangeDeltaPath))
    loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
    appended_recs = appended_recs.head(1)
    ABC(priceChangeArcTblFinalCount=appended_recs[0][0])
  except Exception as ex:
    loggerAtt.error(ex)
    ABC(priceChangeArcTblInitCount='')
    ABC(priceChangeArcTblFinalCount='')
    ABC(priceChangeArchivalCount='')
    err = ErrorReturn('Error', ex,'priceChangeArc')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
  try:
    initial_recs = spark.sql("""SELECT * from delta.`{}`;""".format(PromotionLinkDeltaPath))
    initialCount = initial_recs.count()
    loggerAtt.info("No of records in PromotionLink table: "+str(initialCount)+"," +str(len(initial_recs.columns)))
    initial_recs = initialCount
    ABC(promotionLinkArcTblInitCount=initialCount)
    
    promoLinkArchival_df = spark.sql("""SELECT PromotionLink.* from delta.`{}` as PromotionLink
                                        inner join itemMainWithStatusD
                                        on PromotionLink.SMA_STORE = itemMainWithStatusD.SMA_STORE
                                         and  PromotionLink.SMA_PROMO_LINK_UPC = itemMainWithStatusD.SMA_GTIN_NUM;""".format(PromotionLinkDeltaPath))
    promoLinkArchivalCount = promoLinkArchival_df.count()
    loggerAtt.info(f"No of Promo Link records to get deleted: {promoLinkArchivalCount}")
    ABC(promoLinkArchivalCount=promoLinkArchivalCount)
    promoLinkArchival_df.write.mode('Append').format('parquet').save(promoLinkArchival)

    spark.sql('''MERGE into delta.`{}` as PromotionLink
                  using itemMainWithStatusD
                  on PromotionLink.SMA_STORE = itemMainWithStatusD.SMA_STORE
                   and  PromotionLink.SMA_PROMO_LINK_UPC = itemMainWithStatusD.SMA_GTIN_NUM
                  when matched then delete'''.format(PromotionLinkDeltaPath))

    appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(PromotionLinkDeltaPath))
    loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
    appended_recs = appended_recs.head(1)
    ABC(promotionLinkArcTblFinalCount=appended_recs[0][0])
  except Exception as ex:
    loggerAtt.error(ex)
    ABC(promotionLinkArcTblInitCount='')
    ABC(promotionLinkArcTblFinalCount='')
    ABC(promoLinkArchivalCount='')
    err = ErrorReturn('Error', ex,'promotionLinkArc')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
  try:
    initial_recs = spark.sql("""SELECT * from delta.`{}`;""".format(PromotionMainDeltaPath))
    initialCount = initial_recs.count()
    loggerAtt.info("No of records in PromotionMain table: "+str(initialCount)+"," +str(len(initial_recs.columns)))
    initial_recs = initialCount
    ABC(promotionMainArcTblInitCount=initialCount)
    
    promoMainArchival_df = spark.sql("""SELECT PromotionMain.* from delta.`{}` as PromotionMain
                                        inner join itemMainWithStatusD
                                        on PromotionMain.SMA_STORE = itemMainWithStatusD.SMA_STORE
                                         and  PromotionMain.SMA_PROMO_LINK_UPC = itemMainWithStatusD.SMA_GTIN_NUM;""".format(PromotionMainDeltaPath))
    promoMainArchivalCount = promoMainArchival_df.count()
    loggerAtt.info(f"No of Promo Main records to get deleted: {promoMainArchivalCount}")
    ABC(promoMainArchivalCount=promoMainArchivalCount)
    promoMainArchival_df.write.mode('Append').format('parquet').save(promoMainArchival)

    spark.sql('''MERGE into delta.`{}` as PromotionMain
                  using itemMainWithStatusD
                  on PromotionMain.SMA_STORE = itemMainWithStatusD.SMA_STORE
                   and  PromotionMain.SMA_PROMO_LINK_UPC = itemMainWithStatusD.SMA_GTIN_NUM
                  when matched then delete'''.format(PromotionMainDeltaPath))

    appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(PromotionMainDeltaPath))
    loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
    appended_recs = appended_recs.head(1)
    ABC(promotionMainArcTblFinalCount=appended_recs[0][0])
  except Exception as ex:
    loggerAtt.error(ex)
    ABC(promotionMainArcTblInitCount='')
    ABC(promotionMainArcTblFinalCount='')
    ABC(promoMainArchivalCount='')
    err = ErrorReturn('Error', ex,'promotionMainArc')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
  try:
    initial_recs = spark.sql("""SELECT * from delta.`{}`;""".format(itemMasterDeltaPath))
    initialCount = initial_recs.count()
    loggerAtt.info("No of records in itemMaster table: "+str(initialCount)+"," +str(len(initial_recs.columns)))
    initial_recs = initialCount
    ABC(itemMasterArcTblInitCount=initialCount)
    
    itemMasterArchival_df = spark.sql("""SELECT itemMasterTmp.* from delta.`{}` as itemMasterTmp
                                          inner join itemMainWithStatusD
                                          on itemMasterTmp.SMA_STORE = itemMainWithStatusD.SMA_STORE
                                           and  itemMasterTmp.SMA_GTIN_NUM = itemMainWithStatusD.SMA_GTIN_NUM;""".format(itemMasterDeltaPath))
    itemMasterArchivalCount = itemMasterArchival_df.count()
    loggerAtt.info(f"No of Item Master records to get deleted: {itemMasterArchivalCount}")
    ABC(itemMasterArchivalCount=itemMasterArchivalCount)
    itemMasterArchival_df.write.mode('Append').format('parquet').save(itemMasterArchival)

    spark.sql('''MERGE into delta.`{}` as itemMasterTmp
                  using itemMainWithStatusD
                  on itemMasterTmp.SMA_STORE = itemMainWithStatusD.SMA_STORE
                   and  itemMasterTmp.SMA_GTIN_NUM = itemMainWithStatusD.SMA_GTIN_NUM
                  when matched then delete'''.format(itemMasterDeltaPath))

    appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(itemMasterDeltaPath))
    loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
    appended_recs = appended_recs.head(1)
    ABC(itemMasterArcTblFinalCount=appended_recs[0][0])
  except Exception as ex:
    loggerAtt.error(ex)
    ABC(itemMasterArcTblInitCount='')
    ABC(itemMasterArcTblFinalCount='')
    ABC(itemMasterArchivalCount='')
    err = ErrorReturn('Error', ex,'itemMasterArc')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
  try:
    initial_recs = spark.sql("""SELECT * from delta.`{}`;""".format(ItemMainDeltaPath))
    initialCount = initial_recs.count()
    loggerAtt.info("No of records in ItemMain table: "+str(initialCount)+"," +str(len(initial_recs.columns)))
    initial_recs = initialCount
    ABC(itemMainArcTblInitCount=initialCount)
    
    itemMainArchival_df = spark.sql("""SELECT * from delta.`{}` where SMA_ITEM_STATUS = 'D';""".format(ItemMainDeltaPath))
    itemMainArchivalCount = itemMainArchival_df.count()
    loggerAtt.info(f"No of Item Main records to get deleted: {itemMainArchivalCount}")
    ABC(itemMainArchivalCount=itemMainArchivalCount)
    itemMainArchival_df.write.mode('Append').format('parquet').save(itemMainArchival)

    spark.sql(''' delete from delta.`{}` where SMA_ITEM_STATUS = 'D' '''.format(ItemMainDeltaPath))

    appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(ItemMainDeltaPath))
    loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
    appended_recs = appended_recs.head(1)
    ABC(itemMainArcTblFinalCount=appended_recs[0][0])

    spark.catalog.dropTempView("itemMainWithStatusD")
  except Exception as ex:
    loggerAtt.error(ex)
    ABC(itemMainArcTblInitCount='')
    ABC(itemMainArcTblFinalCount='')
    ABC(itemMainArchivalCount='')
    err = ErrorReturn('Error', ex,'itemMainArc')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Tax Holiday flag update

# COMMAND ----------

def taxHolidayUpdate(itemMasterOutputDf):
  loggerAtt.info("Fetching tax holiday details")
  
  taxTable = spark.sql("""SELECT * from delta.`{}` where '{}' between DB_HOLIDAY_BEGIN_DATE and DB_HOLIDAY_END_DATE;""".format(taxHolidayDeltaPath, currentDate))
  ABC(currTaxTblCount=taxTable.count())
  
  if taxTable.count() > 0:
    temp_table_name = "itemMasterOutputDf"
    itemMasterOutputDf.createOrReplaceTempView(temp_table_name)

    itemMasterWithTaxHoliday = spark.sql("""SELECT itemMasterOutputDf.*, 
                                                DB_UPC, 
                                                DB_STORE, 
                                                DB_HOLIDAY_TAX_1,
                                                DB_HOLIDAY_TAX_2,
                                                DB_HOLIDAY_TAX_3,
                                                DB_HOLIDAY_TAX_4,
                                                DB_HOLIDAY_TAX_5,
                                                DB_HOLIDAY_TAX_6,
                                                DB_HOLIDAY_TAX_7,
                                                DB_HOLIDAY_TAX_8 
                                          from itemMasterOutputDf
                                          left join delta.`{}` as taxHoliday
                                          on itemMasterOutputDf.SMA_STORE = taxHoliday.DB_STORE
                                           and  itemMasterOutputDf.SMA_GTIN_NUM = taxHoliday.DB_UPC
                                          where '{}' between DB_HOLIDAY_BEGIN_DATE and DB_HOLIDAY_END_DATE;""".format(taxHolidayDeltaPath, currentDate))

    itemMasterWtTaxHolidayNull = itemMasterWithTaxHoliday.filter((col(DB_UPC).isNull()))
    itemMasterWtTaxHolidayNullCount = itemMasterWtTaxHolidayNull.count()
    loggerAtt.info(f"No of Item Master records with No Tax Holiday Value: {itemMasterWtTaxHolidayNullCount}")
    ABC(itemMasterWithTaxHolidayNullCount=itemMasterWtTaxHolidayNullCount)

    itemMasterWtTaxHolidayNotNull = itemMasterWithTaxHoliday.filter((col(DB_UPC).isNotNull()))
    itemMasterWtTaxHolidayNotNullCount = itemMasterWtTaxHolidayNotNull.count()
    loggerAtt.info(f"No of Item Master records with Tax Holiday Value: {itemMasterWtTaxHolidayNotNullCount}")
    ABC(itemMasterWtTaxHolidayNotNullCount=itemMasterWtTaxHolidayNotNullCount)

    itemMasterWtTaxHolidayNotNull = itemMasterWtTaxHolidayNotNull.withColumn('SMA_TAX_1', col('DB_HOLIDAY_TAX_1'))
    itemMasterWtTaxHolidayNotNull = itemMasterWtTaxHolidayNotNull.withColumn('SMA_TAX_2', col('DB_HOLIDAY_TAX_2'))
    itemMasterWtTaxHolidayNotNull = itemMasterWtTaxHolidayNotNull.withColumn('SMA_TAX_3', col('DB_HOLIDAY_TAX_3'))
    itemMasterWtTaxHolidayNotNull = itemMasterWtTaxHolidayNotNull.withColumn('SMA_TAX_4', col('DB_HOLIDAY_TAX_4'))
    itemMasterWtTaxHolidayNotNull = itemMasterWtTaxHolidayNotNull.withColumn('SMA_TAX_5', col('DB_HOLIDAY_TAX_5'))
    itemMasterWtTaxHolidayNotNull = itemMasterWtTaxHolidayNotNull.withColumn('SMA_TAX_6', col('DB_HOLIDAY_TAX_6'))
    itemMasterWtTaxHolidayNotNull = itemMasterWtTaxHolidayNotNull.withColumn('SMA_TAX_7', col('DB_HOLIDAY_TAX_7'))
    itemMasterWtTaxHolidayNotNull = itemMasterWtTaxHolidayNotNull.withColumn('SMA_TAX_8', col('DB_HOLIDAY_TAX_8'))

    itemMasterWtTaxHolidayList = [itemMasterWtTaxHolidayNotNull, itemMasterWtTaxHolidayNull]
    itemMasterWithTaxHoliday = reduce(DataFrame.unionAll, itemMasterWtTaxHolidayList) 

    itemMasterWithTaxHoliday = itemMasterWithTaxHoliday.drop("DB_HOLIDAY_TAX_1", "DB_HOLIDAY_TAX_2", "DB_HOLIDAY_TAX_3", "DB_HOLIDAY_TAX_4", "DB_HOLIDAY_TAX_5", "DB_HOLIDAY_TAX_6", "DB_HOLIDAY_TAX_7", "DB_HOLIDAY_TAX_8", "DB_UPC", "DB_STORE")
    return itemMasterWithTaxHoliday
  else:
    return itemMasterOutputDf

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write Ouput File

# COMMAND ----------

def itemMasterWrite(itemMasterDeltaPath,itemMasterOutboundPath):
  try:
    ABC(itemWriteCheck=1)
    itemMasterOutputDf = spark.read.format('delta').load(itemMasterDeltaPath)
    if processing_file == 'COD':
      unified_df = itemMasterOutputDf.filter((col('LAST_UPDATE_ID') == PipelineID))
      storeList = list(set(unified_df.select('SMA_DEST_STORE').toPandas()['SMA_DEST_STORE']))
      itemMasterOutputDf = itemMasterOutputDf.filter((col("SMA_DEST_STORE").isin(storeList))) 
    if itemMasterOutputDf.count() >0:
      ABC(itemMasterOutputFileCount=itemMasterOutputDf.count())
    else:
      loggerAtt.info('======== No Item Master Records Output Done ========')
  except Exception as ex:
    ABC(itemOutputFileCount='')
    ABC(itemWriteCheck=0)
    err = ErrorReturn('Error', ex,'itemMasterWrite data fetching')
    loggerAtt.error(ex)
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
  if itemMasterOutputDf != None:
    if itemMasterOutputDf.count() >0:
      try:
        itemMasterOutputDf = itemMasterOutputTransformation(itemMasterOutputDf)
      except Exception as ex:
        ABC(itemOutputFileCount='')
        ABC(itemWriteCheck=0)
        err = ErrorReturn('Error', ex,'itemMaster Write Transformation')
        loggerAtt.error(ex)
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
      try:
        itemMasterOutputDf = taxHolidayUpdate(itemMasterOutputDf)
      except Exception as ex:
        ABC(itemOutputFileCount='')
        ABC(itemWriteCheck=0)
        err = ErrorReturn('Error', ex,'itemMaster Write Tax Holiday Changes')
        loggerAtt.error(ex)
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
      try:
        itemMasterOutputDf.write.partitionBy('CHAIN_ID', 'SMA_DEST_STORE').mode('overwrite').format('parquet').save(itemMasterOutboundPath + "/" +"ItemMaster_Output")
        loggerAtt.info('========Item Master Records Output successful ========')
      except Exception as ex:
        ABC(itemOutputFileCount='')
        ABC(itemWriteCheck=0)
        err = ErrorReturn('Error', ex,'itemMaster Write Output')
        loggerAtt.error(ex)
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Main Function Processing

# COMMAND ----------

if __name__ == "__main__":
  ## File reading parameters
  loggerAtt.info('======== Input Item Master file processing initiated ========')
  
  if processing_file == 'Recall':
    currItemMain()
    
    itemMasterWrite(itemMasterDeltaPath,itemMasterOutboundPath)
  else:
    if processing_file == 'Delta':
      archivalItemWithStatusX()
    
    currItemMain()

    btlAmtUpdate()

    priceChgRec = currPriceChange()

    priceChgWithPromo = priceChangeNotInPromo(priceChgRec)

    if processing_file == 'COD':
      currPromo = currPromoRecIOD()
    else:
      currPromo = currPromoRecDaily()

    currPromoDaily = checkMissingPromoRec(currPromo, priceChgWithPromo)

    currPromoPrice = salePriceCalFunc(currPromoDaily)
    
    if processing_file !='FullItem':
      fetchCouponChange()
    
    itemMasterWrite(itemMasterDeltaPath,itemMasterOutboundPath)
    
    
    if processing_file == 'Delta':
      archivePromotionTable()

      archivePriceTable()

      archiveTaxTable()

      archivalItemWithStatusD()


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

updateDeltaVersioning('Ahold', 'itemMaster', PipelineID, Filepath, FileName)

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