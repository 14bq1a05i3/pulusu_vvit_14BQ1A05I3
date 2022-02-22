# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Libraries

# COMMAND ----------

from pyspark.sql.types import * 
import quinn
from pyspark.sql import *
import json
from pyspark.sql.functions  import *
from pytz import timezone
import datetime
import quinn
import logging 
from functools import reduce
from delta.tables import *
from pyspark.sql.functions import regexp_extract
import jsonpickle
from json import JSONEncoder

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

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

custom_logfile_Name ='itemMasterCustomlog'
loggerAtt, p_logfile, file_date = logger(custom_logfile_Name, '/tmp/')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Setting Dynamic partition load

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

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
dbutils.widgets.text("directory","")
dbutils.widgets.text("outputDirectory","")
dbutils.widgets.text("Container","")
dbutils.widgets.text("filePath","") #POSdaily/Inbound/RDS/2021/03/31
dbutils.widgets.text("pipelineID","")
dbutils.widgets.text("MountPoint","")
dbutils.widgets.text("deltaPath","")
dbutils.widgets.text("logFilesPath","")
dbutils.widgets.text("invalidItemMasterPath","")
dbutils.widgets.text("clientId","")
dbutils.widgets.text("keyVaultName","")
dbutils.widgets.text("archivalFilePath","")
dbutils.widgets.text("itemTempEffDeltaPath","")
dbutils.widgets.text("itemMainDeltaPath","")
dbutils.widgets.text("productTempEffDeltaPath","")
dbutils.widgets.text("priceTempEffDeltaPath","")
dbutils.widgets.text("inventoryTempEffDeltaPath","")
dbutils.widgets.text("productToStoreTempEffDeltaPath","")
dbutils.widgets.text("storeTempEffDeltaPath","")
dbutils.widgets.text("itemMasterOutboundPath","")
dbutils.widgets.text("promoLinkingDeltaPath","")
dbutils.widgets.text("couponDeltaPath","")



inputDirectory=dbutils.widgets.get("directory")
outputDirectory=dbutils.widgets.get("outputDirectory")
container=dbutils.widgets.get("Container")
fileName=dbutils.widgets.get("filePath")

#Flag to Differentiate Intra Day and Daily Run
intraDayFlag = 'N'
if (fileName.find('POSemergency') != -1):
  intraDayFlag = 'Y'

pipelineid=dbutils.widgets.get("pipelineID")
mount_point=dbutils.widgets.get("MountPoint")
itemMasterDeltaPath=dbutils.widgets.get("deltaPath")
logFilesPath=dbutils.widgets.get("logFilesPath")
invalidItemMasterPath=dbutils.widgets.get("invalidItemMasterPath")
Date = datetime.datetime.now(timezone("America/Halifax")).strftime("%Y-%m-%d")
mount_point_output = '/mnt/'+outputDirectory
inputSource= 'abfss://' + inputDirectory + '@' + container + '.dfs.core.windows.net/'
outputSource= 'abfss://' + outputDirectory + '@' + container + '.dfs.core.windows.net/'
clientId=dbutils.widgets.get("clientId")
keyVaultName=dbutils.widgets.get("keyVaultName")
archivalfilelocation=dbutils.widgets.get("archivalFilePath")
itemTempEffDeltaPath=dbutils.widgets.get("itemTempEffDeltaPath")
productTempEffDeltaPath=dbutils.widgets.get("productTempEffDeltaPath")
priceTempEffDeltaPath=dbutils.widgets.get("priceTempEffDeltaPath")
inventoryTempEffDeltaPath=dbutils.widgets.get("inventoryTempEffDeltaPath")
productToStoreTempEffDeltaPath=dbutils.widgets.get("productToStoreTempEffDeltaPath")
storeTempEffDeltaPath=dbutils.widgets.get("storeTempEffDeltaPath")
itemMasterOutboundPath=dbutils.widgets.get("itemMasterOutboundPath")
promoLinkingDeltaPath=dbutils.widgets.get("promoLinkingDeltaPath")
itemMainDeltaPath=dbutils.widgets.get("itemMainDeltaPath")
couponDeltaPath=dbutils.widgets.get("couponDeltaPath")

itemMainArchivalpath = archivalfilelocation + "/" +Date+ "/" + "itemMainData"
itemMasterArchivalpath = archivalfilelocation + "/" +Date+ "/" + "itemMasterData"
promotionLinkingArchivalpath = archivalfilelocation + "/" +Date+ "/" + "promoLinkingData"
exceptionItemMainTempArchivalpath = archivalfilelocation + "/" +Date+ "/" + "exceptionItemMainTempData"

loggerAtt.info(f"Date : {Date}")
loggerAtt.info(f"Source or File Location on Container : {inputSource}")
loggerAtt.info(f"Source or File Location on Container : {outputSource}")


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
  mounting(mount_point_output, outputSource, clientId, keyVaultName)
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

# MAGIC %md
# MAGIC 
# MAGIC ## Delta table creation

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Item Master

# COMMAND ----------

try:
  ABC(DeltaTableCreateCheck=1)
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
                DSS_DC_ITEM_NUMBER STRING,
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
except Exception as ex:
  ABC(DeltaTableCreateCheck = 0)
  loggerAtt.error(ex)
  err = ErrorReturn('Error', ex,'deltaCreator itemMasterDeltaPath')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Item Main Table creation

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
              PARTITIONED BY (RTX_STORE, SCRTX_HDR_ACT_DATE)""".format(itemMainDeltaPath))
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
# MAGIC ### Item Temp Effective table

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
# MAGIC ### Price Temp Effective table

# COMMAND ----------

try:
  spark.sql("""
  CREATE TABLE IF NOT EXISTS DApriceTemp (
  PRICE_BANNER_ID STRING,
  PRICE_STORE_ID STRING,
  PRICE_UPC LONG,
  PRICE_UNIT_PRICE FLOAT,
  PRICE_UOM_CD STRING,
  PRICE_MLT_Quantity STRING,
  PRICE_PROMO_RTL_PRC FLOAT,
  PRICE_PROMO_UNIT_PRICE FLOAT,
  PRICE_Promotional_Quantity STRING,
  PRICE_START_DATE STRING,
  PRICE_END_DATE STRING,
  TPRX001_STORE_SID_NBR STRING,
  TPRX001_ITEM_NBR STRING,
  SMA_LEGACY_ITEM STRING,
  SMA_SOURCE_WHSE STRING,
  TPRX001_ITEM_SRC_CD STRING,
  TPRX001_CPN_SRC_CD STRING,
  TPRX001_RTL_PRC_EFF_DT STRING,
  SMA_SMR_EFF_DATE STRING,
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
  TPRX001_TAG_PRV_WK_RTL_PRC DECIMAL(7,2)
  )
  USING delta
  Location '{}'
  PARTITIONED BY (PRICE_STORE_ID)
  """.format(priceTempEffDeltaPath))
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
# MAGIC ### Inventory Temp Effective table

# COMMAND ----------

try:
  ABC(DeltaTableCreateCheck=1)
  spark.sql("""
  CREATE TABLE IF NOT EXISTS InventoryTemp (
  INVENTORY_CODE String,
  INVENTORY_QTY_ON_HAND String,
  INVENTORY_SUPER_CATEGORY String,
  INVENTORY_MAJOR_CATEGORY String,
  INVENTORY_INTMD_CATEGORY String,
  INVENTORY_STORE_ID STRING,
  INVENTORY_UPC LONG,
  INVENTORY_BANNER_ID String
  )
  USING delta
  Location '{}'
  PARTITIONED BY (INVENTORY_BANNER_ID)
  """.format(inventoryTempEffDeltaPath))
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
# MAGIC ### Product to Store Temp Effective table

# COMMAND ----------

try:
    ABC(DeltaTableCreateCheck = 1)
    loggerAtt.info("prodtoStoreTemp creation")
    spark.sql(""" CREATE TABLE IF NOT EXISTS prodToStoreTemp(
                PRD2STORE_BANNER_ID STRING,
                PRD2STORE_UPC Long,
                PRD2STORE_STORE_ID STRING,
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
                PRD2STORE_SWAP_SAVE_UPC STRING)
              USING delta 
              PARTITIONED BY (PRD2STORE_STORE_ID)
              LOCATION '{}' """.format(productToStoreTempEffDeltaPath))
except Exception as ex:
    ABC(DeltaTableCreateCheck = 0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'prodtoStoreEffDeltaPath deltaCreator')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Product Temp Effective table

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
                SMA_FSA_IND STRING,
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
              LOCATION '{}' """.format(productTempEffDeltaPath))
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
# MAGIC ### Store Temp Effective table

# COMMAND ----------

try:
  ABC(DeltaTableCreateCheck = 1)
  spark.sql("""
  CREATE TABLE IF NOT EXISTS StoreTemp (
  SMA_STORE_STATE String,
  STORE_BANNER_ID String,
  STORE_STORE_NUMBER String
  )
  USING delta
  Location '{}'
  PARTITIONED BY (STORE_STORE_NUMBER)
  """.format(storeTempEffDeltaPath))
except Exception as ex:
  ABC(DeltaTableCreateCheck = 0)
  loggerAtt.error(ex)
  err = ErrorReturn('Error', ex,'storeTempEffDeltaPath deltaCreator')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))

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
# MAGIC ## Declaration

# COMMAND ----------

unified_df = None
unifiedPromoLinkDf = None
currentTimeStamp = datetime.datetime.now().timestamp()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## User Defined Functions

# COMMAND ----------

def formatZeros(s):
  if s is not None:
    return format(s, '.2f')
  else:
    return s

formatZerosUDF = udf(formatZeros) 

def salePriceMVP(SCRTX_DET_FREQ_SHOP_TYPE, SCRTX_DET_FREQ_SHOP_VAL, SMA_RETL_MULT_UNIT, SMA_MULT_UNIT_RETL, SCRTX_HDR_DESC):
  
  if ((SCRTX_DET_FREQ_SHOP_TYPE is not None) and (SMA_MULT_UNIT_RETL is not None) and (SMA_RETL_MULT_UNIT is not None) and (SCRTX_DET_FREQ_SHOP_VAL is not None)):
    if SCRTX_DET_FREQ_SHOP_TYPE == 1:
      return SMA_MULT_UNIT_RETL - (int(SMA_RETL_MULT_UNIT) * SCRTX_DET_FREQ_SHOP_VAL)
    else:
      return None
#       if SCRTX_HDR_DESC is not None:
#         if 'AD ON' in SCRTX_HDR_DESC:
#           return SMA_MULT_UNIT_RETL
#         elif 'AD OFF' in SCRTX_HDR_DESC:
#           return 0
#         else:
#           if SCRTX_DET_FREQ_SHOP_TYPE == 0:
#             return 0
#           else:
#             return None
#       else:
#         if SCRTX_DET_FREQ_SHOP_TYPE == 0:
#           return 0
#         else:
#           return None
  else:
    return None

salePriceMVPUDF = udf(salePriceMVP)  

def saleQuantityMVP(SMA_RETL_MULT_UNIT, SCRTX_DET_FREQ_SHOP_TYPE, SCRTX_HDR_DESC):
  if ((SCRTX_DET_FREQ_SHOP_TYPE is not None) and (SMA_RETL_MULT_UNIT is not None)):
    if SCRTX_DET_FREQ_SHOP_TYPE == 1:
      return SMA_RETL_MULT_UNIT
    else:
      return None
#       if SCRTX_HDR_DESC is not None:
#         if 'AD ON' in SCRTX_HDR_DESC:
#           return SMA_RETL_MULT_UNIT
#         elif 'AD OFF' in SCRTX_HDR_DESC:
#           return '0'
#         else:
#           if SCRTX_DET_FREQ_SHOP_TYPE == 0:
#             return '0'
#           else:
#             return None
#       else:
#         if SCRTX_DET_FREQ_SHOP_TYPE == 0:
#           return '0'
#         else:
#           return None
  else:
    return None

saleQuantityMVPUDF = udf(saleQuantityMVP)  


def retlMultUnit(SCRTX_DET_FREQ_SHOP_TYPE, SMA_RETL_MULT_UNIT, PROMOTIONAL_QUANTITY, SCRTX_HDR_DESC):
  if PROMOTIONAL_QUANTITY is not None and SCRTX_HDR_DESC is not None:
    if SCRTX_DET_FREQ_SHOP_TYPE == 1:
      return PROMOTIONAL_QUANTITY
    elif 'AD ON' in SCRTX_HDR_DESC:
      return PROMOTIONAL_QUANTITY
    else:
      return SMA_RETL_MULT_UNIT
  else:
    return SMA_RETL_MULT_UNIT

retlMultUnitUDF = udf(retlMultUnit) 


def multUnitRetl(SCRTX_DET_FREQ_SHOP_TYPE, SMA_MULT_UNIT_RETL, PROMO_RTL_PRC, SCRTX_HDR_DESC):
  if PROMO_RTL_PRC is not None and SCRTX_HDR_DESC is not None:
    if SCRTX_DET_FREQ_SHOP_TYPE == 1:
      return PROMO_RTL_PRC
    elif 'AD ON' in SCRTX_HDR_DESC:
      return PROMO_RTL_PRC
    else:
      return SMA_MULT_UNIT_RETL
  else:
    return SMA_MULT_UNIT_RETL

multUnitRetlUDF = udf(multUnitRetl) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Processing functions

# COMMAND ----------

def addNewNullColumn(itemMaster):
  itemMaster=itemMaster.withColumn("SMA_ACTIVATION_CODE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_CHG_TYPE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_COMP_ID", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_COMP_OBSERV_DATE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_COMP_PRICE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_CONTRIB_QTY", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_CONTRIB_QTY_UOM", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_CPN_MLTPY_IND", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_DATA_TYPE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_DEMANDTEC_PROMO_ID", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_DISCOUNTABLE_IND", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_EFF_DOW", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_EMERGY_UPDATE_IND", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_FEE_ITEM_TYPE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_FEE_TYPE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_FUEL_PROD_CATEG", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_GAS_IND", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_HIP_IND", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_GLUTEN_FREE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_IFPS_CODE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_IN_STORE_TAG_PRINT", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_ITEM_BRAND", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_ITEM_MVMT", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_ITEM_POG_COMM", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_KOSHER_FLAG", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_LINK_APPLY_TIME", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_LINK_CHG_TYPE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_LINK_FAMCD_PROMOCD", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_LINK_HDR_LOCATION", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_LINK_HDR_MAINT_TYPE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_LINK_ITEM_NBR", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_LINK_OOPS_ADWK", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_LINK_OOPS_FILE_ID", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_LINK_RCRD_TYPE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_LINK_TYPE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_MANUAL_PRICE_ENTRY", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_MEAS_OF_EACH", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_MEAS_OF_EACH_WIP", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_MEAS_OF_PRICE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_MKT_AREA", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_NEW_ITMUPC", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_NON_PRICED_ITEM", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_ORIG_CHG_TYPE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_ORIG_LIN", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_ORIG_VENDOR", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_POINTS_ELIGIBLE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_POS_REQUIRED", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_POS_SYSTEM", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_PRICE_CHG_ID", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_PRIME_UPC", when((col("ALTERNATE_UPC").isNull()), lit("Y")).otherwise(lit("N")).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_QTY_KEY_OPTIONS", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_RECALL_FLAG", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_REFUND_RECEIPT_EXCLUSION", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_RETL_CHG_TYPE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_RETL_PRC_ENTRY_CHG", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_SEGMENT", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_SHELF_TAG_REQ", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_SOURCE_METHOD", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_STORE_DIV", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_STORE_ZONE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_TAG_REQUIRED", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_TAG_SZ", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_TARE_PCT", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_TARE_UOM", lit(None).cast(IntegerType()))
  itemMaster=itemMaster.withColumn("SMA_TAX_CATEG", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_TAX_CHG_IND", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_TENDER_RESTRICTION_CODE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_TIBCO_DATE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_TRANSMIT_DATE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_UNIT_PRICE_CODE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_UOM_OF_PRICE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_UPC_OVERRIDE_GRP_NUM", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_WIC_ALT", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_WIC_SHLF_MIN", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_WPG", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("HOW_TO_SELL", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_RECV_TYPE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_PL_UPC_REDEF", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("EFF_TS", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SALE_PRICE",lit(None).cast(StringType()))
#   itemMaster=itemMaster.withColumn("SALE_PRICE", lpad(regexp_replace(format_number(col("SALE_PRICE"), 2), ",", ""),10,'0'))
  itemMaster=itemMaster.withColumn("SALE_QUANTITY", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_LINK_APPLY_DATE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("PERF_DETL_SUB_TYPE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_LINK_START_DATE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_LINK_END_DATE", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_HLTHY_IDEAS", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_LINK_SYS_DIGIT", lit(None).cast(StringType()))
  itemMaster=itemMaster.withColumn("SCRTX_DET_FREQ_SHOP_VAL", lpad(formatZerosUDF(round(col('SCRTX_DET_FREQ_SHOP_VAL'),2)),10,'0').cast(FloatType()))
  
  ### Commented for Intraday Purpose
  itemMaster=itemMaster.withColumn("SALE_PRICE", salePriceMVPUDF(col('SCRTX_DET_FREQ_SHOP_TYPE'), col('SCRTX_DET_FREQ_SHOP_VAL'), col('SMA_RETL_MULT_UNIT'), col('SMA_MULT_UNIT_RETL'), col('SCRTX_HDR_DESC')))
  ## Intraday Changes
  itemMaster=itemMaster.withColumn("SALE_PRICE", when((col("SALE_PRICE").isNull()) | (col("SALE_PRICE") == 0), lit(None)).otherwise(lpad(formatZerosUDF(round(col("SALE_PRICE"), 2)),10,'0')).cast(StringType()))
  
  itemMaster=itemMaster.withColumn("SALE_QUANTITY", saleQuantityMVPUDF(col('SMA_RETL_MULT_UNIT'), col('SCRTX_DET_FREQ_SHOP_TYPE'), col('SCRTX_HDR_DESC')))
  ## Intraday changes
  itemMaster=itemMaster.withColumn("SALE_QUANTITY", when((col("SALE_PRICE").isNull()) | (col("SCRTX_DET_FREQ_SHOP_TYPE") == 0) | (col("SALE_QUANTITY").isNull()) | (col("SALE_QUANTITY") == 0), lit(None)).otherwise(col("SALE_QUANTITY")).cast(StringType()))
  
  itemMaster=itemMaster.withColumn("SMA_RETL_MULT_UNIT", retlMultUnitUDF(col('SCRTX_DET_FREQ_SHOP_TYPE'), col('SMA_RETL_MULT_UNIT'), col('PROMOTIONAL_QUANTITY'), col('SCRTX_HDR_DESC')))
  itemMaster=itemMaster.withColumn("SMA_MULT_UNIT_RETL", multUnitRetlUDF(col('SCRTX_DET_FREQ_SHOP_TYPE'),  col('SMA_MULT_UNIT_RETL'), col('PROMO_RTL_PRC'), col('SCRTX_HDR_DESC')))
  
  ## Intraday Changes
#   itemMaster=itemMaster.withColumn("SALE_PRICE", salePriceMVPUDF(col('SCRTX_DET_FREQ_SHOP_TYPE'), col('SCRTX_DET_FREQ_SHOP_VAL'), col('SMA_RETL_MULT_UNIT'), col('SMA_MULT_UNIT_RETL'), col('SCRTX_HDR_DESC')))
#   ## Intraday Changes
#   itemMaster=itemMaster.withColumn("SALE_PRICE", when((col("SALE_PRICE").isNull()) | (col("SALE_PRICE") == 0), lit(None)).otherwise(lpad(formatZerosUDF(round(col("SALE_PRICE"), 2)),10,'0')).cast(StringType()))
  
#   itemMaster=itemMaster.withColumn("SALE_QUANTITY", saleQuantityMVPUDF(col('SMA_RETL_MULT_UNIT'), col('SCRTX_DET_FREQ_SHOP_TYPE'), col('SCRTX_HDR_DESC')))
#   ## Intraday changes
#   itemMaster=itemMaster.withColumn("SALE_QUANTITY", when((col("SALE_PRICE").isNull()) | (col("SCRTX_DET_FREQ_SHOP_TYPE") == 0) | (col("SALE_QUANTITY").isNull()) | (col("SALE_QUANTITY") == 0), lit(None)).otherwise(col("SALE_QUANTITY")).cast(StringType()))
  
    
  itemMaster=itemMaster.withColumn("SMA_LINK_UPC", lit(None).cast(StringType()))
  # Need to chk source and corresponding mapping
#   itemMaster=itemMaster.withColumn("SMA_LINK_PRODUCT", lit(None).cast(StringType()))
#   itemMaster=itemMaster.withColumn("AVG_WGT", lit(None).cast(StringType()))

  return itemMaster

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### UDF

# COMMAND ----------

def salePriceAmt(PERF_DETL_SUB_TYPE, MULT_UNIT_RETL, CHANGE_AMOUNT_PCT, COUPON_NO, PROMO_STATUS):
  if COUPON_NO is not None:
    if PROMO_STATUS != 'C' and PROMO_STATUS != 'M':
      return None
    elif PERF_DETL_SUB_TYPE == 4:
      salePrice = MULT_UNIT_RETL - (MULT_UNIT_RETL*CHANGE_AMOUNT_PCT)
      return salePrice
    else:
      return None
  else:
    return None
  
salePriceAmtUDF = udf(salePriceAmt)  

def saleQuantity(ENHANCED_THRESHOLD_QTY, PERF_DETL_SUB_TYPE, COUPON_NO, PROMO_STATUS):
  if COUPON_NO is not None:
    if PROMO_STATUS != 'C' and PROMO_STATUS != 'M':
      return None
    elif PERF_DETL_SUB_TYPE == 4:
      return ENHANCED_THRESHOLD_QTY
    elif PERF_DETL_SUB_TYPE == 9:
      return 1
    else:
      return None
  else:
    return None

saleQuantityUDF = udf(saleQuantity)  


def formatZeros(s):
  if s is not None:
    return format(s, '.2f')
  else:
    return s

formatZerosUDF = udf(formatZeros) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Transformation

# COMMAND ----------

def performPromoLinkTransformation(itemMaster):
  itemMaster=itemMaster.withColumn("SMA_LINK_APPLY_DATE",date_format(col("START_DATE"), 'yyyy/MM/dd').cast(StringType()))
  itemMaster=itemMaster.withColumn("PROMO_ENHANCED_THRESHOLD_QTY", col('PROMO_ENHANCED_THRESHOLD_QTY').cast(IntegerType()))
  itemMaster=itemMaster.withColumn("PROMO_ENHANCED_THRESHOLD_QTY", when(col("PROMO_ENHANCED_THRESHOLD_QTY").isNull(), lit(0)).otherwise(col("PROMO_ENHANCED_THRESHOLD_QTY")))
  itemMaster=itemMaster.withColumn("SMA_LINK_END_DATE", date_format(col("END_DATE"), 'yyyy/MM/dd').cast(StringType()))
  itemMaster=itemMaster.withColumn("SMA_LINK_START_DATE", col('SMA_LINK_APPLY_DATE'))
#   itemMaster=itemMaster.withColumn("SMA_MULT_UNIT_RETL", col('SMA_MULT_UNIT_RETL').cast(DecimalType()))
  itemMaster=itemMaster.filter((col('SMA_MULT_UNIT_RETL').isNotNull()))
  itemMaster=itemMaster.withColumn("CHANGE_AMOUNT_PCT", (col('CHANGE_AMOUNT_PCT').cast(FloatType()))/100)
  itemMaster=itemMaster.withColumn("SALE_PRICE", salePriceAmtUDF(col('PERF_DETL_SUB_TYPE'), col('SMA_MULT_UNIT_RETL'), col('CHANGE_AMOUNT_PCT'), col('COUPON_NO'), col('PROMO_STATUS')).cast(FloatType()))
  itemMaster=itemMaster.withColumn("SALE_PRICE", lpad(formatZerosUDF(round(col("SALE_PRICE"), 2)),10,'0'))
  itemMaster=itemMaster.withColumn("SALE_QUANTITY", saleQuantityUDF(col('PROMO_ENHANCED_THRESHOLD_QTY'), col('PERF_DETL_SUB_TYPE'), col('COUPON_NO'), col('PROMO_STATUS')))
#   itemMaster=itemMaster.withColumn("SALE_PRICE", salePriceMVPUDF(col('SCRTX_DET_FREQ_SHOP_TYPE'), col('SCRTX_DET_FREQ_SHOP_VAL'), col('SMA_RETL_MULT_UNIT'), col('SMA_MULT_UNIT_RETL'), col('SCRTX_HDR_DESC')))
  ## Intraday Changes
  itemMaster=itemMaster.withColumn("SALE_PRICE", when((col("SALE_PRICE").isNull()) | (col("SALE_PRICE") == 0), lit(None)).otherwise(col("SALE_PRICE")).cast(StringType()))
  
#   itemMaster=itemMaster.withColumn("SALE_QUANTITY", saleQuantityMVPUDF(col('SMA_RETL_MULT_UNIT'), col('SCRTX_DET_FREQ_SHOP_TYPE'), col('SCRTX_HDR_DESC')))
  ## Intraday changes
  itemMaster=itemMaster.withColumn("SALE_QUANTITY", when((col("SALE_PRICE").isNull()) | (col("SCRTX_DET_FREQ_SHOP_TYPE") == 0) | (col("SALE_QUANTITY").isNull()) | (col("SALE_QUANTITY") == 0), lit(None)).otherwise(col("SALE_QUANTITY")).cast(StringType()))
  itemMaster=itemMaster.withColumn("PERF_DETL_SUB_TYPE", col('PERF_DETL_SUB_TYPE').cast(StringType()))
  return itemMaster

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SQL Table query function

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/DELHAIZE/unifiedItemSQLQuery

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write Output file based on delta table changes

# COMMAND ----------

def itemMasterWrite(itemMasterDeltaPath,itemMasterOutboundPath):
  itemMasterOutputDf = spark.read.format('delta').load(itemMasterDeltaPath)
  if processing_file == 'COD':
    storeList = list(set(unified_df.select('SMA_DEST_STORE').toPandas()['SMA_DEST_STORE']))
    itemMasterOutputDf = itemMasterOutputDf.filter((col("SMA_DEST_STORE").isin(storeList))) 
  ABC(itemOutputFileCount=itemMasterOutputDf.count())
  if itemMasterOutputDf.count() >0:
    # Add other field that need 0 padding - SMA_MULT_UNIT_RETL, COUPON_NO, 
    ABC(itemMasterOutputFileCount=itemMasterOutputDf.count())
    itemMasterOutputDf=itemMasterOutputDf.withColumn("SMA_MULT_UNIT_RETL", lpad(regexp_replace(format_number(col("SMA_MULT_UNIT_RETL"), 2), ",", ""),10,'0'))
    itemMasterOutputDf = itemMasterOutputDf.withColumn('SMA_LINK_HDR_COUPON', lpad(col('SMA_LINK_HDR_COUPON'),14,'0'))
    itemMasterOutputDf = itemMasterOutputDf.withColumn('SMA_GTIN_NUM', lpad(col('SMA_GTIN_NUM'),14,'0'))
    itemMasterOutputDf = itemMasterOutputDf.withColumn('SMA_BATCH_SERIAL_NBR', col('SMA_BATCH_SERIAL_NBR').cast(LongType()))
#     itemMasterOutputDf = itemMasterOutputDf.drop(col('PRD2STORE_BANNER_ID'))
    itemMasterOutputDf = itemMasterOutputDf.withColumn('CHAIN_ID', lit('DELHAIZE'))
  
  ## Intraday changes
    itemMasterOutputDf = itemMasterOutputDf.withColumn("SALE_PRICE", when((col("SALE_PRICE") == '0000000.00') | (col("SALE_PRICE") == 0), lit(None)).otherwise(col("SALE_PRICE")).cast(StringType()))
    itemMasterOutputDf = itemMasterOutputDf.withColumn("SALE_QUANTITY", when((col("SALE_PRICE").isNull() | col("SALE_QUANTITY") == '0') | (col("SALE_QUANTITY") == 0), lit(None)).otherwise(col("SALE_QUANTITY")).cast(StringType()))
    
    
    itemMasterOutputDf.write.partitionBy('CHAIN_ID', 'SMA_DEST_STORE').mode('overwrite').format('parquet').save(itemMasterOutboundPath + "/" +"ItemMaster_Output")
    loggerAtt.info('========Item Master Records Output successful ========')
  else:
    loggerAtt.info('======== No Item Master Records Output Done ========')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Main file processing

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
if __name__ == "__main__":
  loggerAtt.info('======== Input Product file processing initiated ========')
  ## Step 1: Find initial item master count
  try:
    itemMasterCount = spark.read.format('delta').load(itemMasterDeltaPath).count()
    loggerAtt.info('Initail Item Master Count ' + str(itemMasterCount))
  except Exception as ex:
    err = ErrorReturn('Error', ex,'Item Master Count Check')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
  
  try:
    processing_file='Delta'
    if (fileName.find('POSemergency') ==-1):
      processing_file ='Delta'
    else:
      processing_file='COD'
  except Exception as ex:
    err = ErrorReturn('Error', ex,'processing_file check failed')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
    
  ## Step 3: Fetching Item Main Records
  ABC(FetchDataFromAllFeedDailyCheck=1)
  unified_df = fetchDataFromAllFeedDaily(itemTempEffDeltaPath)
      
  ## Step 3.1: Saving exception item main temp records 
  ABC(findItemsNotInItemCheck=1)
#   findItemsNotInItemTemp(unified_df, itemTempEffDeltaPath, exceptionItemMainTempArchivalpath)
  unified_df = unified_df.drop('BANNER_ID')
  unified_df.createOrReplaceTempView("unified_table")
  
  unified_df2 = spark.sql("""
    (SELECT PRICE_STORE_ID, PRICE_UPC, PRICE_PROMO_RTL_PRC as PROMO_RTL_PRC, PRICE_PROMOTIONAL_QUANTITY as PROMOTIONAL_QUANTITY, LAST_UPDATE_TIMESTAMP
            FROM DAPRiceDelta 
            WHERE (PRICE_PROMO_RTL_PRC !=0 AND PRICE_PROMOTIONAL_QUANTITY !=0) AND ( PRICE_PROMOTIONAL_QUANTITY !=1) )
    UNION 
    (SELECT PRICE_STORE_ID, PRICE_UPC, PRICE_RTL_PRC AS PROMO_RTL_PRC, PRICE_MLT_Quantity as PROMOTIONAL_QUANTITY, LAST_UPDATE_TIMESTAMP
            FROM DAPriceDelta 
            WHERE (PRICE_PROMO_RTL_PRC=0 AND PRICE_PROMOTIONAL_Quantity=0) OR ( PRICE_PROMOTIONAL_Quantity=1))
         """)
  
  windowSpec = Window.partitionBy(["PRICE_STORE_ID", "PRICE_UPC"]).orderBy(desc("LAST_UPDATE_TIMESTAMP"))
  unified_df2 = unified_df2.withColumn("row_number", row_number().over(windowSpec))
  unified_df2 = unified_df2.filter(unified_df2.row_number == 1)
  
  unified_df2.drop("row_number")
  unified_df2.createOrReplaceTempView("unified_df3")
  
  if intraDayFlag == 'N':
    unified_df = spark.sql("""
     select im.*, i.PROMO_RTL_PRC as PROMO_RTL_PRC, i.PROMOTIONAL_QUANTITY AS PROMOTIONAL_QUANTITY 
            from unified_table im JOIN unified_df3  i ON i.PRICE_STORE_ID = im.SMA_DEST_STORE AND im.SMA_GTIN_NUM=i.PRICE_UPC  """)
  elif intraDayFlag == 'Y':
    unified_df = spark.sql(""" select im.*, i.SCRTX_DET_RTL_PRC AS PROMO_RTL_PRC, i.SCRTX_DET_UNT_QTY AS PROMOTIONAL_QUANTITY from unified_table im JOIN item_main i ON i.RTX_STORE = im.SMA_DEST_STORE AND im.SMA_GTIN_NUM=i.RTX_UPC
    """)
  

  ## Step 4: Adding NUll Columns and Default Sale Price Value
  try:  
    ABC(RenamingCheck=1)
    ABC(TransformationCheck=1)
    if unified_df is not None:
      if unified_df.count() > 0:
        unified_df = addNewNullColumn(unified_df)
  except Exception as ex:
    ABC(TransformationCheck=0)
    err = ErrorReturn('Error', ex,'addNewNullColumn')
    loggerAtt.error(ex)
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))    
  
  ##Handling NULLs
  unified_df.createOrReplaceTempView("unified_minus_table")
  unified_df = spark.sql("""(SELECT * FROM unified_minus_table) MINUS (SELECT *  FROM unified_minus_table WHERE PROMO_RTL_PRC is NULL and PROMOTIONAL_QUANTITY is NULL)""")
  
  #Handling Nulls using Window function
  windowSpec = Window.partitionBy(["SMA_DEST_STORE", "SMA_GTIN_NUM"]).orderBy(desc("LAST_UPDATE_TIMESTAMP"))
  unified_df = unified_df.withColumn("row_number", row_number().over(windowSpec))
  unified_df = unified_df.filter(unified_df.row_number == 1)
  
  unified_df = unified_df.drop('PROMO_RTL_PRC')
  unified_df = unified_df.drop('PROMOTIONAL_QUANTITY')
  
  unified_df = unified_df.drop('row_number')
  
  ## Step 5: Update Item Master table
  if unified_df is not None:
    if unified_df.count() > 0:
      upsertItemRecordsDaily(unified_df, itemMasterDeltaPath)
#       itemMasterDeltaPath = itemMasterDeltaPath.withColumn("SALE_PRICE", when((col("SALE_PRICE") == '0000000.00') | (col("SALE_PRICE") == 0), lit(None)).otherwise(col("SALE_PRICE")).cast(StringType()))
#       itemMasterDeltaPath = itemMasterDeltaPath.withColumn("SALE_QUANTITY", when((col("SALE_QUANTITY") == '0') | (col("SALE_QUANTITY") == 0), lit(None)).otherwise(col("SALE_QUANTITY")).cast(StringType()))
    
  
  ## Step 6: Fetching all promo Link changes for current day
  ABC(FetchPromoCheck=1)
  unifiedPromoLinkDf = fetchPromoLinkData(itemMasterDeltaPath, promoLinkingDeltaPath, itemMasterCount)
    
  ## Step 7: Calculate Sale Price and Add coupon information in Unified Item table
  try:  
    ABC(RenamingCheck=1)
    ABC(TransformationCheck=1)
    if unifiedPromoLinkDf is not None:
      if unifiedPromoLinkDf.count() > 0:
        unifiedPromoLinkDf = performPromoLinkTransformation(unifiedPromoLinkDf)
  except Exception as ex:
    ABC(TransformationCheck=0)
    err = ErrorReturn('Error', ex,'performPromoLinkTransformation')
    loggerAtt.error(ex)
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
  ## Step 8: Update Item Master with latest Sale Price Values
  if unifiedPromoLinkDf is not None:
    if unifiedPromoLinkDf.count() > 0:
      updatePromoRecords(unifiedPromoLinkDf, itemMasterDeltaPath)
  
  ## Step 9: Write output file
  try:
    ABC(itemWriteCheck=1)
    itemMasterWrite(itemMasterDeltaPath,itemMasterOutboundPath)
  except Exception as ex:
    ABC(itemOutputFileCount='')
    ABC(itemWriteCheck=0)
    err = ErrorReturn('Error', ex,'itemMasterWrite')
    loggerAtt.error(ex)
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
    
  if processing_file != 'COD':
    ## Step 10: Delete old day effective records in Item Main
    ABC(archivePreviousDayItemMainCheck=1)
    archivePreviousDayItemMain(itemMainDeltaPath, itemMainArchivalpath)

    ## Step 11: Delete Promotion Linking record with status D
    ABC(archivePromoLinkCheck=1)
    archivePromoLink(promotionLinkingArchivalpath, promoLinkingDeltaPath)

    ## Step 12: Delete Item Master record with status D
    ABC(archivePreviousDayItemMasterCheck=1)
    archivePreviousDayItemMaster(itemMasterDeltaPath, itemMasterArchivalpath)
  
loggerAtt.info('======== Input product file processing ended ========')

# COMMAND ----------

# windowSpec = Window.partitionBy(["SMA_DEST_STORE", "SMA_GTIN_NUM"]).orderBy(desc("LAST_UPDATE_TIMESTAMP"))
# unified_df = unified_df.withColumn("row_number", row_number().over(windowSpec))
# unified_df = unified_df.filter(unified_df.row_number == 1)
  

# COMMAND ----------

# MAGIC %sql SELECT * FROM UNIFIED_TABLE WHERE SMA_DEST_STORE in ('1167') and SMA_GTIN_NUM in ('44700030390','44700030500','44700030530','44700030540','44700030700','44700030950','44700030990','44700032500')

# COMMAND ----------

# MAGIC %sql SELECT SMA_DEST_STORE, SMA_GTIN_NUM, COUNT(*) FROM default.itemmaster GROUP BY SMA_DEST_STORE, SMA_GTIN_NUM HAVING COUNT(*) > 1

# COMMAND ----------

# unified_df.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select SMA_GTIN_NUM, SMA_RETL_MULT_UNIT, SMA_MULT_UNIT_RETL from default.itemmaster where SMA_DEST_STORE in ('1167')  and SMA_GTIN_NUM in ('44700030390','44700030500','44700030530','44700030540','44700030700','44700030950','44700030990','44700032500', '852298002130')

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

# Eg:updateDeltaVersioning('Delhaize', 'pos', 'arunTest123', 'abc/abc/', 'abc/2021/08/19/')

updateDeltaVersioning('Delhaize', 'itemMaster', pipelineid, fileName, fileName)

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

# %sql (SELECT * FROM unified_table) MINUS (SELECT *  FROM unified_table WHERE SCRTX_DET_RTL_PRC is NULL and SCRTX_DET_UNT_QTY is NULL)

# COMMAND ----------

# %sql SELECT * FROM unified_temp

# COMMAND ----------

