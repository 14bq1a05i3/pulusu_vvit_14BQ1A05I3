# Databricks notebook source
# MAGIC %md 
# MAGIC ## LOADING LIBRARIES

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

# MAGIC %sql
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = False;
# MAGIC SET TIME ZONE 'America/Halifax';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calling Logger

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/Logging

# COMMAND ----------

custom_logfile_Name ='storeCompare_daily_customlog'
loggerAtt, p_logfile, file_date = logger(custom_logfile_Name, '/tmp/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Error Class Definition

# COMMAND ----------

class ErrorReturn:
  def __init__(self, status, errorMessage, functionName):
    self.status = status
    self.errorMessage = str(errorMessage)
    self.functionName = functionName
    self.time = datetime.now(timezone("America/Halifax")).isoformat()
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
# MAGIC ## Widgets for getting dynamic paramters from ADF 

# COMMAND ----------

loggerAtt.info("========Widgets call initiated==========")
#dbutils.widgets.removeAll()
dbutils.widgets.text("filePath","")
dbutils.widgets.text("directory","")
dbutils.widgets.text("fileName","")
dbutils.widgets.text("deltaPath","")
dbutils.widgets.text("logFilesPath","")
dbutils.widgets.text("invalidRecordsPath","")
dbutils.widgets.text("MountPoint","")
dbutils.widgets.text("Container","")
dbutils.widgets.text("clientId", "")
dbutils.widgets.text("keyVaultName", "")
dbutils.widgets.text("pipelineID", "")
dbutils.widgets.text("bottleDepositDeltaPath","")
dbutils.widgets.text("storeDeltaPath","")
dbutils.widgets.text("archivalFilePath","")


FileName=dbutils.widgets.get("fileName")
Filepath=dbutils.widgets.get("filePath")
Directory=dbutils.widgets.get("directory")
container=dbutils.widgets.get("Container")
clientId = dbutils.widgets.get("clientId")
keyVaultName = dbutils.widgets.get("keyVaultName")
DeltaPath = dbutils.widgets.get("deltaPath")
BottleDepositpath = dbutils.widgets.get("bottleDepositDeltaPath")
storeDeltaPath = dbutils.widgets.get("storeDeltaPath")
PipelineID=dbutils.widgets.get("pipelineID")
MountPoint=dbutils.widgets.get("MountPoint")
Log_FilesPath=dbutils.widgets.get("logFilesPath")
Invalid_RecordsPath=dbutils.widgets.get("invalidRecordsPath")
Archivalfilelocation=dbutils.widgets.get("archivalFilePath")
storeCompareOutput='/mnt/' + Directory + '/POSstorecompare/Outbound/SDM/StoreCompareOutput'
itemMasterDeltaPath = '/mnt/centralized-price-promo/Delhaize/Outbound/SDM/Item_Master_delta'

Date = datetime.datetime.now(timezone("America/Halifax")).strftime("%Y-%m-%d")
file_location = '/mnt' + '/' + Directory + '/' + Filepath +'/' + FileName 
source= 'abfss://' + Directory + '@' + container + '.dfs.core.windows.net/'

# New change partition addition -- 2022-01-25
from dateutil import tz
import dateutil.relativedelta as REL
from datetime import datetime, timedelta

# wed_day = datetime.now()
file_l = Filepath.split('/')[3:-1]
file_s = file_l[0]+'-'+file_l[1]+'-'+file_l[2]
wed_day = datetime.strptime(file_s, '%Y-%m-%d')

rd = REL.relativedelta(days=0, weekday=REL.WE)
next_wednesday = wed_day + rd
next_wednesday = next_wednesday.strftime("%Y-%m-%d")

storeCompareOutput = storeCompareOutput + '/' + next_wednesday

loggerAtt.info(f"Date : {Date}")
loggerAtt.info(f"File Location on Mount Point : {file_location}")
loggerAtt.info(f"Source or File Location on Container : {source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mounting ADLS location

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/Mount_Point_Creation

# COMMAND ----------

try:
  mounting(MountPoint, source, clientId, keyVaultName)
  ABC(MountCheck=1)
except Exception as ex:
  ABC(MountCheck=0)
  loggerAtt.error(str(ex))
  err = ErrorReturn('Error', ex,'Mounting')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

inputDataSchema = StructType([
                           StructField("Data",StringType(),True)
])


#Defining the schema for Delhaize Store Compare Table
loggerAtt.info('========Schema definition initiated ========')

#just in case if you want to change the types
posStoreCompareRaw_schema = StructType([
  StructField('RTX_STORE',StringType(),True),
  StructField('RTX_BATCH',LongType(),True),
  StructField('RTX_TYPE',IntegerType(),True),
  StructField('RTX_UPC',LongType(),True),
  StructField('RTX_LOAD',StringType(),True),
  StructField('SCRTX_DET_PLU_BTCH_NBR',IntegerType(),True),
  StructField('SCRTX_DET_OP_CODE',IntegerType(),True),
  StructField('SCRTX_DET_ITM_ID',IntegerType(),True),
  StructField('SCRTX_DET_STR_HIER_ID',IntegerType(),True),
  StructField('SCRTX_DET_DFLT_RTN_LOC_ID',IntegerType(),True),
  StructField('SCRTX_DET_MSG_CD',IntegerType(),True),
  StructField('SCRTX_DET_DSPL_DESCR',StringType(),True),
  StructField('SCRTX_DET_SLS_RESTRICT_GRP',IntegerType(),True),
  StructField('SCRTX_DET_RCPT_DESCR',StringType(),True),
  StructField('SCRTX_DET_TAXABILITY_CD',IntegerType(),True),
  StructField('SCRTX_DET_MDSE_XREF_ID',IntegerType(),True),
  StructField('SCRTX_DET_NON_MDSE_ID',IntegerType(),True),
  StructField('SCRTX_DET_UOM',StringType(),False),
  StructField('SCRTX_DET_UNT_QTY',StringType(),False),
#   StructField('FILLER',StringType(),False),
  StructField('SCRTX_DET_LIN_ITM_CD',IntegerType(),True),
  StructField('SCRTX_DET_MD_FG',IntegerType(),True),
  StructField('SCRTX_DET_QTY_RQRD_FG',IntegerType(),True),
  StructField('SCRTX_DET_SUBPRD_CNT',IntegerType(),True),
  StructField('SCRTX_DET_QTY_ALLOWED_FG',IntegerType(),True),
  StructField('SCRTX_DET_SLS_AUTH_FG',IntegerType(),True),
  StructField('SCRTX_DET_FOOD_STAMP_FG',IntegerType(),True),
  StructField('SCRTX_DET_WIC_FG',IntegerType(),True),
  StructField('SCRTX_DET_PERPET_INV_FG',IntegerType(),True),
  StructField('SCRTX_DET_RTL_PRC',LongType(),True),
  StructField('SCRTX_DET_UNT_CST',StringType(),False),
  StructField('SCRTX_DET_MAN_PRC_LVL',IntegerType(),True),
  StructField('SCRTX_DET_MIN_MDSE_AMT',StringType(),False),
  StructField('SCRTX_DET_RTL_PRC_DATE',StringType(),False),
  StructField('SCRTX_DET_SERIAL_MDSE_FG',IntegerType(),True),
  StructField('SCRTX_DET_CNTR_PRC',StringType(),False),
  StructField('SCRTX_DET_MAX_MDSE_AMT',StringType(),False),
  StructField('SCRTX_DET_CNTR_PRC_DATE',StringType(),False),
  StructField('SCRTX_DET_NG_ENTRY_FG',IntegerType(),True),
  StructField('SCRTX_DET_STR_CPN_FG',IntegerType(),True),
  StructField('SCRTX_DET_VEN_CPN_FG',IntegerType(),True),
  StructField('SCRTX_DET_MAN_PRC_FG',IntegerType(),True),
  StructField('SCRTX_DET_WGT_ITM_FG',IntegerType(),True),
  StructField('SCRTX_DET_NON_DISC_FG',IntegerType(),True),
  StructField('SCRTX_DET_COST_PLUS_FG',IntegerType(),True),
  StructField('SCRTX_DET_PRC_VRFY_FG',IntegerType(),True),
  StructField('SCRTX_DET_PRC_OVRD_FG',IntegerType(),True),
  StructField('SCRTX_DET_SPLR_PROM_FG',IntegerType(),True),
  StructField('SCRTX_DET_SAVE_DISC_FG',IntegerType(),True),
  StructField('SCRTX_DET_ITM_ONSALE_FG',IntegerType(),True),
  StructField('SCRTX_DET_INHBT_QTY_FG',IntegerType(),True),
  StructField('SCRTX_DET_DCML_QTY_FG',IntegerType(),True),
  StructField('SCRTX_DET_SHELF_LBL_RQRD_FG',IntegerType(),True),
  StructField('SCRTX_DET_TAX_RATE1_FG',IntegerType(),True),
  StructField('SCRTX_DET_TAX_RATE2_FG',IntegerType(),True),
  StructField('SCRTX_DET_TAX_RATE3_FG',IntegerType(),True),
  StructField('SCRTX_DET_TAX_RATE4_FG',IntegerType(),True),
  StructField('SCRTX_DET_TAX_RATE5_FG',IntegerType(),True),
  StructField('SCRTX_DET_TAX_RATE6_FG',IntegerType(),True),
  StructField('SCRTX_DET_TAX_RATE7_FG',IntegerType(),True),
  StructField('SCRTX_DET_TAX_RATE8_FG',IntegerType(),True),
  StructField('SCRTX_DET_COST_CASE_PRC',StringType(),False),
  StructField('SCRTX_DET_DATE_COST_CASE_PRC',StringType(),False),
  StructField('SCRTX_DET_UNIT_CASE',IntegerType(),True),
  StructField('SCRTX_DET_MIX_MATCH_CD',IntegerType(),True),
  StructField('SCRTX_DET_RTN_CD',IntegerType(),True),
  StructField('SCRTX_DET_FAMILY_CD',IntegerType(),True),
  StructField('SCRTX_DET_SUBDEP_ID',IntegerType(),True),
  StructField('SCRTX_DET_DISC_CD',IntegerType(),True),
  StructField('SCRTX_DET_LBL_QTY',IntegerType(),True),
  StructField('SCRTX_DET_SCALE_FG',IntegerType(),True),
  StructField('SCRTX_DET_LOCAL_DEL_FG',IntegerType(),True),
  StructField('SCRTX_DET_HOST_DEL_FG',IntegerType(),True),
  StructField('SCRTX_DET_HEAD_OFFICE_DEP',IntegerType(),True),
  StructField('SCRTX_DET_WGT_SCALE_FG',IntegerType(),True),
  StructField('SCRTX_DET_FREQ_SHOP_TYPE',IntegerType(),True),
  StructField('SCRTX_DET_FREQ_SHOP_VAL',StringType(),True),
  StructField('SCRTX_DET_SEC_FAMILY',IntegerType(),True),
  StructField('SCRTX_DET_POS_MSG',IntegerType(),True),
  StructField('SCRTX_DET_SHELF_LIFE_DAY',IntegerType(),True),
  StructField('SCRTX_DET_PROM_NBR',IntegerType(),True),
  StructField('SCRTX_DET_BCKT_NBR',IntegerType(),True),
  StructField('SCRTX_DET_EXTND_PROM_NBR',IntegerType(),True),
  StructField('SCRTX_DET_EXTND_BCKT_NBR',IntegerType(),True),
  StructField('SCRTX_DET_RCPT_DESCR1',StringType(),False),
  StructField('SCRTX_DET_RCPT_DESCR2',StringType(),False),
  StructField('SCRTX_DET_RCPT_DESCR3',StringType(),False),
  StructField('SCRTX_DET_RCPT_DESCR4',StringType(),False),
  StructField('SCRTX_DET_TAR_WGT_NBR',IntegerType(),True),
  StructField('SCRTX_DET_RSTRCT_LAYOUT',IntegerType(),True),
  StructField('SCRTX_DET_INTRNL_ID',IntegerType(),True),
  StructField('SCRTX_DET_OLD_PRC',IntegerType(),True),
  StructField('SCRTX_DET_QDX_FREQ_SHOP_VAL',IntegerType(),True),
  StructField('SCRTX_DET_VND_ID',StringType(),False),
  StructField('SCRTX_DET_VND_ITM_ID',StringType(),False),
  StructField('SCRTX_DET_VND_ITM_SZ',StringType(),False),
  StructField('SCRTX_DET_CMPRTV_UOM',IntegerType(),True),
  StructField('SCRTX_DET_CMPR_QTY',IntegerType(),True),
  StructField('SCRTX_DET_CMPR_UNT',IntegerType(),True),
  StructField('SCRTX_DET_BNS_CPN_FG',IntegerType(),True),
  StructField('SCRTX_DET_EX_MIN_PURCH_FG',IntegerType(),True),
  StructField('SCRTX_DET_FUEL_FG',IntegerType(),True),
  StructField('SCRTX_DET_SPR_AUTH_RQRD_FG',IntegerType(),True),
  StructField('SCRTX_DET_SSP_PRDCT_FG',IntegerType(),True),
  StructField('SCRTX_DET_NU06_FG',IntegerType(),True),
  StructField('SCRTX_DET_NU07_FG',IntegerType(),True),
  StructField('SCRTX_DET_NU08_FG',IntegerType(),True),
  StructField('SCRTX_DET_NU09_FG',IntegerType(),True),
  StructField('SCRTX_DET_NU10_FG',IntegerType(),True),
  StructField('SCRTX_DET_FREQ_SHOP_LMT',IntegerType(),True),
  StructField('SCRTX_DET_ITM_STATUS',IntegerType(),True),
  StructField('SCRTX_DET_DEA_GRP',IntegerType(),True),
  StructField('SCRTX_DET_BNS_BY_OPCODE',IntegerType(),True),
  StructField('SCRTX_DET_BNS_BY_DESCR',StringType(),False),
  StructField('SCRTX_DET_COMP_TYPE',IntegerType(),True),
  StructField('SCRTX_DET_COMP_PRC',StringType(),True),
  StructField('SCRTX_DET_COMP_QTY',IntegerType(),True),
  StructField('SCRTX_DET_ASSUME_QTY_FG',IntegerType(),True),
  StructField('SCRTX_DET_EXCISE_TAX_NBR',IntegerType(),True),
  StructField('SCRTX_DET_RTL_PRICE_DATE',StringType(),False),
  StructField('SCRTX_DET_PRC_RSN_ID',IntegerType(),True),
  StructField('SCRTX_DET_ITM_POINT',IntegerType(),True),
  StructField('SCRTX_DET_PRC_GRP_ID',IntegerType(),True),
  StructField('SCRTX_DET_SWW_CODE_FG',IntegerType(),True),
  StructField('SCRTX_DET_SHELF_STOCK_FG',IntegerType(),True),
  StructField('SCRTX_DET_PRT_PLUID_RCPT_FG',IntegerType(),True),
  StructField('SCRTX_DET_BLK_GRP',IntegerType(),True),
  StructField('SCRTX_DET_EXCHNGE_TENDER_ID',IntegerType(),True),
  StructField('SCRTX_DET_CAR_WASH_FG',IntegerType(),True),
  StructField('SCRTX_DET_EXMPT_FRM_PROM_FG',IntegerType(),True),
  StructField('SCRTX_DET_QSR_ITM_TYP',IntegerType(),True),
  StructField('SCRTX_DET_RSTRCSALE_BRCD_FG',IntegerType(),True),
  StructField('SCRTX_DET_NON_RX_HEALTH_FG',IntegerType(),True),
  StructField('SCRTX_DET_RX_FG',IntegerType(),True),
  StructField('SCRTX_DET_LNK_NBR',IntegerType(),True),
  StructField('SCRTX_DET_WIC_CVV_FG',IntegerType(),True),
  StructField('SCRTX_DET_CENTRAL_ITEM',IntegerType(),True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table Creation

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Item Main

# COMMAND ----------

def deltaCreator(deltapath):
    global DeltaTableCreateCheck
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
                  SCRTX_DET_FOOD_STAMP_FG  STRING,
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
                  SCRTX_DET_WGT_ITM_FG STRING,
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
                  SCRTX_DET_TAX_RATE1_FG STRING,
                  SCRTX_DET_TAX_RATE2_FG STRING,
                  SCRTX_DET_TAX_RATE3_FG STRING,
                  SCRTX_DET_TAX_RATE4_FG STRING,
                  SCRTX_DET_TAX_RATE5_FG STRING,
                  SCRTX_DET_TAX_RATE6_FG STRING,
                  SCRTX_DET_TAX_RATE7_FG STRING,
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
                  SCRTX_HDR_DESC STRING,
                  RowNumber LONG,
                  INSERT_ID STRING,
                  INSERT_TIMESTAMP TIMESTAMP,
                  LAST_UPDATE_ID STRING,
                  LAST_UPDATE_TIMESTAMP TIMESTAMP )
                USING delta 
                LOCATION '{}' 
                PARTITIONED BY (RTX_STORE, SCRTX_HDR_ACT_DATE)""".format(deltapath))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Bottle deposit table

# COMMAND ----------

try:
  ABC(DeltaTableCreateCheck=1)
  spark.sql(""" CREATE TABLE IF NOT EXISTS bottleDeposit(
                BOTTLE_DEPOSIT_STORE STRING,
                BOTTLE_DEPOSIT_LNK_NBR STRING,
                BOTTLE_DEPOSIT_ITM_ID LONG,
                BOTTLE_DEPOSIT_RTL_PRC FLOAT)
              USING delta
              PARTITIONED BY (BOTTLE_DEPOSIT_STORE)
              LOCATION '{}' """.format(BottleDepositpath))
except Exception as ex:
  ABC(DeltaTableCreateCheck = 0)
  loggerAtt.error(ex)
  err = ErrorReturn('Error', ex,'BottleDepositpath deltaCreator')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Store Detail Table information

# COMMAND ----------

try:
  ABC(DeltaTableCreateCheck=1)
  spark.sql("""
  CREATE TABLE IF NOT EXISTS Store_delta_Delhaize (
  Banner_ID String,
  STORE_NUMBER String,
  STORE_NAME String,
  STORE_TYPE String,
  ADDRESS_LINE_1 String, 
  ADDRESS_LINE_2 String,
  CITY String,
  STATE String,
  ZIP_CODE String,
  PHONE String,
  MANAGERS_NAME String,
  PHARMACY_INDICATOR String,
  CATALOG_ID Integer,
  FAX String,
  PHONE_PHARM String,
  FAX_PHARM String,
  SUMMERHOURSYN String,
  MONDAY String,
  MONDAY_SUMMER String,
  MONDAY_PHARM String,
  TUESDAY String,
  TUESDAY_SUMMER String,
  TUESDAY_PHARM String,
  WEDNESDAY String,
  WEDNESDAY_SUMMER String,
  WEDNESDAY_PHARM String,
  THURSDAY String,
  THURSDAY_SUMMER String,
  THURSDAY_PHARM String,
  FRIDAY String,
  FRIDAY_SUMMER String,
  FRIDAY_PHARM String,
  SATURDAY String,
  SATURDAY_SUMMER String,
  SATURDAY_PHARM String,
  SUNDAY String,
  SUNDAY_SUMMER String,
  SUNDAY_PHARM String,
  MLK String,
  MLK_PHARM String,
  PRESIDENTS String,
  PRESIDENTS_PHARM String,
  EASTER String,
  EASTER_PHARM String,
  PATRIOTS String,
  PATRIOTS_PHARM String,
  MEMORIAL String,
  MEMORIAL_PHARM String,
  JULY4 String,
  JULY4_PHARM String,
  LABOR String,
  LABOR_PHARM String,
  COLUMBUS String,
  COLUMBUS_PHARM String,
  VETERANS String,
  VETERANS_PHARM String,
  THXGIVING String,
  THXGIVING_PHARM String,
  XMASEVE String,
  XMASEVE_PHARM String,
  XMAS String,
  XMAS_PHARM String,
  NEWYRSEVE String,
  NEWYRSEVE_PHARM String,
  NEWYRS String,
  NEWYRS_PHARM String,
  INDEPENDENT String,
  WEBSITE String,
  ATM String,
  CHECK_CASHING String,
  INSTORE_BANK String,
  MONEY_ORDERS String,
  MONEY_TRANSFERS String, 
  UTILITY_PAYMENTS String,
  POSTAGE_STAMPS String,
  LOTTERY_TICKETS String,
  NEW_YORK_EZPASS String,
  DUMP_PASSES String,
  FISHING_LICENSES String,
  COIN_COUNTING_KIOSK String,
  DVD_RENTAL_KIOSK String,
  BOTTLE_REDEMPTION String,
  CLYNK_REDEMPTION String,
  GREENBACK_REDEMPTION String,
  TOWN_TRASH_BAGS String,
  PROPANE_TANK_EXCHANGE String,
  WATER_COOLER_JUGS String,
  RUG_CLEANER_RENTALS String,
  PARTY_BALLOONS String,
  PARTY_PLATTERS_AND_FRUIT_BAS String,
  CAKE_DECORATING String,
  FLOWERS_AND_FLORAL_ARRANGEMEN String,
  SUSHI String,
  WINE String,
  LIQUOR String,
  CAFE String,
  SHELLFISH_STEAMER String,
  LOBSTER_TANK String,
  DRIVE_THRU_RX String,
  BLOOD_PRESSURE_MACHINE String,
  NUTITIONIST String,
  BUS_PARTNERSHIP String,
  ISLAND_DELIVERY_SERVICE String,
  ONLINE_WINE String,
  SELF_SCAN String,
  SALAD_BAR String,
  OLIVE_BAR String,
  WING_BAR String,
  BEER String,
  BUTCHER_SHOP String,
  ISPU_INDICATOR String,
  DIRECT_FLAG String,
  PICKING_FEE_WAIVED String,
  LIQUOR_STORE_UPDATE_FLAG String,
  MONDAY_LIQUOR String,
  TUESDAY_LIQUOR String,
  WEDNESDAY_LIQUOR String, 
  THURSDAY_LIQUOR String,
  FRIDAY_LIQUOR String,
  SATURDAY_LIQUOR String,
  SUNDAY_LIQUOR String,
  PHONE_LIQUOR String,
  PHYS_CLSE_DT  Date,
  FIN_CLSE_DT  String,
  SYS_XPIR_DT  String,
  PHAR_CLSE_DT  String,
  INSERT_TIMESTAMP TimeStamp,
  LAST_UPDATE_ID String,
  LAST_UPDATE_TIMESTAMP TimeStamp,
  INSERT_ID String
  )
  USING delta
  Location '{}'
  PARTITIONED BY (Banner_ID)
  """.format(storeDeltaPath))
except Exception as ex:
  ABC(DeltaTableCreateCheck = 0)
  loggerAtt.error(ex)
  err = ErrorReturn('Error', ex,'storeDeltaPath deltaCreator')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Transformation and UDF

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Store detail fetch

# COMMAND ----------

def fetchStoreDetail():
  loggerAtt.info("Fetch Banner ID from Store Detail Delta table initiated")
  storeDf = spark.sql('''SELECT BANNER_ID, Store_NUMBER FROM delta.`{}`'''.format(storeDeltaPath))
  storeDf = storeDf.withColumn('BANNER_ID', regexp_replace(col("BANNER_ID"), " ", ""))
  storeDf.createOrReplaceTempView("store")
  loggerAtt.info("Fetch Banner ID from Store Detial Delta table end")
  return storeDf

# COMMAND ----------

# MAGIC %md 
# MAGIC ### SUBSTR and TYPE CAST

# COMMAND ----------

def inputDataSubstr(inputDataDF):
  df = None
    
  df = inputDataDF.withColumn('RTX_STORE',col('Data').substr(1,4))\
  .withColumn('RTX_BATCH',col('Data').substr(5,13))\
  .withColumn('RTX_TYPE',col('Data').substr(14,1))\
  .withColumn('RTX_UPC',col('Data').substr(15,13))\
  .withColumn('RTX_LOAD',col('Data').substr(28,4))\
  .withColumn('SCRTX_DET_PLU_BTCH_NBR',col('Data').substr(28,9))\
  .withColumn('SCRTX_DET_OP_CODE',col('Data').substr(37,1))\
  .withColumn('SCRTX_DET_ITM_ID',col('Data').substr(38,13))\
  .withColumn('SCRTX_DET_STR_HIER_ID',col('Data').substr(51,4))\
  .withColumn('SCRTX_DET_DFLT_RTN_LOC_ID',col('Data').substr(55,4))\
  .withColumn('SCRTX_DET_MSG_CD',col('Data').substr(59,2))\
  .withColumn('SCRTX_DET_DSPL_DESCR',col('Data').substr(61,40))\
  .withColumn('SCRTX_DET_SLS_RESTRICT_GRP',col('Data').substr(101,2))\
  .withColumn('SCRTX_DET_RCPT_DESCR',col('Data').substr(103,20))\
  .withColumn('SCRTX_DET_TAXABILITY_CD',col('Data').substr(123,4))\
  .withColumn('SCRTX_DET_MDSE_XREF_ID',col('Data').substr(127,4))\
  .withColumn('SCRTX_DET_NON_MDSE_ID',col('Data').substr(131,1))\
  .withColumn('SCRTX_DET_UOM',col('Data').substr(132,4))\
  .withColumn('SCRTX_DET_UNT_QTY',col('Data').substr(137,2))\
  .withColumn('SCRTX_DET_LIN_ITM_CD',col('Data').substr(139,4))\
  .withColumn('SCRTX_DET_MD_FG',col('Data').substr(143,1))\
  .withColumn('SCRTX_DET_QTY_RQRD_FG',col('Data').substr(144,1))\
  .withColumn('SCRTX_DET_SUBPRD_CNT',col('Data').substr(145,4))\
  .withColumn('SCRTX_DET_QTY_ALLOWED_FG',col('Data').substr(149,1))\
  .withColumn('SCRTX_DET_SLS_AUTH_FG',col('Data').substr(150,1))\
  .withColumn('SCRTX_DET_FOOD_STAMP_FG',col('Data').substr(151,1))\
  .withColumn('SCRTX_DET_WIC_FG',col('Data').substr(152,1))\
  .withColumn('SCRTX_DET_PERPET_INV_FG',col('Data').substr(153,1))\
  .withColumn('SCRTX_DET_RTL_PRC',col('Data').substr(154,13))\
  .withColumn('SCRTX_DET_UNT_CST',col('Data').substr(167,13))\
  .withColumn('SCRTX_DET_MAN_PRC_LVL',col('Data').substr(180,2))\
  .withColumn('SCRTX_DET_MIN_MDSE_AMT',col('Data').substr(182,13))\
  .withColumn('SCRTX_DET_RTL_PRC_DATE',col('Data').substr(195,26))\
  .withColumn('SCRTX_DET_SERIAL_MDSE_FG',col('Data').substr(221,1))\
  .withColumn('SCRTX_DET_CNTR_PRC',col('Data').substr(222,13))\
  .withColumn('SCRTX_DET_MAX_MDSE_AMT',col('Data').substr(235,13))\
  .withColumn('SCRTX_DET_CNTR_PRC_DATE',col('Data').substr(248,26))\
  .withColumn('SCRTX_DET_NG_ENTRY_FG',col('Data').substr(274,1))\
  .withColumn('SCRTX_DET_STR_CPN_FG',col('Data').substr(275,1))\
  .withColumn('SCRTX_DET_VEN_CPN_FG',col('Data').substr(276,1))\
  .withColumn('SCRTX_DET_MAN_PRC_FG',col('Data').substr(277,1))\
  .withColumn('SCRTX_DET_WGT_ITM_FG',col('Data').substr(278,1))\
  .withColumn('SCRTX_DET_NON_DISC_FG',col('Data').substr(279,1))\
  .withColumn('SCRTX_DET_COST_PLUS_FG',col('Data').substr(280,1))\
  .withColumn('SCRTX_DET_PRC_VRFY_FG',col('Data').substr(281,1))\
  .withColumn('SCRTX_DET_PRC_OVRD_FG',col('Data').substr(282,1))\
  .withColumn('SCRTX_DET_SPLR_PROM_FG',col('Data').substr(283,1))\
  .withColumn('SCRTX_DET_SAVE_DISC_FG',col('Data').substr(284,1))\
  .withColumn('SCRTX_DET_ITM_ONSALE_FG',col('Data').substr(285,1))\
  .withColumn('SCRTX_DET_INHBT_QTY_FG',col('Data').substr(286,1))\
  .withColumn('SCRTX_DET_DCML_QTY_FG',col('Data').substr(287,1))\
  .withColumn('SCRTX_DET_SHELF_LBL_RQRD_FG',col('Data').substr(288,1))\
  .withColumn('SCRTX_DET_TAX_RATE1_FG',col('Data').substr(289,1))\
  .withColumn('SCRTX_DET_TAX_RATE2_FG',col('Data').substr(290,1))\
  .withColumn('SCRTX_DET_TAX_RATE3_FG',col('Data').substr(291,1))\
  .withColumn('SCRTX_DET_TAX_RATE4_FG',col('Data').substr(292,1))\
  .withColumn('SCRTX_DET_TAX_RATE5_FG',col('Data').substr(293,1))\
  .withColumn('SCRTX_DET_TAX_RATE6_FG',col('Data').substr(294,1))\
  .withColumn('SCRTX_DET_TAX_RATE7_FG',col('Data').substr(295,1))\
  .withColumn('SCRTX_DET_TAX_RATE8_FG',col('Data').substr(296,1))\
  .withColumn('SCRTX_DET_COST_CASE_PRC',col('Data').substr(297,13))\
  .withColumn('SCRTX_DET_DATE_COST_CASE_PRC',col('Data').substr(310,26))\
  .withColumn('SCRTX_DET_UNIT_CASE',col('Data').substr(336,3))\
  .withColumn('SCRTX_DET_MIX_MATCH_CD',col('Data').substr(339,3))\
  .withColumn('SCRTX_DET_RTN_CD',col('Data').substr(342,3))\
  .withColumn('SCRTX_DET_FAMILY_CD',col('Data').substr(345,3))\
  .withColumn('SCRTX_DET_SUBDEP_ID',col('Data').substr(348,12))\
  .withColumn('SCRTX_DET_DISC_CD',col('Data').substr(360,2))\
  .withColumn('SCRTX_DET_LBL_QTY',col('Data').substr(362,2))\
  .withColumn('SCRTX_DET_SCALE_FG',col('Data').substr(364,1))\
  .withColumn('SCRTX_DET_LOCAL_DEL_FG',col('Data').substr(365,1))\
  .withColumn('SCRTX_DET_HOST_DEL_FG',col('Data').substr(366,1))\
  .withColumn('SCRTX_DET_HEAD_OFFICE_DEP',col('Data').substr(367,12))\
  .withColumn('SCRTX_DET_WGT_SCALE_FG',col('Data').substr(379,1))\
  .withColumn('SCRTX_DET_FREQ_SHOP_TYPE',col('Data').substr(380,1))\
  .withColumn('SCRTX_DET_FREQ_SHOP_VAL',col('Data').substr(381,13))\
  .withColumn('SCRTX_DET_SEC_FAMILY',col('Data').substr(394,4))\
  .withColumn('SCRTX_DET_POS_MSG',col('Data').substr(398,2))\
  .withColumn('SCRTX_DET_SHELF_LIFE_DAY',col('Data').substr(400,4))\
  .withColumn('SCRTX_DET_PROM_NBR',col('Data').substr(404,6))\
  .withColumn('SCRTX_DET_BCKT_NBR',col('Data').substr(410,6))\
  .withColumn('SCRTX_DET_EXTND_PROM_NBR',col('Data').substr(416,6))\
  .withColumn('SCRTX_DET_EXTND_BCKT_NBR',col('Data').substr(422,6))\
  .withColumn('SCRTX_DET_RCPT_DESCR1',col('Data').substr(428,20))\
  .withColumn('SCRTX_DET_RCPT_DESCR2',col('Data').substr(448,20))\
  .withColumn('SCRTX_DET_RCPT_DESCR3',col('Data').substr(468,20))\
  .withColumn('SCRTX_DET_RCPT_DESCR4',col('Data').substr(488,20))\
  .withColumn('SCRTX_DET_TAR_WGT_NBR',col('Data').substr(521,2))\
  .withColumn('SCRTX_DET_RSTRCT_LAYOUT',col('Data').substr(523,2))\
  .withColumn('SCRTX_DET_INTRNL_ID',col('Data').substr(525,13))\
  .withColumn('SCRTX_DET_OLD_PRC',col('Data').substr(538,13))\
  .withColumn('SCRTX_DET_QDX_FREQ_SHOP_VAL',col('Data').substr(551,13))\
  .withColumn('SCRTX_DET_VND_ID',col('Data').substr(564,8))\
  .withColumn('SCRTX_DET_VND_ITM_ID',col('Data').substr(572,25))\
  .withColumn('SCRTX_DET_VND_ITM_SZ',col('Data').substr(597,10))\
  .withColumn('SCRTX_DET_CMPRTV_UOM',col('Data').substr(607,3))\
  .withColumn('SCRTX_DET_CMPR_QTY',col('Data').substr(610,12))\
  .withColumn('SCRTX_DET_CMPR_UNT',col('Data').substr(622,12))\
  .withColumn('SCRTX_DET_BNS_CPN_FG',col('Data').substr(634,1))\
  .withColumn('SCRTX_DET_EX_MIN_PURCH_FG',col('Data').substr(635,1))\
  .withColumn('SCRTX_DET_FUEL_FG',col('Data').substr(636,1))\
  .withColumn('SCRTX_DET_SPR_AUTH_RQRD_FG',col('Data').substr(637,3))\
  .withColumn('SCRTX_DET_SSP_PRDCT_FG',col('Data').substr(640,1))\
  .withColumn('SCRTX_DET_NU06_FG',col('Data').substr(641,3))\
  .withColumn('SCRTX_DET_NU07_FG',col('Data').substr(644,3))\
  .withColumn('SCRTX_DET_NU08_FG',col('Data').substr(647,3))\
  .withColumn('SCRTX_DET_NU09_FG',col('Data').substr(650,3))\
  .withColumn('SCRTX_DET_NU10_FG',col('Data').substr(653,3))\
  .withColumn('SCRTX_DET_FREQ_SHOP_LMT',col('Data').substr(656,3))\
  .withColumn('SCRTX_DET_ITM_STATUS',col('Data').substr(659,3))\
  .withColumn('SCRTX_DET_DEA_GRP',col('Data').substr(662,2))\
  .withColumn('SCRTX_DET_BNS_BY_OPCODE',col('Data').substr(664,2))\
  .withColumn('SCRTX_DET_BNS_BY_DESCR',col('Data').substr(666,20))\
  .withColumn('SCRTX_DET_COMP_TYPE',col('Data').substr(686,2))\
  .withColumn('SCRTX_DET_COMP_PRC',col('Data').substr(688,13))\
  .withColumn('SCRTX_DET_COMP_QTY',col('Data').substr(701,4))\
  .withColumn('SCRTX_DET_ASSUME_QTY_FG',col('Data').substr(705,1))\
  .withColumn('SCRTX_DET_EXCISE_TAX_NBR',col('Data').substr(706,3))\
  .withColumn('SCRTX_DET_RTL_PRICE_DATE',col('Data').substr(709,26))\
  .withColumn('SCRTX_DET_PRC_RSN_ID',col('Data').substr(735,3))\
  .withColumn('SCRTX_DET_ITM_POINT',col('Data').substr(738,4))\
  .withColumn('SCRTX_DET_PRC_GRP_ID',col('Data').substr(742,2))\
  .withColumn('SCRTX_DET_SWW_CODE_FG',col('Data').substr(744,1))\
  .withColumn('SCRTX_DET_SHELF_STOCK_FG',col('Data').substr(745,1))\
  .withColumn('SCRTX_DET_PRT_PLUID_RCPT_FG',col('Data').substr(746,1))\
  .withColumn('SCRTX_DET_BLK_GRP',col('Data').substr(747,1))\
  .withColumn('SCRTX_DET_EXCHNGE_TENDER_ID',col('Data').substr(748,2))\
  .withColumn('SCRTX_DET_CAR_WASH_FG',col('Data').substr(750,1))\
  .withColumn('SCRTX_DET_EXMPT_FRM_PROM_FG',col('Data').substr(751,1))\
  .withColumn('SCRTX_DET_QSR_ITM_TYP',col('Data').substr(752,1))\
  .withColumn('SCRTX_DET_RSTRCSALE_BRCD_FG',col('Data').substr(753,1))\
  .withColumn('SCRTX_DET_NON_RX_HEALTH_FG',col('Data').substr(754,1))\
  .withColumn('SCRTX_DET_RX_FG',col('Data').substr(755,1))\
  .withColumn('SCRTX_DET_LNK_NBR',col('Data').substr(756,3))\
  .withColumn('SCRTX_DET_WIC_CVV_FG',col('Data').substr(759,1))\
  .withColumn('SCRTX_DET_CENTRAL_ITEM',col('Data').substr(760,1))\
  
  return df.drop('Data')
  


# COMMAND ----------

def rawDataTypeCast(rawDataDF):
  df = None

  df = rawDataDF.withColumn('RTX_STORE',col('RTX_STORE'))\
  .withColumn('RTX_BATCH',col('RTX_BATCH').cast(LongType()))\
  .withColumn('RTX_TYPE',col('RTX_TYPE').cast(IntegerType()))\
  .withColumn('RTX_UPC',col('RTX_UPC').cast(LongType()))\
  .withColumn('RTX_LOAD',col('RTX_LOAD'))\
  .withColumn('SCRTX_DET_PLU_BTCH_NBR',col('SCRTX_DET_PLU_BTCH_NBR').cast(IntegerType()))\
  .withColumn('SCRTX_DET_OP_CODE',col('SCRTX_DET_OP_CODE').cast(IntegerType()))\
  .withColumn('SCRTX_DET_ITM_ID',col('SCRTX_DET_ITM_ID').cast(LongType()))\
  .withColumn('SCRTX_DET_STR_HIER_ID',col('SCRTX_DET_STR_HIER_ID').cast(IntegerType()))\
  .withColumn('SCRTX_DET_DFLT_RTN_LOC_ID',col('SCRTX_DET_DFLT_RTN_LOC_ID').cast(IntegerType()))\
  .withColumn('SCRTX_DET_MSG_CD',col('SCRTX_DET_MSG_CD').cast(IntegerType()))\
  .withColumn('SCRTX_DET_DSPL_DESCR',col('SCRTX_DET_DSPL_DESCR'))\
  .withColumn('SCRTX_DET_SLS_RESTRICT_GRP',col('SCRTX_DET_SLS_RESTRICT_GRP').cast(IntegerType()))\
  .withColumn('SCRTX_DET_RCPT_DESCR',col('SCRTX_DET_RCPT_DESCR'))\
  .withColumn('SCRTX_DET_TAXABILITY_CD',col('SCRTX_DET_TAXABILITY_CD').cast(IntegerType()))\
  .withColumn('SCRTX_DET_MDSE_XREF_ID',col('SCRTX_DET_MDSE_XREF_ID').cast(IntegerType()))\
  .withColumn('SCRTX_DET_NON_MDSE_ID',col('SCRTX_DET_NON_MDSE_ID').cast(IntegerType()))\
  .withColumn('SCRTX_DET_UOM',col('SCRTX_DET_UOM'))\
  .withColumn('SCRTX_DET_LIN_ITM_CD',col('SCRTX_DET_LIN_ITM_CD').cast(IntegerType()))\
  .withColumn('SCRTX_DET_MD_FG',col('SCRTX_DET_MD_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_QTY_RQRD_FG',col('SCRTX_DET_QTY_RQRD_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_SUBPRD_CNT',col('SCRTX_DET_SUBPRD_CNT').cast(IntegerType()))\
  .withColumn('SCRTX_DET_QTY_ALLOWED_FG',col('SCRTX_DET_QTY_ALLOWED_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_SLS_AUTH_FG',col('SCRTX_DET_SLS_AUTH_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_FOOD_STAMP_FG',col('SCRTX_DET_FOOD_STAMP_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_WIC_FG',col('SCRTX_DET_WIC_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_PERPET_INV_FG',col('SCRTX_DET_PERPET_INV_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_UNT_CST',col('SCRTX_DET_UNT_CST'))\
  .withColumn('SCRTX_DET_MAN_PRC_LVL',col('SCRTX_DET_MAN_PRC_LVL').cast(IntegerType()))\
  .withColumn('SCRTX_DET_MIN_MDSE_AMT',col('SCRTX_DET_MIN_MDSE_AMT'))\
  .withColumn('SCRTX_DET_RTL_PRC_DATE',col('SCRTX_DET_RTL_PRC_DATE'))\
  .withColumn('SCRTX_DET_SERIAL_MDSE_FG',col('SCRTX_DET_SERIAL_MDSE_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_CNTR_PRC',col('SCRTX_DET_CNTR_PRC'))\
  .withColumn('SCRTX_DET_MAX_MDSE_AMT',col('SCRTX_DET_MAX_MDSE_AMT'))\
  .withColumn('SCRTX_DET_CNTR_PRC_DATE',col('SCRTX_DET_CNTR_PRC_DATE'))\
  .withColumn('SCRTX_DET_NG_ENTRY_FG',col('SCRTX_DET_NG_ENTRY_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_STR_CPN_FG',col('SCRTX_DET_STR_CPN_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_VEN_CPN_FG',col('SCRTX_DET_VEN_CPN_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_MAN_PRC_FG',col('SCRTX_DET_MAN_PRC_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_WGT_ITM_FG',col('SCRTX_DET_WGT_ITM_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_NON_DISC_FG',col('SCRTX_DET_NON_DISC_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_COST_PLUS_FG',col('SCRTX_DET_COST_PLUS_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_PRC_VRFY_FG',col('SCRTX_DET_PRC_VRFY_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_PRC_OVRD_FG',col('SCRTX_DET_PRC_OVRD_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_SPLR_PROM_FG',col('SCRTX_DET_SPLR_PROM_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_SAVE_DISC_FG',col('SCRTX_DET_SAVE_DISC_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_ITM_ONSALE_FG',col('SCRTX_DET_ITM_ONSALE_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_INHBT_QTY_FG',col('SCRTX_DET_INHBT_QTY_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_DCML_QTY_FG',col('SCRTX_DET_DCML_QTY_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_SHELF_LBL_RQRD_FG',col('SCRTX_DET_SHELF_LBL_RQRD_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_TAX_RATE1_FG',col('SCRTX_DET_TAX_RATE1_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_TAX_RATE2_FG',col('SCRTX_DET_TAX_RATE2_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_TAX_RATE3_FG',col('SCRTX_DET_TAX_RATE3_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_TAX_RATE4_FG',col('SCRTX_DET_TAX_RATE4_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_TAX_RATE5_FG',col('SCRTX_DET_TAX_RATE5_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_TAX_RATE6_FG',col('SCRTX_DET_TAX_RATE6_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_TAX_RATE7_FG',col('SCRTX_DET_TAX_RATE7_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_TAX_RATE8_FG',col('SCRTX_DET_TAX_RATE8_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_COST_CASE_PRC',col('SCRTX_DET_COST_CASE_PRC'))\
  .withColumn('SCRTX_DET_DATE_COST_CASE_PRC',col('SCRTX_DET_DATE_COST_CASE_PRC'))\
  .withColumn('SCRTX_DET_UNIT_CASE',col('SCRTX_DET_UNIT_CASE').cast(IntegerType()))\
  .withColumn('SCRTX_DET_MIX_MATCH_CD',col('SCRTX_DET_MIX_MATCH_CD').cast(IntegerType()))\
  .withColumn('SCRTX_DET_RTN_CD',col('SCRTX_DET_RTN_CD').cast(IntegerType()))\
  .withColumn('SCRTX_DET_FAMILY_CD',col('SCRTX_DET_FAMILY_CD').cast(IntegerType()))\
  .withColumn('SCRTX_DET_SUBDEP_ID',col('SCRTX_DET_SUBDEP_ID').cast(LongType()))\
  .withColumn('SCRTX_DET_DISC_CD',col('SCRTX_DET_DISC_CD').cast(IntegerType()))\
  .withColumn('SCRTX_DET_LBL_QTY',col('SCRTX_DET_LBL_QTY').cast(IntegerType()))\
  .withColumn('SCRTX_DET_SCALE_FG',col('SCRTX_DET_SCALE_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_LOCAL_DEL_FG',col('SCRTX_DET_LOCAL_DEL_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_HOST_DEL_FG',col('SCRTX_DET_HOST_DEL_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_HEAD_OFFICE_DEP',col('SCRTX_DET_HEAD_OFFICE_DEP').cast(LongType()))\
  .withColumn('SCRTX_DET_WGT_SCALE_FG',col('SCRTX_DET_WGT_SCALE_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_FREQ_SHOP_TYPE',col('SCRTX_DET_FREQ_SHOP_TYPE').cast(IntegerType()))\
  .withColumn('SCRTX_DET_FREQ_SHOP_VAL',col('SCRTX_DET_FREQ_SHOP_VAL'))\
  .withColumn('SCRTX_DET_SEC_FAMILY',col('SCRTX_DET_SEC_FAMILY').cast(IntegerType()))\
  .withColumn('SCRTX_DET_POS_MSG',col('SCRTX_DET_POS_MSG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_SHELF_LIFE_DAY',col('SCRTX_DET_SHELF_LIFE_DAY').cast(IntegerType()))\
  .withColumn('SCRTX_DET_PROM_NBR',col('SCRTX_DET_PROM_NBR').cast(IntegerType()))\
  .withColumn('SCRTX_DET_BCKT_NBR',col('SCRTX_DET_BCKT_NBR').cast(IntegerType()))\
  .withColumn('SCRTX_DET_EXTND_PROM_NBR',col('SCRTX_DET_EXTND_PROM_NBR').cast(IntegerType()))\
  .withColumn('SCRTX_DET_EXTND_BCKT_NBR',col('SCRTX_DET_EXTND_BCKT_NBR').cast(IntegerType()))\
  .withColumn('SCRTX_DET_RCPT_DESCR1',col('SCRTX_DET_RCPT_DESCR1'))\
  .withColumn('SCRTX_DET_RCPT_DESCR2',col('SCRTX_DET_RCPT_DESCR2'))\
  .withColumn('SCRTX_DET_RCPT_DESCR3',col('SCRTX_DET_RCPT_DESCR3'))\
  .withColumn('SCRTX_DET_RCPT_DESCR4',col('SCRTX_DET_RCPT_DESCR4'))\
  .withColumn('SCRTX_DET_TAR_WGT_NBR',col('SCRTX_DET_TAR_WGT_NBR').cast(IntegerType()))\
  .withColumn('SCRTX_DET_RSTRCT_LAYOUT',col('SCRTX_DET_RSTRCT_LAYOUT').cast(IntegerType()))\
  .withColumn('SCRTX_DET_INTRNL_ID',col('SCRTX_DET_INTRNL_ID').cast(LongType()))\
  .withColumn('SCRTX_DET_OLD_PRC',col('SCRTX_DET_OLD_PRC').cast(LongType()))\
  .withColumn('SCRTX_DET_QDX_FREQ_SHOP_VAL',col('SCRTX_DET_QDX_FREQ_SHOP_VAL').cast(LongType()))\
  .withColumn('SCRTX_DET_VND_ID',col('SCRTX_DET_VND_ID'))\
  .withColumn('SCRTX_DET_VND_ITM_ID',col('SCRTX_DET_VND_ITM_ID'))\
  .withColumn('SCRTX_DET_VND_ITM_SZ',col('SCRTX_DET_VND_ITM_SZ'))\
  .withColumn('SCRTX_DET_CMPRTV_UOM',col('SCRTX_DET_CMPRTV_UOM').cast(IntegerType()))\
  .withColumn('SCRTX_DET_CMPR_QTY',col('SCRTX_DET_CMPR_QTY').cast(LongType()))\
  .withColumn('SCRTX_DET_CMPR_UNT',col('SCRTX_DET_CMPR_UNT').cast(LongType()))\
  .withColumn('SCRTX_DET_BNS_CPN_FG',col('SCRTX_DET_BNS_CPN_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_EX_MIN_PURCH_FG',col('SCRTX_DET_EX_MIN_PURCH_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_FUEL_FG',col('SCRTX_DET_FUEL_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_SPR_AUTH_RQRD_FG',col('SCRTX_DET_SPR_AUTH_RQRD_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_SSP_PRDCT_FG',col('SCRTX_DET_SSP_PRDCT_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_NU06_FG',col('SCRTX_DET_NU06_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_NU07_FG',col('SCRTX_DET_NU07_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_NU08_FG',col('SCRTX_DET_NU08_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_NU09_FG',col('SCRTX_DET_NU09_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_NU10_FG',col('SCRTX_DET_NU10_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_FREQ_SHOP_LMT',col('SCRTX_DET_FREQ_SHOP_LMT').cast(IntegerType()))\
  .withColumn('SCRTX_DET_ITM_STATUS',col('SCRTX_DET_ITM_STATUS').cast(IntegerType()))\
  .withColumn('SCRTX_DET_DEA_GRP',col('SCRTX_DET_DEA_GRP').cast(IntegerType()))\
  .withColumn('SCRTX_DET_BNS_BY_OPCODE',col('SCRTX_DET_BNS_BY_OPCODE').cast(IntegerType()))\
  .withColumn('SCRTX_DET_BNS_BY_DESCR',col('SCRTX_DET_BNS_BY_DESCR'))\
  .withColumn('SCRTX_DET_COMP_TYPE',col('SCRTX_DET_COMP_TYPE').cast(IntegerType()))\
  .withColumn('SCRTX_DET_COMP_PRC',col('SCRTX_DET_COMP_PRC'))\
  .withColumn('SCRTX_DET_COMP_QTY',col('SCRTX_DET_COMP_QTY').cast(IntegerType()))\
  .withColumn('SCRTX_DET_ASSUME_QTY_FG',col('SCRTX_DET_ASSUME_QTY_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_EXCISE_TAX_NBR',col('SCRTX_DET_EXCISE_TAX_NBR').cast(IntegerType()))\
  .withColumn('SCRTX_DET_RTL_PRICE_DATE',col('SCRTX_DET_RTL_PRICE_DATE'))\
  .withColumn('SCRTX_DET_PRC_RSN_ID',col('SCRTX_DET_PRC_RSN_ID').cast(IntegerType()))\
  .withColumn('SCRTX_DET_ITM_POINT',col('SCRTX_DET_ITM_POINT').cast(IntegerType()))\
  .withColumn('SCRTX_DET_PRC_GRP_ID',col('SCRTX_DET_PRC_GRP_ID').cast(IntegerType()))\
  .withColumn('SCRTX_DET_SWW_CODE_FG',col('SCRTX_DET_SWW_CODE_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_SHELF_STOCK_FG',col('SCRTX_DET_SHELF_STOCK_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_PRT_PLUID_RCPT_FG',col('SCRTX_DET_PRT_PLUID_RCPT_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_BLK_GRP',col('SCRTX_DET_BLK_GRP').cast(IntegerType()))\
  .withColumn('SCRTX_DET_EXCHNGE_TENDER_ID',col('SCRTX_DET_EXCHNGE_TENDER_ID').cast(IntegerType()))\
  .withColumn('SCRTX_DET_CAR_WASH_FG',col('SCRTX_DET_CAR_WASH_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_EXMPT_FRM_PROM_FG',col('SCRTX_DET_EXMPT_FRM_PROM_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_QSR_ITM_TYP',col('SCRTX_DET_QSR_ITM_TYP').cast(IntegerType()))\
  .withColumn('SCRTX_DET_RSTRCSALE_BRCD_FG',col('SCRTX_DET_RSTRCSALE_BRCD_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_NON_RX_HEALTH_FG',col('SCRTX_DET_NON_RX_HEALTH_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_RX_FG',col('SCRTX_DET_RX_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_LNK_NBR',col('SCRTX_DET_LNK_NBR').cast(IntegerType()))\
  .withColumn('SCRTX_DET_WIC_CVV_FG',col('SCRTX_DET_WIC_CVV_FG').cast(IntegerType()))\
  .withColumn('SCRTX_DET_CENTRAL_ITEM',col('SCRTX_DET_CENTRAL_ITEM').cast(IntegerType()))\
  .withColumn('SCRTX_DET_UNT_QTY',col('SCRTX_DET_UNT_QTY').cast(StringType()))\
  .withColumn('SCRTX_DET_RTL_PRC',col('SCRTX_DET_RTL_PRC').cast(FloatType()))\

  return df
  

# COMMAND ----------

# MAGIC %md 
# MAGIC ### READ RAW SOURCE FOLDER 

# COMMAND ----------

def readInputFile(file_location, infer_schema, header, delimiter,file_type, schema):
  global raw_df
  
  inputDataDF = spark.read.format(file_type) \
  .option("mode","PERMISSIVE") \
  .option("header", header) \
  .option("sep", delimiter) \
  .schema(schema) \
  .load(file_location)
  
  raw_df = inputDataSubstr(inputDataDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Store Compare transformation

# COMMAND ----------

def flagChange(s):
  if s == 1:
    return 'Y'
  else:
    return 'N'
  
flagChangeUDF = udf(flagChange)   

def fsaFlagChange(s):
  if s == 1:
    return 1
  else:
    return 0
  
fsaFlagChangeUDF = udf(fsaFlagChange)   

def typeCheck(s):
  try: 
      int(s)
      return True
  except ValueError:
      return False

typeCheckUDF = udf(typeCheck)

# COMMAND ----------

def removeItemCheck(raw_df, itemMasterDeltaPath):
  weekDay = datetime.today().weekday()
#   weekDay = 1
  
  loggerAtt.info("Which week day it is: "+str(weekDay))
  
  temp_table_name = "raw_df"
  raw_df.createOrReplaceTempView(temp_table_name)
  
  initialCount = raw_df.count()
  loggerAtt.info("No of records for in raw_df: "+str(initialCount))
  ABC(rawMasterInitCount=initialCount)
  
  if weekDay == 0:
    raw_df = spark.sql('''select raw_df.* from raw_df
                                  LEFT ANTI JOIN (select * from delta.`{}` 
                                                  where DATE(LAST_UPDATE_TIMESTAMP) = current_date()) as itemMasterTemp
                                  On raw_df.SCRTX_DET_ITM_ID = itemMasterTemp.SMA_GTIN_NUM
                                    and raw_df.RTX_STORE = itemMasterTemp.SMA_STORE'''.format(itemMasterDeltaPath))
    
    finalCount = raw_df.count()
    loggerAtt.info("No of records for in raw_df: "+str(finalCount))
    ABC(rawMasterFinalCount=finalCount)
    
  elif weekDay == 1:
    raw_df = spark.sql('''select raw_df.* from raw_df
                                  LEFT ANTI JOIN (select * from delta.`{}` 
                                                  where DATE(LAST_UPDATE_TIMESTAMP) >= current_date() - 1) as itemMasterTemp
                                  On RTX_UPC = SMA_GTIN_NUM
                                    and RTX_STORE = SMA_STORE'''.format(itemMasterDeltaPath))
    
    finalCount = raw_df.count()
    loggerAtt.info("No of records for in raw_df: "+str(finalCount))
    ABC(rawMasterFinalCount=finalCount)
  else:
    finalCount = raw_df.count()
  return raw_df

# COMMAND ----------

def storeCompareTransformation(df, pipelineid):
  global final_df
  global invalidRecords
  global TransformationCheck
  
   #1. Setting DF with empth schema
  invalidRecords = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema=posStoreCompareRaw_schema)
  invalidRecords = df.filter(typeCheckUDF(col('SCRTX_DET_ITM_ID')) == False)
  df = df.filter(typeCheckUDF(col('SCRTX_DET_ITM_ID')) == True)
  
  # CAST operation
  df = rawDataTypeCast(df)
  #Derive New Column
  df = df.withColumn('RTX_UPC', col('RTX_UPC')*10)
  df = df.withColumn('SCRTX_DET_ITM_ID', col('SCRTX_DET_ITM_ID')*10)
  df = df.withColumn('RTX_STORE', lpad(col('RTX_STORE').cast(IntegerType()),4,'0'))
  df = df.withColumn('COUPON_NO', lit(None))
  df = df.withColumn('RowNumber', lit(0).cast(LongType()))
  df = df.withColumn("INSERT_ID",lit(pipelineid))
  df = df.withColumn("INSERT_TIMESTAMP",current_timestamp())
  df = df.withColumn("LAST_UPDATE_ID",lit(pipelineid))
  df = df.withColumn("LAST_UPDATE_TIMESTAMP",current_timestamp())
  df = df.withColumn("SCRTX_HDR_ACT_DATE", to_date(current_date()).cast(DateType()))
  df = df.withColumn("SCRTX_DET_TAX_RATE1_FG", flagChangeUDF(col("SCRTX_DET_TAX_RATE1_FG")))
  df = df.withColumn("SCRTX_DET_TAX_RATE2_FG", flagChangeUDF(col("SCRTX_DET_TAX_RATE2_FG")))
  df = df.withColumn("SCRTX_DET_TAX_RATE3_FG", flagChangeUDF(col("SCRTX_DET_TAX_RATE3_FG")))
  df = df.withColumn("SCRTX_DET_TAX_RATE4_FG", flagChangeUDF(col("SCRTX_DET_TAX_RATE4_FG")))
  df = df.withColumn("SCRTX_DET_TAX_RATE5_FG", flagChangeUDF(col("SCRTX_DET_TAX_RATE5_FG")))
  df = df.withColumn("SCRTX_DET_TAX_RATE6_FG", flagChangeUDF(col("SCRTX_DET_TAX_RATE6_FG")))
  df = df.withColumn("SCRTX_DET_TAX_RATE7_FG", flagChangeUDF(col("SCRTX_DET_TAX_RATE7_FG")))
  df = df.withColumn("SCRTX_DET_TAX_RATE8_FG", flagChangeUDF(col("SCRTX_DET_TAX_RATE8_FG")))
  df = df.withColumn("SCRTX_DET_TAX_RATE8_FG", flagChangeUDF(col("SCRTX_DET_TAX_RATE8_FG")))
  df = df.withColumn("SCRTX_DET_WGT_ITM_FG", flagChangeUDF(col("SCRTX_DET_WGT_ITM_FG")))
  df = df.withColumn("SCRTX_DET_FOOD_STAMP_FG", flagChangeUDF(col("SCRTX_DET_FOOD_STAMP_FG")))
  df = df.withColumn("SCRTX_DET_RX_FG", fsaFlagChangeUDF(col("SCRTX_DET_RX_FG")).cast(IntegerType())) 
  df = df.withColumn("SCRTX_HDR_DESC", lit("Store Compare").cast(StringType()))
  df = df.withColumn("SCRTX_DET_NON_RX_HEALTH_FG", fsaFlagChangeUDF(col("SCRTX_DET_NON_RX_HEALTH_FG")).cast(IntegerType()))
  df.createOrReplaceTempView("StoreCompareTable")

  
  final_df = spark.sql("""
  SELECT 
    RTX_STORE
    ,BANNER_ID
    ,COUPON_NO
    ,RTX_BATCH
    ,RTX_TYPE
    ,RTX_UPC
    ,RTX_LOAD
    ,SCRTX_DET_PLU_BTCH_NBR
    ,SCRTX_DET_OP_CODE
    ,SCRTX_DET_ITM_ID
    ,SCRTX_DET_STR_HIER_ID
    ,SCRTX_DET_DFLT_RTN_LOC_ID
    ,SCRTX_DET_MSG_CD
    ,SCRTX_DET_DSPL_DESCR
    ,SCRTX_DET_SLS_RESTRICT_GRP
    ,SCRTX_DET_RCPT_DESCR
    ,SCRTX_DET_TAXABILITY_CD
    ,SCRTX_DET_MDSE_XREF_ID
    ,SCRTX_DET_NON_MDSE_ID
    ,SCRTX_DET_UOM
    ,SCRTX_DET_UNT_QTY
    ,SCRTX_DET_LIN_ITM_CD
    ,SCRTX_DET_MD_FG
    ,SCRTX_DET_QTY_RQRD_FG
    ,SCRTX_DET_SUBPRD_CNT
    ,SCRTX_DET_QTY_ALLOWED_FG
    ,SCRTX_DET_SLS_AUTH_FG
    ,SCRTX_DET_FOOD_STAMP_FG 
    ,SCRTX_DET_WIC_FG
    ,SCRTX_DET_PERPET_INV_FG
    ,SCRTX_DET_RTL_PRC
    ,SCRTX_HDR_ACT_DATE
    ,SCRTX_DET_UNT_CST
    ,SCRTX_DET_MAN_PRC_LVL
    ,SCRTX_DET_MIN_MDSE_AMT
    ,SCRTX_DET_RTL_PRC_DATE
    ,SCRTX_DET_SERIAL_MDSE_FG
    ,SCRTX_DET_CNTR_PRC
    ,SCRTX_DET_MAX_MDSE_AMT
    ,SCRTX_DET_CNTR_PRC_DATE
    ,SCRTX_DET_NG_ENTRY_FG
    ,SCRTX_DET_STR_CPN_FG
    ,SCRTX_DET_VEN_CPN_FG
    ,SCRTX_DET_MAN_PRC_FG
    ,SCRTX_DET_WGT_ITM_FG
    ,SCRTX_DET_NON_DISC_FG
    ,SCRTX_DET_COST_PLUS_FG
    ,SCRTX_DET_PRC_VRFY_FG
    ,SCRTX_DET_PRC_OVRD_FG
    ,SCRTX_DET_SPLR_PROM_FG
    ,SCRTX_DET_SAVE_DISC_FG
    ,SCRTX_DET_ITM_ONSALE_FG
    ,SCRTX_DET_INHBT_QTY_FG
    ,SCRTX_DET_DCML_QTY_FG
    ,SCRTX_DET_SHELF_LBL_RQRD_FG
    ,SCRTX_DET_TAX_RATE1_FG
    ,SCRTX_DET_TAX_RATE2_FG
    ,SCRTX_DET_TAX_RATE3_FG
    ,SCRTX_DET_TAX_RATE4_FG
    ,SCRTX_DET_TAX_RATE5_FG
    ,SCRTX_DET_TAX_RATE6_FG
    ,SCRTX_DET_TAX_RATE7_FG
    ,SCRTX_DET_TAX_RATE8_FG
    ,SCRTX_DET_COST_CASE_PRC
    ,SCRTX_DET_DATE_COST_CASE_PRC
    ,SCRTX_DET_UNIT_CASE
    ,SCRTX_DET_MIX_MATCH_CD
    ,SCRTX_DET_RTN_CD
    ,SCRTX_DET_FAMILY_CD
    ,SCRTX_DET_SUBDEP_ID
    ,SCRTX_DET_DISC_CD
    ,SCRTX_DET_LBL_QTY
    ,SCRTX_DET_SCALE_FG
    ,SCRTX_DET_LOCAL_DEL_FG
    ,SCRTX_DET_HOST_DEL_FG
    ,SCRTX_DET_HEAD_OFFICE_DEP
    ,SCRTX_DET_WGT_SCALE_FG
    ,SCRTX_DET_FREQ_SHOP_TYPE
    ,SCRTX_DET_FREQ_SHOP_VAL
    ,SCRTX_DET_SEC_FAMILY
    ,SCRTX_DET_POS_MSG
    ,SCRTX_DET_SHELF_LIFE_DAY
    ,SCRTX_DET_PROM_NBR
    ,SCRTX_DET_BCKT_NBR
    ,SCRTX_DET_EXTND_PROM_NBR
    ,SCRTX_DET_EXTND_BCKT_NBR
    ,SCRTX_DET_RCPT_DESCR1
    ,SCRTX_DET_RCPT_DESCR2
    ,SCRTX_DET_RCPT_DESCR3
    ,SCRTX_DET_RCPT_DESCR4
    ,SCRTX_DET_TAR_WGT_NBR
    ,SCRTX_DET_RSTRCT_LAYOUT
    ,SCRTX_DET_INTRNL_ID
    ,SCRTX_DET_OLD_PRC
    ,SCRTX_DET_QDX_FREQ_SHOP_VAL
    ,SCRTX_DET_VND_ID
    ,SCRTX_DET_VND_ITM_ID
    ,SCRTX_DET_VND_ITM_SZ
    ,SCRTX_DET_CMPRTV_UOM
    ,SCRTX_DET_CMPR_QTY
    ,SCRTX_DET_CMPR_UNT
    ,SCRTX_DET_BNS_CPN_FG
    ,SCRTX_DET_EX_MIN_PURCH_FG
    ,SCRTX_DET_FUEL_FG
    ,SCRTX_DET_SPR_AUTH_RQRD_FG
    ,SCRTX_DET_SSP_PRDCT_FG
    ,SCRTX_DET_NU06_FG
    ,SCRTX_DET_NU07_FG
    ,SCRTX_DET_NU08_FG
    ,SCRTX_DET_NU09_FG
    ,SCRTX_DET_NU10_FG
    ,SCRTX_DET_FREQ_SHOP_LMT
    ,SCRTX_DET_ITM_STATUS         
    ,SCRTX_DET_DEA_GRP
    ,SCRTX_DET_BNS_BY_OPCODE
    ,SCRTX_DET_BNS_BY_DESCR
    ,SCRTX_DET_COMP_TYPE
    ,SCRTX_DET_COMP_PRC
    ,SCRTX_DET_COMP_QTY
    ,SCRTX_DET_ASSUME_QTY_FG
    ,SCRTX_DET_EXCISE_TAX_NBR
    ,SCRTX_DET_RTL_PRICE_DATE
    ,SCRTX_DET_PRC_RSN_ID
    ,SCRTX_DET_ITM_POINT
    ,SCRTX_DET_PRC_GRP_ID
    ,SCRTX_DET_SWW_CODE_FG
    ,SCRTX_DET_SHELF_STOCK_FG
    ,SCRTX_DET_PRT_PLUID_RCPT_FG
    ,SCRTX_DET_BLK_GRP
    ,SCRTX_DET_EXCHNGE_TENDER_ID
    ,SCRTX_DET_CAR_WASH_FG
    ,SCRTX_DET_EXMPT_FRM_PROM_FG
    ,SCRTX_DET_QSR_ITM_TYP
    ,SCRTX_DET_RSTRCSALE_BRCD_FG
    ,SCRTX_DET_NON_RX_HEALTH_FG
    ,SCRTX_DET_RX_FG
    ,SCRTX_DET_LNK_NBR
    ,SCRTX_DET_WIC_CVV_FG
    ,SCRTX_DET_CENTRAL_ITEM
    ,SCRTX_HDR_DESC
    ,RowNumber
    ,INSERT_ID
    ,INSERT_TIMESTAMP
    ,LAST_UPDATE_ID
    ,LAST_UPDATE_TIMESTAMP 
    FROM StoreCompareTable AS SC
     Inner JOIN store AS STORE  ON
        STORE.Store_NUMBER=SC.RTX_STORE """)
#display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SQL Functions

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Insert into Item Main Delta table

# COMMAND ----------

def insertintoDelta(df,deltapath):
  global initial_recs
  global appended_recs
  loggerAtt.info("Merge into Delta table initiated")
  
  if df is not None:
    temp_table_name = "df_data_delta_to_insert"
    df.createOrReplaceTempView(temp_table_name)
    initial_recs = spark.sql("""SELECT count(*) as count from Item_Main;""")
    print(f"Initial count of records in Delta Table: {initial_recs.head(1)}")
    initial_recs = initial_recs.head(1)

    df.write.format('delta').mode('append').save(deltapath)

    appended_recs = spark.sql("""SELECT count(*) as count from Item_Main""")
    print(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
    appended_recs = appended_recs.head(1)
  loggerAtt.info("Insert into Delta table StoreCompareDelta successful")    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Archiving StoreCompare records

# COMMAND ----------

# If Deltatable.Storeid == FinalDf.Storeid then take Deltatable Matching records & append into Archival path as Parquet format else Skip.
def storeCompare_Archival(deltapath,Date,archivalfilelocation,storeidList):
  global DeltaTableArchive

  storeCompareArchivalDf = spark.read.format('delta').load(deltapath)
  storeCompareArchivalDf = storeCompareArchivalDf.filter(col("RTX_STORE").isin(storeidList)) 
  
  if storeCompareArchivalDf.count() > 0:
    storeCompareArchivalDf.write.mode('Append').format('parquet').save(archivalfilelocation + "/" +Date+ "/" +"StoreCompare_Archival_Records")
    deltaTable = DeltaTable.forPath(spark, deltapath)
    deltaTable.delete(col("RTX_STORE").isin(storeidList))
    loggerAtt.info('========StoreCompare Records Archival successful ========')
  else:
    
    loggerAtt.info('======== No StoreCompare Records Archival Done ========')

    #   datediff(to_date(current_date()).to_date(date_format(date_func(col('LAST_UPDATE_TIMESTAMP')),'yyyy-MM-dd')))>1)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Fetching all Bottle Deposit Records

# COMMAND ----------

def fetchAllBottleDepositRecords(df):
  temp_table_name = "df_bottleDeposit"
  df.createOrReplaceTempView(temp_table_name)
  loggerAtt.info("Fetch and ,PRICE_MLT_Quantity  from Product Delta table initiated")
  bottleDepositDf =  spark.sql('''select RTX_STORE as BOTTLE_DEPOSIT_STORE,
                                   substr(SCRTX_DET_ITM_ID,2,5) as BOTTLE_DEPOSIT_LNK_NBR ,
                                   SCRTX_DET_ITM_ID as BOTTLE_DEPOSIT_ITM_ID,
                                   SCRTX_DET_RTL_PRC as BOTTLE_DEPOSIT_RTL_PRC 
                                   from df_bottleDeposit 
                                  where 
                                   SCRTX_DET_ITM_ID between 400000 and 409999
                                  and SCRTX_DET_FOOD_STAMP_FG = 'Y'
                            union
                            select RTX_STORE as BOTTLE_DEPOSIT_STORE,
                                   substr(SCRTX_DET_ITM_ID,2,5)  as BOTTLE_DEPOSIT_LNK_NBR,
                                   SCRTX_DET_ITM_ID as BOTTLE_DEPOSIT_ITM_ID,
                                   SCRTX_DET_RTL_PRC as BOTTLE_DEPOSIT_RTL_PRC
                                   from df_bottleDeposit 
                                  where 
                                   SCRTX_DET_ITM_ID between 410000 and 419999
                                  and SCRTX_DET_FOOD_STAMP_FG != 'Y'
                                  ''')  
  loggerAtt.info("Fetch Fetch PRICE_PROMO_RTL_PRC and ,PRICE_MLT_Quantity  from Product Delta table end") 
  return bottleDepositDf

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Delete bottle deposit records

# COMMAND ----------

# If Deltatable.Storeid == FinalDf.Storeid then take Deltatable Matching records & append into Archival path as Parquet format else Skip.
def bottleDepositArchival(BottleDepositpath,storeidList):
  bottleDepositDf = spark.read.format('delta').load(BottleDepositpath)
  if bottleDepositDf.count() > 0:
    bottleDepositDf = bottleDepositDf.filter(col("BOTTLE_DEPOSIT_STORE").isin(storeidList)) 
    loggerAtt.info('Bottle Deposit Count for store list'+str(bottleDepositDf.count()))
    if bottleDepositDf.count() > 0:
      deltaTable = DeltaTable.forPath(spark, BottleDepositpath)
      deltaTable.delete(col("BOTTLE_DEPOSIT_STORE").isin(storeidList))
      loggerAtt.info('========BottleDeposit Records Deletion successful ========')
  else:
    loggerAtt.info('======== No BottleDeposit Records Deletion Done ========')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Insert into Bottle Deposit Table

# COMMAND ----------

def insertIntoBottleDepositDelta(bottleDepositDf,BottleDepositpath):
  loggerAtt.info("Insert into Bottle Deposit Delta table initiated")
  
  if bottleDepositDf is not None:
    initial_recs = spark.sql("""SELECT count(*) as count from bottleDeposit;""")
    loggerAtt.info(f"Initial count of records in Delta Table: {initial_recs.head(1)}")
    initial_recs = initial_recs.head(1)

    bottleDepositDf.write.format('delta').mode('append').save(BottleDepositpath)

    appended_recs = spark.sql("""SELECT count(*) as count from bottleDeposit""")
    loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
    appended_recs = appended_recs.head(1)
  loggerAtt.info("Insert into Bottle Deposit Delta table  successful")    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Store Compare Output

# COMMAND ----------

def itemMainWrite(itemMainDeltaPath,storeCompareOutput):
  itemMainOutputDf = spark.read.format('delta').load(itemMainDeltaPath)
  ABC(storeCompareOutputCount=itemMainOutputDf.count())
  if itemMainOutputDf.count() >0: 
    
    itemMainOutputDf.write.partitionBy('BANNER_ID', 'RTX_STORE').mode('overwrite').format('parquet').save(storeCompareOutput)
    
    loggerAtt.info('========Store Compare Records Output successful ========')
  else:
    loggerAtt.info('======== No Store Compare Records Output Done ========')

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Main Function  

# COMMAND ----------

if __name__ == "__main__":
  loggerAtt.info('======== Input Delhaize Store Compare Feed processing initiated ========')
   # Step 1: Reading File
  ##### Step 1.a: Reading Params
  file_type = "com.databricks.spark.csv"
  infer_schema = "True"
  header = "False"
  delimiter = "|"
  schema = inputDataSchema
  file_location = str(file_location)

  raw_df              = None
  final_df            = None
  pipelineid          = str(PipelineID)
  Archivalfilelocation= str(Archivalfilelocation)
  deltapath           = str(DeltaPath)
  
  
  loggerAtt.info("------------------Executing Read Function------------------------------------\n")
  
  ##### Step 1.b: Executing Read Function 
  
  try:
    readInputFile(file_location, infer_schema, header, delimiter, file_type, schema)
    ABC(ReadDataCheck=1)
    RawDataCount = raw_df.count()
    ABC(RawDataCount=RawDataCount)
    loggerAtt.info("Raw count check initiated")
    loggerAtt.info(f"Count of Records in the File: {RawDataCount}")
  except Exception as ex:
    ABC(ReadDataCheck=0)
    ABC(RawDataCount='')
    if 'java.io.FileNotFoundException' in str(ex):
      loggerAtt.error('File does not exists')
    else:
      loggerAtt.error(ex)
      err = ErrorReturn('Error', ex,'readFile')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
#       err.exit()

  try:
    ## Fetching Store Detail record
    ABC(fetchStoreDetailCheck=1)
    storeDf = fetchStoreDetail()
  except:
    NullValueCheck = 0
    NullValuCnt=''
    ABC(fetchStoreDetailCheck=0) 
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'fetchStoreDetail')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
  loggerAtt.info("----------------------Unit Tesing--------------------------------\n")
  # Step 2: Transformation
  ##### Step 2.a: Removing Duplicate Records
  
  try:
    ## Fetching problem records having null rows for all columns
    storeCompare_nullRows = raw_df.where(reduce(lambda x, y: x | y, (col(x).isNull() for x in raw_df.columns)))
    ABC(NullValueCheck=1)
    raw_df = raw_df.na.drop()
    ABC(DropNACheck = 1)
    NullValuCnt = storeCompare_nullRows.count()
    ABC(NullValuCount = NullValuCnt)
  except:
    NullValueCheck = 0
    NullValuCnt=''
    ABC(NullValueCheck=0)
    ABC(NullValuCount = NullValuCnt)  
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'NullRecordHandling')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
  try:
    ABC(DuplicateValueCheck = 1)
    if (raw_df.groupBy('RTX_Store','RTX_BATCH', 'RTX_UPC').count().filter("count > 1").count()) > 0:
      storeCompare_ProblemRecs = raw_df.groupBy('RTX_Store','RTX_BATCH','RTX_UPC').count().filter(col('count') > 1)
      storeCompare_ProblemRecs = storeCompare_ProblemRecs.drop(storeCompare_ProblemRecs['count'])
      storeCompare_ProblemRecs = (raw_df.join(storeCompare_ProblemRecs,["RTX_Store","RTX_BATCH","RTX_UPC"], "leftsemi"))
      DuplicateValueCnt = price_ProblemRecs.count()
      ABC(DuplicateValueCount=DuplicateValueCnt)
      raw_df = (raw_df.join(storeCompare_ProblemRecs,["RTX_Store","RTX_BATCH","RTX_UPC"], "leftanti"))
    else:
      storeCompare_ProblemRecs = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema=posStoreCompareRaw_schema)
      ABC(DuplicateValueCount=0)
      loggerAtt.info(f"No PROBLEM RECORDS")
  except Exception as ex:
    ABC(DuplicateValueCheck = 0)
    ABC(DuplicateValueCount ='')
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'StoreCompare_ProblemRecs')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
        
  loggerAtt.info("--------------------Store Compare Transformation----------------------------------\n")
  try:
    storeCompareTransformation(raw_df, pipelineid=str(PipelineID))
    ABC(TransformationCheck = 1)
  except Exception as ex:
    ABC(TransformationCheck = 0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'storeCompareTransformation')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))  
    
  loggerAtt.info("--------------------Store Compare Removing Current modified item records----------------------------------\n")
  try:
    temp_df_1 = final_df
    final_df = removeItemCheck(final_df, itemMasterDeltaPath)
    temp_df = final_df
    ABC(RemoveItemCheck = 1)
  except Exception as ex:
    ABC(RemoveItemCheck = 0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'RemoveItemCheck')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))    
  
  loggerAtt.info("-----------------------Creating Delta Table-------------------------------\n")	
  try:
    ## Use this below Drop and Remove for any Change/Alter in Delta Table Column 
    deltaCreator(deltapath)
    ABC(DeltaTableCreateCheck = 1)
  except Exception as ex:
    ABC(DeltaTableCreateCheck = 0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'StoreCompareCreationMerge')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
#   loggerAtt.info("-----------------------Archive Delta Table-------------------------------\n")
#   try:
#     storeids = [x.RTX_STORE for x in final_df.select('RTX_STORE').distinct().collect()]
#     storeCompare_Archival(str(DeltaPath),Date,Archivalfilelocation,storeids)
#     ABC(DeltaTableArchive = 1)
#   except Exception as ex:
#     ABC(DeltaTableArchive = 0)
#     loggerAtt.error(ex)
#     err = ErrorReturn('Error', ex,'StoreCompareArchive')
#     errJson = jsonpickle.encode(err)
#     errJson = json.loads(errJson)
#     dbutils.notebook.exit(Merge(ABCChecks,errJson))
    
  loggerAtt.info("-----------------------Merge Delta Table-------------------------------\n")
  try:
    #OPTIMIZE StoreCompare
    insertintoDelta(df=final_df,deltapath= str(DeltaPath))
    ABC(DeltaTableMerge = 1)
    ABC(DeltaTableInitCount=initial_recs[0][0])
    ABC(DeltaTableFinalCount=appended_recs[0][0])
  except Exception as ex:
    ABC(DeltaTableMerge = 0)
    ABC(DeltaTableInitCount='')
    ABC(DeltaTableFinalCount='')
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'StoreCompareMerge')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

  loggerAtt.info("-----------------------Store Compare Output-------------------------------\n")
  try:
    #OPTIMIZE StoreCompare
    itemMainWrite(str(DeltaPath),storeCompareOutput)
    ABC(itemMainWriteCheck = 1)
  except Exception as ex:
    ABC(itemMainWriteCheck = 0)
    ABC(storeCompareOutputCount='')
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'itemMainWrite')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))    
    
  loggerAtt.info("-----------------------Save Invalid Records-------------------------------\n")
  try:
    storeCompare_ProblemRecs = storeCompare_ProblemRecs.union(storeCompare_nullRows)
    storeCompare_ProblemRecs.write.format('com.databricks.spark.csv').mode('overwrite').option("header", "true").save(Invalid_RecordsPath)
    # Save problem records in Invalid Path
    ABC(InvalidRecordSaveCheck = 1)
    ABC(InvalidRecordCount = storeCompare_ProblemRecs.count())
    loggerAtt.info(f"Invalid Records saved at path: {Invalid_RecordsPath}")
  except Exception as ex:
    ABC(InvalidRecordSaveCheck = 0)
    ABC(InvalidRecordCount='')
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'InvalidRecordsSave')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
  
  
  loggerAtt.info("-----------------------Fetch Bottle Deposit Delta Table-------------------------------\n")
  try:
    bottleDepositDf = fetchAllBottleDepositRecords(final_df)
    ABC(fetchAllBottleDepositCheck = 1)
  except Exception as ex:
    ABC(fetchAllBottleDepositCheck = 0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'fetchAllBottleDepositRecords')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
  loggerAtt.info("-----------------------Delete Bottle Deposit Delta Table-------------------------------\n")
  try:
    storeids = [x.RTX_STORE for x in final_df.select('RTX_STORE').distinct().collect()]
    bottleDepositArchival(BottleDepositpath,storeids)
    ABC(bottleDepositArchival = 1)
  except Exception as ex:
    ABC(bottleDepositArchival = 0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'bottleDepositArchival')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
  loggerAtt.info("-----------------------Insert Bottle Deposit Delta Table-------------------------------\n")
  try:
    insertIntoBottleDepositDelta(bottleDepositDf,BottleDepositpath)
    ABC(insertIntoBottleDepositDelta = 1)
  except Exception as ex:
    ABC(insertIntoBottleDepositDelta = 0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'insertIntoBottleDepositDelta')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
  loggerAtt.info('======== Delhaize Store Compare Feed processing ended ========')

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Writing log file to ADLS location

# COMMAND ----------

dbutils.fs.mv("file:"+p_logfile, Log_FilesPath+"/"+ custom_logfile_Name + file_date + '.log')
loggerAtt.info('======== Log file is updated at ADLS Location ========')
logging.shutdown()
err = ErrorReturn('Success', '','')
errJson = jsonpickle.encode(err)
errJson = json.loads(errJson)
Merge(ABCChecks,errJson)
dbutils.notebook.exit(Merge(ABCChecks,errJson))