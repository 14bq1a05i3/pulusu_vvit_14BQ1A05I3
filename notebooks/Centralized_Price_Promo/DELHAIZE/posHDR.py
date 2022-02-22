# Databricks notebook source
from pyspark.sql.types import * 
from pyspark.sql import *
import json
from pyspark.sql.functions  import *
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
dbutils.widgets.text("directory","")
dbutils.widgets.text("container","")
dbutils.widgets.text("pipelineID","")
dbutils.widgets.text("MountPoint","")
dbutils.widgets.text("deltaPath","")
dbutils.widgets.text("logFilesPath","")
dbutils.widgets.text("invalidRecordsPath","")
dbutils.widgets.text("clientId","")
dbutils.widgets.text("keyVaultName","")
dbutils.widgets.text("storeDeltaPath","")
dbutils.widgets.text("archivalFilePath","")



fileName=dbutils.widgets.get("fileName")
filePath=dbutils.widgets.get("filePath")
directory=dbutils.widgets.get("directory")
container=dbutils.widgets.get("container")
pipelineid=dbutils.widgets.get("pipelineID")
mount_point=dbutils.widgets.get("MountPoint")
itemDeltaPath=dbutils.widgets.get("deltaPath")
storeDeltaPath=dbutils.widgets.get("storeDeltaPath")
logFilesPath=dbutils.widgets.get("logFilesPath")
invalidRecordsPath=dbutils.widgets.get("invalidRecordsPath")
Date = datetime.datetime.now(timezone("America/Halifax")).strftime("%Y-%m-%d")
file_location = '/mnt' + '/' + directory + '/' + filePath +'/' + fileName 
source= 'abfss://' + directory + '@' + container + '.dfs.core.windows.net/'
clientId=dbutils.widgets.get("clientId")
keyVaultName=dbutils.widgets.get("keyVaultName")
itemArchivalfilelocation=dbutils.widgets.get("archivalFilePath")

storeDeltaPath = '/mnt/delhaize-centralized-price-promo/Storedetails/Outbound/SDM/Store_delta'
loggerAtt.info(f"Date : {Date}")
loggerAtt.info(f"File Location on Mount Point : {file_location}")
loggerAtt.info(f"Source or File Location on Container : {source}")

# #file_location = '/mnt' + '/' + Filepath + '/' + Directory +'/' + FileName 
# file_location = '/mnt' + '/' + Directory + '/' + Filepath +'/' + FileName 
# source= "abfss://delhaize-centralized-price-promo@rs06ue2dmasadata02.dfs.core.windows.net/"
# DeltaPath ='/mnt/delhaize-centralized-price-promo/POSdaily/Outbound/SDM/Item_Main_temp'
# pipelineid = "temp"
# itemArchivalfilelocation= '/mnt/delhaize-centralized-price-promo/POSdaily/Outbound/SDM/ArchivalRecords'
# mount_point = "/mnt/delhaize-centralized-price-promo"
# storeDeltaPath = '/mnt/delhaize-centralized-price-promo/Storedetails/Outbound/SDM/Store_delta'
# logFilesPath = "/mnt/delhaize-centralized-price-promo/POSdaily/Outbound/SDM/Logfiles"
# source= "abfss://delhaize-centralized-price-promo@rs06ue2dmasadata02.dfs.core.windows.net/"
# invalidRecordsPath: "/mnt/delhaize-centralized-price-promo/POSdaily/Outbound/SDM/InvalidRecords"
# file_location ="/mnt/delhaize-centralized-price-promo/POSdaily/Inbound/RDS/2021/03/18/"
# itemDeltaPath='/mnt/delhaize-centralized-price-promo/POSdaily/Outbound/SDM/Item_Main'
# clientId="2cbef55f-e5b2-403d-a3c0-430d6f5e83d4"
# keyVaultName="MerchandisingApp-Key-Vault-DEV"
  

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

inputDataSchema = StructType([
                           StructField("Data",StringType(),True),
                           StructField("RowNumber",IntegerType(),False)                           
])

hdrDetailSchema = StructType([
                           StructField("SCRTX_DET_PLU_BTCH_NBR",StringType(),True),
                           StructField("SCRTX_DET_OP_CODE",StringType(),True),
                           StructField("SCRTX_DET_ITM_ID",StringType(),True),
                           StructField("SCRTX_DET_STR_HIER_ID",StringType(),True),
                           StructField("SCRTX_DET_DFLT_RTN_LOC_ID",StringType(),True),
                           StructField("SCRTX_DET_MSG_CD",StringType(),True),
                           StructField("SCRTX_DET_DSPL_DESCR",StringType(),True),
                           StructField("SCRTX_DET_SLS_RESTRICT_GRP",StringType(),True),
                           StructField("SCRTX_DET_RCPT_DESCR",StringType(),True),
                           StructField("SCRTX_DET_TAXABILITY_CD",StringType(),True),
                           StructField("SCRTX_DET_MDSE_XREF_ID",StringType(),True),
                           StructField("SCRTX_DET_NON_MDSE_ID",StringType(),True),
                           StructField("SCRTX_DET_UOM",StringType(),True),
                           StructField("SCRTX_DET_UNT_QTY",StringType(),True),
                           StructField("SCRTX_DET_LIN_ITM_CD",StringType(),True),
                           StructField("SCRTX_DET_MD_FG",StringType(),True),
                           StructField("SCRTX_DET_QTY_RQRD_FG",StringType(),True),
                           StructField("SCRTX_DET_SUBPRD_CNT",StringType(),True),
                           StructField("SCRTX_DET_QTY_ALLOWED_FG",StringType(),True),
                           StructField("SCRTX_DET_SLS_AUTH_FG",StringType(),True),
                           StructField("SCRTX_DET_FOOD_STAMP_FG",StringType(),True),
                           StructField("SCRTX_DET_WIC_FG",StringType(),True),
                           StructField("SCRTX_DET_PERPET_INV_FG",StringType(),True),
                           StructField("SCRTX_DET_RTL_PRC",StringType(),True),
                           StructField("SCRTX_DET_UNT_CST",StringType(),True),
                           StructField("SCRTX_DET_MAN_PRC_LVL",StringType(),True),
                           StructField("SCRTX_DET_MIN_MDSE_AMT",StringType(),True),
                           StructField("SCRTX_DET_RTL_PRC_DATE",StringType(),True),
                           StructField("SCRTX_DET_SERIAL_MDSE_FG",StringType(),True),
                           StructField("SCRTX_DET_CNTR_PRC",StringType(),True),
                           StructField("SCRTX_DET_MAX_MDSE_AMT",StringType(),True),
                           StructField("SCRTX_DET_CNTR_PRC_DATE",StringType(),True),
                           StructField("SCRTX_DET_NG_ENTRY_FG",StringType(),True),
                           StructField("SCRTX_DET_STR_CPN_FG",StringType(),True),
                           StructField("SCRTX_DET_VEN_CPN_FG",StringType(),True),
                           StructField("SCRTX_DET_MAN_PRC_FG",StringType(),True),
                           StructField("SCRTX_DET_WGT_ITM_FG",StringType(),True),
                           StructField("SCRTX_DET_NON_DISC_FG",StringType(),True),
                           StructField("SCRTX_DET_COST_PLUS_FG",StringType(),True),
                           StructField("SCRTX_DET_PRC_VRFY_FG",StringType(),True),
                           StructField("SCRTX_DET_PRC_OVRD_FG",StringType(),True),
                           StructField("SCRTX_DET_SPLR_PROM_FG",StringType(),True),
                           StructField("SCRTX_DET_SAVE_DISC_FG",StringType(),True),
                           StructField("SCRTX_DET_ITM_ONSALE_FG",StringType(),True),
                           StructField("SCRTX_DET_INHBT_QTY_FG",StringType(),True),
                           StructField("SCRTX_DET_DCML_QTY_FG",StringType(),True),
                           StructField("SCRTX_DET_SHELF_LBL_RQRD_FG",StringType(),True),
                           StructField("SCRTX_DET_TAX_RATE1_FG",StringType(),True),
                           StructField("SCRTX_DET_TAX_RATE2_FG",StringType(),True),
                           StructField("SCRTX_DET_TAX_RATE3_FG",StringType(),True),
                           StructField("SCRTX_DET_TAX_RATE4_FG",StringType(),True),
                           StructField("SCRTX_DET_TAX_RATE5_FG",StringType(),True),
                           StructField("SCRTX_DET_TAX_RATE6_FG",StringType(),True),
                           StructField("SCRTX_DET_TAX_RATE7_FG",StringType(),True),
                           StructField("SCRTX_DET_TAX_RATE8_FG",StringType(),True),
                           StructField("SCRTX_DET_COST_CASE_PRC",StringType(),True),
                           StructField("SCRTX_DET_DATE_COST_CASE_PRC",StringType(),True),
                           StructField("SCRTX_DET_UNIT_CASE",StringType(),True),
                           StructField("SCRTX_DET_MIX_MATCH_CD",StringType(),True),
                           StructField("SCRTX_DET_RTN_CD",StringType(),True),
                           StructField("SCRTX_DET_FAMILY_CD",StringType(),True),
                           StructField("SCRTX_DET_SUBDEP_ID",StringType(),True),
                           StructField("SCRTX_DET_DISC_CD",StringType(),True),
                           StructField("SCRTX_DET_LBL_QTY",StringType(),True),
                           StructField("SCRTX_DET_SCALE_FG",StringType(),True),
                           StructField("SCRTX_DET_LOCAL_DEL_FG",StringType(),True),
                           StructField("SCRTX_DET_HOST_DEL_FG",StringType(),True),
                           StructField("SCRTX_DET_HEAD_OFFICE_DEP",StringType(),True),
                           StructField("SCRTX_DET_WGT_SCALE_FG",StringType(),True),
                           StructField("SCRTX_DET_FREQ_SHOP_TYPE",StringType(),True),
                           StructField("SCRTX_DET_FREQ_SHOP_VAL",StringType(),True),
                           StructField("SCRTX_DET_SEC_FAMILY",StringType(),True),
                           StructField("SCRTX_DET_POS_MSG",StringType(),True),
                           StructField("SCRTX_DET_SHELF_LIFE_DAY",StringType(),True),
                           StructField("SCRTX_DET_PROM_NBR",StringType(),True),
                           StructField("SCRTX_DET_BCKT_NBR",StringType(),True),
                           StructField("SCRTX_DET_EXTND_PROM_NBR",StringType(),True),
                           StructField("SCRTX_DET_EXTND_BCKT_NBR",StringType(),True),
                           StructField("SCRTX_DET_RCPT_DESCR1",StringType(),True),
                           StructField("SCRTX_DET_RCPT_DESCR2",StringType(),True),
                           StructField("SCRTX_DET_RCPT_DESCR3",StringType(),True),
                           StructField("SCRTX_DET_RCPT_DESCR4",StringType(),True),
                           StructField("SCRTX_DET_CPN_NBR",StringType(),True),
                           StructField("SCRTX_DET_TAR_WGT_NBR",StringType(),True),
                           StructField("SCRTX_DET_RSTRCT_LAYOUT",StringType(),True),
                           StructField("SCRTX_DET_INTRNL_ID",StringType(),True),
                           StructField("SCRTX_DET_OLD_PRC",StringType(),True),
                           StructField("SCRTX_DET_QDX_FREQ_SHOP_VAL",StringType(),True),
                           StructField("SCRTX_DET_VND_ID",StringType(),True),
                           StructField("SCRTX_DET_VND_ITM_ID",StringType(),True),
                           StructField("SCRTX_DET_VND_ITM_SZ",StringType(),True),
                           StructField("SCRTX_DET_CMPRTV_UOM",StringType(),True),
                           StructField("SCRTX_DET_CMPR_QTY",StringType(),True),
                           StructField("SCRTX_DET_CMPR_UNT",StringType(),True),
                           StructField("SCRTX_DET_BNS_CPN_FG",StringType(),True),
                           StructField("SCRTX_DET_EX_MIN_PURCH_FG",StringType(),True),
                           StructField("SCRTX_DET_FUEL_FG",StringType(),True),
                           StructField("SCRTX_DET_SPR_AUTH_RQRD_FG",StringType(),True),
                           StructField("SCRTX_DET_SSP_PRDCT_FG",StringType(),True),
                           StructField("SCRTX_DET_NU06_FG",StringType(),True),
                           StructField("SCRTX_DET_NU07_FG",StringType(),True),
                           StructField("SCRTX_DET_NU08_FG",StringType(),True),
                           StructField("SCRTX_DET_NU09_FG",StringType(),True),
                           StructField("SCRTX_DET_NU10_FG",StringType(),True),
                           StructField("SCRTX_DET_FREQ_SHOP_LMT",StringType(),True),
                           StructField("SCRTX_DET_ITM_STATUS",StringType(),True),
                           StructField("SCRTX_DET_DEA_GRP",StringType(),True),
                           StructField("SCRTX_DET_BNS_BY_OPCODE",StringType(),True),
                           StructField("SCRTX_DET_BNS_BY_DESCR",StringType(),True),
                           StructField("SCRTX_DET_COMP_TYPE",StringType(),True),
                           StructField("SCRTX_DET_COMP_PRC",StringType(),True),
                           StructField("SCRTX_DET_COMP_QTY",StringType(),True),
                           StructField("SCRTX_DET_ASSUME_QTY_FG",StringType(),True),
                           StructField("SCRTX_DET_EXCISE_TAX_NBR",StringType(),True),
                           StructField("SCRTX_DET_RTL_PRICE_DATE",StringType(),True),
                           StructField("SCRTX_DET_PRC_RSN_ID",StringType(),True),
                           StructField("SCRTX_DET_ITM_POINT",StringType(),True),
                           StructField("SCRTX_DET_PRC_GRP_ID",StringType(),True),
                           StructField("SCRTX_DET_SWW_CODE_FG",StringType(),True),
                           StructField("SCRTX_DET_SHELF_STOCK_FG",StringType(),True),
                           StructField("SCRTX_DET_PRT_PLUID_RCPT_FG",StringType(),True),
                           StructField("SCRTX_DET_BLK_GRP",StringType(),True),
                           StructField("SCRTX_DET_EXCHNGE_TENDER_ID",StringType(),True),
                           StructField("SCRTX_DET_CAR_WASH_FG",StringType(),True),
                           StructField("SCRTX_DET_EXMPT_FRM_PROM_FG",StringType(),True),
                           StructField("SCRTX_DET_QSR_ITM_TYP",StringType(),True),
                           StructField("SCRTX_DET_RSTRCSALE_BRCD_FG",StringType(),True),
                           StructField("SCRTX_DET_NON_RX_HEALTH_FG",StringType(),True),
                           StructField("SCRTX_DET_RX_FG",StringType(),True),
                           StructField("SCRTX_DET_LNK_NBR",StringType(),True),
                           StructField("SCRTX_DET_WIC_CVV_FG",StringType(),True),
                           StructField("SCRTX_DET_CENTRAL_ITEM",StringType(),True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Delta table creation

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from delta.`/mnt/delhaize-centralized-price-promo/POSdaily/Outbound/SDM/Item_Main` as itemMain where RTX_STORE="1417" and RTX_UPC = 43000006640

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
# MAGIC ## Declarations

# COMMAND ----------

headerList = ['SCRTX_HDR_BTCH_NBR','SCRTX_HDR_DESC','SCRTX_HDR_DT_CREATED','SCRTX_HDR_REC_CNT','SCRTX_HDR_ACT_DATE','SCRTX_HDR_ACT_TIME','SCRTX_HDR_STORE_ID','SCRTX_HDR_IMPORT_FILE','SCRTX_HDR_EXPORT_FILE','SCRTX_HDR_BTCH_TYPE','SCRTX_HDR_BTCH_VER','SCRTX_HDR_BTCH_STATUS','SCRTX_HDR_LAST_EXEC','SCRTX_HDR_ITM_PASS_CNT','SCRTX_HDR_END_SALE_DATE','SCRTX_HDR_END_SALE_TIME','SCRTX_HDR_BATCH_OPER','SCRTX_HDR_BATCH_SCOPE','SCRTX_HDR_NEXT_BTCH_NBR','hdrRowNumber','RTX_STORE','hdrEndRowNumber','BANNER_ID']

itemMasterList = ["RTX_STORE", "SCRTX_DET_BLK_GRP", "SCRTX_DET_CENTRAL_ITEM", "SCRTX_DET_COMP_PRC", "SCRTX_DET_COMP_QTY", "SCRTX_DET_COMP_TYPE", "SCRTX_DET_DEA_GRP", "SCRTX_DET_DSPL_DESCR", "SCRTX_DET_FAMILY_CD", "SCRTX_DET_FOOD_STAMP_FG", "SCRTX_DET_FREQ_SHOP_TYPE", "SCRTX_DET_FREQ_SHOP_VAL", "SCRTX_DET_INTRNL_ID", "SCRTX_DET_ITM_ID", "SCRTX_DET_LNK_NBR", "SCRTX_DET_MAN_PRC_FG", "SCRTX_DET_MIX_MATCH_CD", "SCRTX_DET_NG_ENTRY_FG", "SCRTX_DET_NON_MDSE_ID", "SCRTX_DET_NON_RX_HEALTH_FG", "SCRTX_DET_OP_CODE", "SCRTX_DET_PLU_BTCH_NBR", "SCRTX_DET_QTY_RQRD_FG", "SCRTX_DET_RCPT_DESCR", "SCRTX_DET_RSTRCSALE_BRCD_FG", "SCRTX_DET_RTL_PRC", "SCRTX_DET_RX_FG", "SCRTX_DET_SEC_FAMILY", "SCRTX_DET_SLS_RESTRICT_GRP", "SCRTX_DET_STR_CPN_FG", "SCRTX_DET_STR_HIER_ID", "SCRTX_DET_SUBDEP_ID", "SCRTX_DET_TAR_WGT_NBR", "SCRTX_DET_TAX_RATE1_FG", "SCRTX_DET_TAX_RATE2_FG", "SCRTX_DET_TAX_RATE3_FG", "SCRTX_DET_TAX_RATE4_FG", "SCRTX_DET_TAX_RATE5_FG", "SCRTX_DET_TAX_RATE6_FG", "SCRTX_DET_TAX_RATE7_FG", "SCRTX_DET_TAX_RATE8_FG", "SCRTX_DET_UNT_QTY", "SCRTX_DET_VEN_CPN_FG", "SCRTX_DET_VND_ID", "SCRTX_DET_WGT_ITM_FG", "SCRTX_DET_WIC_CVV_FG", "SCRTX_DET_WIC_FG", "SCRTX_HDR_ACT_DATE", "COUPON_NO", "BANNER_ID"]

itemList = ["RTX_STORE", "BANNER_ID", "COUPON_NO", "RTX_BATCH", "RTX_TYPE", "RTX_UPC", "RTX_LOAD", "SCRTX_DET_PLU_BTCH_NBR", "SCRTX_DET_OP_CODE", "SCRTX_DET_ITM_ID", "SCRTX_DET_STR_HIER_ID", "SCRTX_DET_DFLT_RTN_LOC_ID", "SCRTX_DET_MSG_CD", "SCRTX_DET_DSPL_DESCR", "SCRTX_DET_SLS_RESTRICT_GRP", "SCRTX_DET_RCPT_DESCR", "SCRTX_DET_TAXABILITY_CD", "SCRTX_DET_MDSE_XREF_ID", "SCRTX_DET_NON_MDSE_ID", "SCRTX_DET_UOM", "SCRTX_DET_UNT_QTY", "SCRTX_DET_LIN_ITM_CD", "SCRTX_DET_MD_FG", "SCRTX_DET_QTY_RQRD_FG", "SCRTX_DET_SUBPRD_CNT", "SCRTX_DET_QTY_ALLOWED_FG", "SCRTX_DET_SLS_AUTH_FG", "SCRTX_DET_FOOD_STAMP_FG", "SCRTX_DET_WIC_FG", "SCRTX_DET_PERPET_INV_FG", "SCRTX_DET_RTL_PRC", "SCRTX_HDR_ACT_DATE", "SCRTX_DET_UNT_CST", "SCRTX_DET_MAN_PRC_LVL", "SCRTX_DET_MIN_MDSE_AMT", "SCRTX_DET_RTL_PRC_DATE", "SCRTX_DET_SERIAL_MDSE_FG", "SCRTX_DET_CNTR_PRC", "SCRTX_DET_MAX_MDSE_AMT", "SCRTX_DET_CNTR_PRC_DATE", "SCRTX_DET_NG_ENTRY_FG", "SCRTX_DET_STR_CPN_FG", "SCRTX_DET_VEN_CPN_FG", "SCRTX_DET_MAN_PRC_FG", "SCRTX_DET_WGT_ITM_FG", "SCRTX_DET_NON_DISC_FG", "SCRTX_DET_COST_PLUS_FG", "SCRTX_DET_PRC_VRFY_FG", "SCRTX_DET_PRC_OVRD_FG", "SCRTX_DET_SPLR_PROM_FG", "SCRTX_DET_SAVE_DISC_FG", "SCRTX_DET_ITM_ONSALE_FG", "SCRTX_DET_INHBT_QTY_FG", "SCRTX_DET_DCML_QTY_FG", "SCRTX_DET_SHELF_LBL_RQRD_FG", "SCRTX_DET_TAX_RATE1_FG", "SCRTX_DET_TAX_RATE2_FG", "SCRTX_DET_TAX_RATE3_FG", "SCRTX_DET_TAX_RATE4_FG", "SCRTX_DET_TAX_RATE5_FG", "SCRTX_DET_TAX_RATE6_FG", "SCRTX_DET_TAX_RATE7_FG", "SCRTX_DET_TAX_RATE8_FG", "SCRTX_DET_COST_CASE_PRC", "SCRTX_DET_DATE_COST_CASE_PRC", "SCRTX_DET_UNIT_CASE", "SCRTX_DET_MIX_MATCH_CD", "SCRTX_DET_RTN_CD", "SCRTX_DET_FAMILY_CD", "SCRTX_DET_SUBDEP_ID", "SCRTX_DET_DISC_CD", "SCRTX_DET_LBL_QTY", "SCRTX_DET_SCALE_FG", "SCRTX_DET_LOCAL_DEL_FG", "SCRTX_DET_HOST_DEL_FG", "SCRTX_DET_HEAD_OFFICE_DEP", "SCRTX_DET_WGT_SCALE_FG", "SCRTX_DET_FREQ_SHOP_TYPE", "SCRTX_DET_FREQ_SHOP_VAL", "SCRTX_DET_SEC_FAMILY", "SCRTX_DET_POS_MSG", "SCRTX_DET_SHELF_LIFE_DAY", "SCRTX_DET_PROM_NBR", "SCRTX_DET_BCKT_NBR", "SCRTX_DET_EXTND_PROM_NBR", "SCRTX_DET_EXTND_BCKT_NBR", "SCRTX_DET_RCPT_DESCR1", "SCRTX_DET_RCPT_DESCR2", "SCRTX_DET_RCPT_DESCR3", "SCRTX_DET_RCPT_DESCR4", "SCRTX_DET_TAR_WGT_NBR", "SCRTX_DET_RSTRCT_LAYOUT", "SCRTX_DET_INTRNL_ID", "SCRTX_DET_OLD_PRC", "SCRTX_DET_QDX_FREQ_SHOP_VAL", "SCRTX_DET_VND_ID", "SCRTX_DET_VND_ITM_ID", "SCRTX_DET_VND_ITM_SZ", "SCRTX_DET_CMPRTV_UOM", "SCRTX_DET_CMPR_QTY", "SCRTX_DET_CMPR_UNT", "SCRTX_DET_BNS_CPN_FG", "SCRTX_DET_EX_MIN_PURCH_FG", "SCRTX_DET_FUEL_FG", "SCRTX_DET_SPR_AUTH_RQRD_FG", "SCRTX_DET_SSP_PRDCT_FG", "SCRTX_DET_NU06_FG", "SCRTX_DET_NU07_FG", "SCRTX_DET_NU08_FG", "SCRTX_DET_NU09_FG", "SCRTX_DET_NU10_FG", "SCRTX_DET_FREQ_SHOP_LMT", "SCRTX_DET_ITM_STATUS", "SCRTX_DET_DEA_GRP", "SCRTX_DET_BNS_BY_OPCODE", "SCRTX_DET_BNS_BY_DESCR", "SCRTX_DET_COMP_TYPE", "SCRTX_DET_COMP_PRC", "SCRTX_DET_COMP_QTY", "SCRTX_DET_ASSUME_QTY_FG", "SCRTX_DET_EXCISE_TAX_NBR", "SCRTX_DET_RTL_PRICE_DATE", "SCRTX_DET_PRC_RSN_ID", "SCRTX_DET_ITM_POINT", "SCRTX_DET_PRC_GRP_ID", "SCRTX_DET_SWW_CODE_FG", "SCRTX_DET_SHELF_STOCK_FG", "SCRTX_DET_PRT_PLUID_RCPT_FG", "SCRTX_DET_BLK_GRP", "SCRTX_DET_EXCHNGE_TENDER_ID", "SCRTX_DET_CAR_WASH_FG", "SCRTX_DET_EXMPT_FRM_PROM_FG", "SCRTX_DET_QSR_ITM_TYP", "SCRTX_DET_RSTRCSALE_BRCD_FG", "SCRTX_DET_NON_RX_HEALTH_FG", "SCRTX_DET_RX_FG", "SCRTX_DET_LNK_NBR", "SCRTX_DET_WIC_CVV_FG", "SCRTX_DET_CENTRAL_ITEM", "RowNumber", "INSERT_ID", "INSERT_TIMESTAMP", "LAST_UPDATE_ID", "LAST_UPDATE_TIMESTAMP"]

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
# MAGIC ### Processing functions

# COMMAND ----------

def posheaderflat_promotable(s):
    return posHeaderRenaming[s]
def posheaderflat_change_col_name(s):
    return s in posHeaderRenaming
  
def itemMain_promotable(s):
    return itemMasterRenaming[s]
def itemMain_change_col_name(s):
    return s in itemMasterRenaming
  
def itemEff_promotable(s):
    return itemEffRenaming[s]
def itemEff_change_col_name(s):
    return s in itemEffRenaming

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Fetching start and end row number for Headers

# COMMAND ----------

def fetchHeader(posRawDf):
  ## if the column name changed, change the select statement in the below code
  ABC(fetchHeaderCheck=1)

  rawDataColumn = "Data"
  rowNumberColumn = "RowNumber"
  endRowNumberColumn = "endRowNumber"
  storeColumn = "Store_ID"
  storeHeaderExpr = "^.&EMBCNTRL"
  allHdrExpr = "^/HDR"
  hdrExpr = "^/HDR$"
  hdrPromoExpr = "^/HDR PROMO$"
  hdrLinkExpr = "^/HDR LINK$"
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
#   storeDf = storeDf.withColumn("BANNER_ID", lit("HAN"))
  storeDf = fetchBannerId(storeDf)
  loggerAtt.info("storeDf record count after banner id fetch is " + str(storeDf.count()))
  
  ## Step6: Creating all header df with proper start and end rownumbers for hdr header with store value
  allHeaderDf = allHeaderDf.join(storeDf, [allHeaderDf.RowNumber > storeDf.storeRowNumber, allHeaderDf.RowNumber <= storeDf.storeEndRowNumber], how='inner').select([col(xx) for xx in allHeaderDf.columns]+['Store_ID', 'BANNER_ID'])
  loggerAtt.info("allHeader record count after join is " + str(allHeaderDf.count()))
  
  ## Step7: Creating seperate dataframe for each header: HDR, HDR LINK and HDR PROMO
  hdrHeaderDf = allHeaderDf.filter(posRawDf[rawDataColumn].rlike(hdrExpr))
  hdrHeaderDf = hdrHeaderDf.withColumn(rowNumberColumn, col(rowNumberColumn)+1)
  hdrHeaderDf = hdrHeaderDf.selectExpr("RowNumber as hdrRowNumber","Data as hdrData","endRowNumber as hdrEndRowNumber","Store_ID as RTX_STORE", "BANNER_ID")
  ABC(hdrHeaderDfCount=hdrHeaderDf.count())
  loggerAtt.info("hdrHeaderDf record count is " + str(hdrHeaderDf.count()))
  
  return hdrHeaderDf

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Fetching HDR Detail records

# COMMAND ----------

def fetchHdrDetailRecords(posRawDetailDf, hdrHeaderDf):
  rawDataColumn = "Data"
  rowNumberColumn = "RowNumber"
  hdrRowNumber = "hdrRowNumber"
  hdrEndRowNumber = "hdrEndRowNumber"
  detCentralItem = "SCRTX_DET_CENTRAL_ITEM"
  detQtyAllowedFg = "SCRTX_DET_QTY_ALLOWED_FG"
  detPluAllowedNbr = "SCRTX_DET_PLU_BTCH_NBR"
  hdrRecCnt = "SCRTX_HDR_REC_CNT"
  hdrActDate = "SCRTX_HDR_ACT_DATE"
  storeColumn = "RTX_STORE"
  bannerId = 'BANNER_ID'
  headerColumn = ['RTX_STORE', 'SCRTX_HDR_REC_CNT', 'SCRTX_HDR_ACT_DATE', 'SCRTX_HDR_ACT_TIME', 'SCRTX_HDR_END_SALE_DATE', 'SCRTX_HDR_END_SALE_TIME', 'hdrRowNumber', 'hdrEndRowNumber', 'BANNER_ID']
  
  ABC(fetchHdrDetailCheck=1)
  loggerAtt.info("before row number fetch posRawDetailDf count " + str(posRawDetailDf.count()))
  ## Step1: Splitting columns to find row numbers. Split colm=umn based on Pipe delimited
  posRawDetailDf = posRawDetailDf.withColumn(rowNumberColumn, split(col(detCentralItem), "\\|").getItem(1).cast(IntegerType()))

  posRawDetailDf = posRawDetailDf.withColumn(detCentralItem, split(col(detCentralItem), "\\|").getItem(0))

  posRawDetailDf = posRawDetailDf.withColumn(rowNumberColumn, when(col(rowNumberColumn).isNull(), split(col(detQtyAllowedFg), "\\|").getItem(1)).otherwise(col(rowNumberColumn)).cast(IntegerType()))

  posRawDetailDf = posRawDetailDf.withColumn(detQtyAllowedFg, split(col(detQtyAllowedFg), "\\|").getItem(0))

  posRawDetailDf = posRawDetailDf.withColumn(rowNumberColumn, when(col(rowNumberColumn).isNull(), split(col(detPluAllowedNbr), "\\|").getItem(1)).otherwise(col(rowNumberColumn)).cast(IntegerType()))

  posRawDetailDf = posRawDetailDf.withColumn(detPluAllowedNbr, split(col(detPluAllowedNbr), "\\|").getItem(0))
  
  ABC(posRawDetailCount=posRawDetailDf.count())
  
  ## Step2: Fetching header with individual column values, start row number, end row number and store id
  hdrHeaderValuesDf = posRawDetailDf.join(hdrHeaderDf, [hdrHeaderDf.hdrRowNumber == posRawDetailDf.RowNumber], how='inner').select([col(xx) for xx in posRawDetailDf.columns] + [hdrRowNumber, hdrEndRowNumber, storeColumn, bannerId])
  loggerAtt.info("hdrHeaderValuesDf count before transformation" + str(hdrHeaderValuesDf.count()))
  loggerAtt.info("hdrHeaderDf count " + str(hdrHeaderDf.count()))
  ## Step3: Rename header column and select those columns alone
  hdrHeaderValuesDf = quinn.with_some_columns_renamed(posheaderflat_promotable, posheaderflat_change_col_name)(hdrHeaderValuesDf)

  hdrHeaderValuesDf = hdrHeaderValuesDf.select([c for c in hdrHeaderValuesDf.columns if c in headerList])

  hdrHeaderValuesDf = hdrHeaderValuesDf.withColumn(hdrEndRowNumber, (col(hdrRowNumber) + col(hdrRecCnt)).cast(IntegerType()))
  
#   hdrHeaderValuesDf = hdrHeaderValuesDf.withColumn(hdrActDate, when(col(hdrActDate).isNull(), to_date(current_date())).otherwise(to_date(date_format(date_func(col(hdrActDate)), 'yyyy-MM-dd'))))
  hdrHeaderValuesDf = hdrHeaderValuesDf.withColumn(hdrActDate, when(col(hdrActDate).isNotNull(),to_date(col(hdrActDate),"MM/dd/yyyy")).otherwise(to_date(current_date())))
  loggerAtt.info("hdrHeaderValuesDf count after transformation" + str(hdrHeaderValuesDf.count()))
#   hdrHeaderValuesDf = hdrHeaderValuesDf.withColumn(hdrActDate, to_date(current_date()))
  ABC(hdrHeaderValueCount=hdrHeaderValuesDf.count())                                                           
  ## Step 4: Combing all HDR detail records based on HDR header
  loggerAtt.info("before join posRawDetailDf " + str(posRawDetailDf.count()))
  posRawDetailDf = posRawDetailDf.join(hdrHeaderValuesDf, [posRawDetailDf.RowNumber > hdrHeaderValuesDf.hdrRowNumber, posRawDetailDf.RowNumber <= hdrHeaderValuesDf.hdrEndRowNumber], how='inner').select([col(xx) for xx in posRawDetailDf.columns]+headerColumn)
  loggerAtt.info("after join posRawDetailDf " + str(posRawDetailDf.count()))
  
  return posRawDetailDf, hdrHeaderValuesDf

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

# The applied options are for CSV files. For other file types, these will be ignored.
def readFileHdrDetail(file_location, infer_schema, first_row_is_header, delimiter,file_type):
  hdrDetail = spark.read.format(file_type) \
    .option("mode","PERMISSIVE") \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .schema(hdrDetailSchema) \
    .load(file_location)
  ABC(ReadDataCheck=1)
  RawDataCount = hdrDetail.count()
  ABC(RawDataCount=RawDataCount)
  loggerAtt.info("Raw count check initiated for readFileHdrDetail")
  loggerAtt.info(f"Count of Records in the File: {RawDataCount}")
  return hdrDetail

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Transformation and its UDF

# COMMAND ----------

def dateFunc(s):
  if s is not None:
    sDate = datetime.datetime.strptime(str(s), '%m/%d/%Y')
    return sDate
  else:
    return None

date_func =  udf(dateFunc) 



def fetchFirst(s):
  if len(s) != 0:
    return s[0]
  else:
    return None

spark.udf.register("fetchFirstFunction", fetchFirst)

# COMMAND ----------

def hdrDetailTransformation(hdrDetailValueDf):
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
  
  hdrDetailValueDf = hdrDetailValueDf.withColumn("SCRTX_DET_ITM_ID", (col("SCRTX_DET_ITM_ID")*10).cast(LongType()))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("COUPON_NO", lit(None).cast(LongType()))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("RTX_BATCH", lit(None).cast(LongType()))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("RTX_TYPE", lit(None).cast(IntegerType()))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("RTX_LOAD", lit(None).cast(StringType()))
  hdrDetailValueDf = hdrDetailValueDf.withColumn('RowNumber', col('RowNumber').cast(LongType()))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("RTX_UPC", col("SCRTX_DET_ITM_ID"))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("INSERT_ID",lit(pipelineid))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("INSERT_TIMESTAMP",current_timestamp())
  hdrDetailValueDf = hdrDetailValueDf.withColumn("LAST_UPDATE_ID",lit(pipelineid))
  hdrDetailValueDf = hdrDetailValueDf.withColumn("LAST_UPDATE_TIMESTAMP",current_timestamp())
  
  return hdrDetailValueDf
  

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming

# COMMAND ----------

posHeaderRenaming = { "SCRTX_DET_PLU_BTCH_NBR":"SCRTX_HDR_BTCH_NBR",
                  "SCRTX_DET_OP_CODE":"SCRTX_HDR_DESC",
                  "SCRTX_DET_ITM_ID":"SCRTX_HDR_DT_CREATED",
                  "SCRTX_DET_STR_HIER_ID":"SCRTX_HDR_REC_CNT",
                  "SCRTX_DET_DFLT_RTN_LOC_ID":"SCRTX_HDR_ACT_DATE",
                  "SCRTX_DET_MSG_CD":"SCRTX_HDR_ACT_TIME",
                  "SCRTX_DET_DSPL_DESCR":"SCRTX_HDR_STORE_ID",
                  "SCRTX_DET_SLS_RESTRICT_GRP":"SCRTX_HDR_IMPORT_FILE",
                  "SCRTX_DET_RCPT_DESCR":"SCRTX_HDR_EXPORT_FILE",
                  "SCRTX_DET_TAXABILITY_CD":"SCRTX_HDR_BTCH_TYPE",
                  "SCRTX_DET_MDSE_XREF_ID":"SCRTX_HDR_BTCH_VER",
                  "SCRTX_DET_NON_MDSE_ID":"SCRTX_HDR_BTCH_STATUS",
                  "SCRTX_DET_UOM":"SCRTX_HDR_LAST_EXEC",
                  "SCRTX_DET_UNT_QTY":"SCRTX_HDR_ITM_PASS_CNT",
                  "SCRTX_DET_LIN_ITM_CD":"SCRTX_HDR_END_SALE_DATE",
                  "SCRTX_DET_MD_FG":"SCRTX_HDR_END_SALE_TIME",
                  "SCRTX_DET_QTY_RQRD_FG":"SCRTX_HDR_BATCH_OPER",
                  "SCRTX_DET_SUBPRD_CNT":"SCRTX_HDR_BATCH_SCOPE",
                  "SCRTX_DET_QTY_ALLOWED_FG":"SCRTX_HDR_NEXT_BTCH_NBR"
                }


itemMasterRenaming = {  "SCRTX_DET_NON_RX_HEALTH_FG":"SMA_FSA_IND",
                        "SCRTX_HDR_ACT_DATE":"SMA_ITM_EFF",
                        "SCRTX_DET_TAR_WGT_NBR":"SMA_FIXED_TARE_WGT",
                        "SCRTX_DET_WGT_ITM_FG":"SMA_SBW_IND",
                        "SCRTX_DET_STR_HIER_ID":"SMA_SUB_DEPT",
                        "RTX_STORE":"SMA_DEST_STORE",
                        "SCRTX_DET_PLU_BTCH_NBR":"SMA_BATCH_SERIAL_NBR",
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
                        "SCRTX_DET_SLS_RESTRICT_GRP":"SMA_RESTR_CODE",
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
                      "FIRST_SCRTX_HDR_REC_CNT":"SCRTX_HDR_REC_CNT",
                      "FIRST_SCRTX_HDR_ACT_DATE":"SCRTX_HDR_ACT_DATE",
                      "FIRST_SCRTX_HDR_ACT_TIME":"SCRTX_HDR_ACT_TIME",
                      "FIRST_SCRTX_HDR_END_SALE_DATE":"SCRTX_HDR_END_SALE_DATE",
                      "FIRST_SCRTX_HDR_END_SALE_TIME":"SCRTX_HDR_END_SALE_TIME",
                      "FIRST_BANNER_ID":"BANNER_ID",
                      "FIRST_RowNumber":"RowNumber" }

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SQL Table query function

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Fetching Banner id from Store detail table

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
# MAGIC ### Item Main Archival

# COMMAND ----------

def posArchival(itemDeltaPath,Date,itemArchivalfilelocation):
  posArchivalDf = spark.read.format('delta').load(itemDeltaPath)
  
  initial_recs = posArchivalDf.count()
  loggerAtt.info(f"Initial count of records in delta table: {initial_recs}")
  ABC(archivalInitCount=initial_recs)
  
  if posArchivalDf.count() > 0:
    posArchivalDf = posArchivalDf.filter(((col("SCRTX_DET_OP_CODE")== '4') & (datediff(to_date(current_date()),to_date(col('LAST_UPDATE_TIMESTAMP'))) >=1)))
    if posArchivalDf.count() >0:
      posArchivalDf.write.mode('Append').format('parquet').save(itemArchivalfilelocation + "/" +Date+ "/" +"pos_Archival_Records")
      deltaTable = DeltaTable.forPath(spark, itemDeltaPath)
      deltaTable.delete(((col("SCRTX_DET_OP_CODE")== '4') & (datediff(to_date(current_date()),to_date(col('LAST_UPDATE_TIMESTAMP'))) >=1)))

      after_recs = spark.read.format('delta').load(itemDeltaPath).count()
      loggerAtt.info(f"After count of records in delta table: {after_recs}")
      ABC(archivalAfterCount=after_recs)

      loggerAtt.info('========Item Records Archival successful ========')
    else:
      loggerAtt.info('======== No Item Records Archival Done ========')
  else:
    loggerAtt.info('======== No Item Records Archival Done ========')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Merging records with same effective date, store number and upc

# COMMAND ----------

def mergeItemRecords(hdrDetailValueDf):
  ABC(mergeItemRecordsCheck = 1)
  loggerAtt.info("Combining records based on effective date")
  temp_table_name = "hdrDetailValue"
  hdrDetailValueDf.createOrReplaceTempView(temp_table_name)
  initial_recs = hdrDetailValueDf.count()
  loggerAtt.info(f"Initial count of records in Dataframe: {initial_recs}")
  ABC(mergeItemInitCount=initial_recs)
  hdrDetailValueDf = spark.sql('''SELECT 
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
                  fetchFirstFunction(collect_list(SCRTX_HDR_REC_CNT)) as FIRST_SCRTX_HDR_REC_CNT,
                  fetchFirstFunction(collect_list(SCRTX_HDR_ACT_TIME)) as FIRST_SCRTX_HDR_ACT_TIME,
                  fetchFirstFunction(collect_list(SCRTX_HDR_END_SALE_DATE)) as FIRST_SCRTX_HDR_END_SALE_DATE,
                  fetchFirstFunction(collect_list(SCRTX_HDR_END_SALE_TIME)) as FIRST_SCRTX_HDR_END_SALE_TIME,
                  fetchFirstFunction(collect_list(BANNER_ID)) as FIRST_BANNER_ID,
                  RTX_STORE,
                  SCRTX_DET_ITM_ID,
                  SCRTX_HDR_ACT_DATE
                  FROM (select * from hdrDetailValue order by SCRTX_HDR_ACT_DATE asc, RowNumber desc) as hdrOrdered
                  GROUP BY RTX_STORE,SCRTX_DET_ITM_ID, SCRTX_HDR_ACT_DATE''')
  after_recs = hdrDetailValueDf.count()
  loggerAtt.info(f"After count of records in Dataframe: {after_recs}")
  ABC(mergeItemafterCount=after_recs)
  spark.catalog.dropTempView(temp_table_name)
  loggerAtt.info("Combining records based on effective date successful")  
  return hdrDetailValueDf


# fetchFirstFunction(collect_list(SCRTX_DET_CPN_NBR)) as FIRST_SCRTX_DET_CPN_NBR, removed element

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Upsert Item Main records

# COMMAND ----------

def updateInsertItemRecords(hdrDetailValueDf):
  loggerAtt.info("Merge into Delta table initiated for pos item update")
#   temp_table_name = "hdrDetailValue"
#   hdrDetailValueDf.createOrReplaceTempView(temp_table_name)
  
  initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(itemDeltaPath))
  loggerAtt.info(f"Initial count of records in Delta Table: {initial_recs.head(1)}")
  initial_recs = initial_recs.head(1)

  ABC(DeltaTableInitCount=initial_recs[0][0])
  
  hdrDetailValueDf = hdrDetailValueDf.select([col(xx) for xx in itemList])
  
  hdrDetailValueDf.write.partitionBy('RTX_STORE', 'SCRTX_HDR_ACT_DATE').format('delta').mode('append').save(itemDeltaPath)
  
#     WHEN NOT MATCHED THEN INSERT *  '''.format(itemDeltaPath)) 
  
  appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(itemDeltaPath))
  loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
  appended_recs = appended_recs.head(1)
  ABC(DeltaTableFinalCount=appended_recs[0][0])
  
  loggerAtt.info("Merge into Delta table successful")  

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
  
  ## Step 3: Read records based on delimiter ^
  try:
    posRawDetailDf = readFileHdrDetail(file_location, infer_schema, first_row_is_header, delimiterDetail, file_type)
    
  except Exception as ex:
    if 'java.io.FileNotFoundException' in str(ex):
      loggerAtt.error('File does not exists')
    else:
      loggerAtt.error(ex)
    ABC(ReadDataCheck=0)
    ABC(RawDataCount="")
    err = ErrorReturn('Error', ex,'readFileHdrDetail')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

  if posRawDf is not None:
    ## Step 4: Fetching Different Header records with start and end row number
    try:
      hdrHeaderDf = fetchHeader(posRawDf)
    except Exception as ex:
      loggerAtt.error(ex)
      ABC(fetchHeaderCheck=0)
      ABC(allHeaderCount='')
      ABC(storeDfCount='')
      ABC(hdrHeaderDfCount='')
      err = ErrorReturn('Error', ex,'fetchHeader')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
    
    ## Step 5: Fetching HDR detail records from Dataframe
    try:  
      if hdrHeaderDf is not None and posRawDetailDf is not None: 
        hdrDetailValueDf, hdrHeaderDf = fetchHdrDetailRecords(posRawDetailDf, hdrHeaderDf)
    except Exception as ex:
      loggerAtt.error(ex)
      ABC(fetchHdrDetailCheck=0)
      ABC(hdrHeaderValueCount='') 
      ABC(posRawDetailCount='')
      err = ErrorReturn('Error', ex,'fetchHdrDetailRecords')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
    
    ## Step 9: HDR Detail Transformation
    try:  
      if hdrDetailValueDf is not None: 
        ABC(TransformationCheck = 1)
        hdrDetailValueDf = hdrDetailTransformation(hdrDetailValueDf)
    except Exception as ex:
      loggerAtt.error(str(ex))
      ABC(TransformationCheck = 0)
      err = ErrorReturn('Error', ex,'promoTransformation')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))     
    
    ## Step 10: Remove item delete records greater then 1 days and move it to archive
    try:
      ABC(archivalCheck = 1)
      if processing_file =='Delta':
        posArchival(itemDeltaPath,Date,itemArchivalfilelocation)
    except Exception as ex:
      ABC(archivalCheck = 0)
      ABC(archivalInitCount='')
      ABC(archivalAfterCount='')
      err = ErrorReturn('Error', ex,'posArchival')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))     
    
    ## Step 11: Update/Insert records into Item Main Delta Table
    try:
      ABC(DeltaTableCreateCheck = 1)
      updateInsertItemRecords(hdrDetailValueDf)
    except Exception as ex:
      loggerAtt.info("Merge into Delta table failed and throwed error")
      loggerAtt.error(str(ex))
      ABC(DeltaTableCreateCheck = 0)
      ABC(DeltaTableInitCount='')
      ABC(DeltaTableFinalCount='')
      err = ErrorReturn('Error', ex,'updateInsertItemRecords')
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