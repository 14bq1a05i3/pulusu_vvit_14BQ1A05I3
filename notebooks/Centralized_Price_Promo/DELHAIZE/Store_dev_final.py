# Databricks notebook source
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
import re
import jsonpickle
from json import JSONEncoder

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
# MAGIC ## Calling Logger

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/Logging

# COMMAND ----------

custom_logfile_Name ='store_dailymaintainence_customlog'
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
dbutils.widgets.text("inputDirectory","")
dbutils.widgets.text("outputDirectory","")
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
dbutils.widgets.text("itemTempEffDeltaPath","")
dbutils.widgets.text("feedEffDeltaPath","")
dbutils.widgets.text("archivalFilePath","")
dbutils.widgets.text("POSemergencyFlag","")

inputDirectory=dbutils.widgets.get("inputDirectory")
outputDirectory=dbutils.widgets.get("outputDirectory")
Archival_filePath=dbutils.widgets.get("archivalFilePath")
FileName=dbutils.widgets.get("fileName")
Filepath=dbutils.widgets.get("filePath")
Directory=dbutils.widgets.get("directory")
container=dbutils.widgets.get("container")
PipelineID=dbutils.widgets.get("pipelineID")
mount_point=dbutils.widgets.get("MountPoint")
StoreDeltaPath=dbutils.widgets.get("deltaPath")
Log_FilesPath=dbutils.widgets.get("logFilesPath")
Invalid_RecordsPath=dbutils.widgets.get("invalidRecordsPath")
inputSource= 'abfss://' + inputDirectory + '@' + container + '.dfs.core.windows.net/'
outputSource= 'abfss://' + outputDirectory + '@' + container + '.dfs.core.windows.net/'
Date = datetime.datetime.now(timezone("America/Halifax")).strftime("%Y-%m-%d")
file_location = '/mnt' + '/' + Directory + '/' + Filepath +'/' + FileName 
source= 'abfss://' + Directory + '@' + container + '.dfs.core.windows.net/'
clientId=dbutils.widgets.get("clientId")
keyVaultName=dbutils.widgets.get("keyVaultName")
itemTempEffDeltaPath=dbutils.widgets.get("itemTempEffDeltaPath")
storeEffDeltaPath=dbutils.widgets.get("feedEffDeltaPath")
storeOutboundPath= '/mnt' + '/' + outputDirectory + '/Store/Outbound/CDM'
POSemergencyFlag=dbutils.widgets.get("POSemergencyFlag")

unifiedStoreFields = ["STATE", "Banner_ID", "STORE_NUMBER"]

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

# MAGIC %md
# MAGIC ## Code to determine the  type of file maintainence to be processed based on directory path

# COMMAND ----------

#Defining the schema for Store Table
loggerAtt.info('========Schema definition initiated ========')
storeRaw_schema = StructType([ StructField("BANNER_ID",StringType(),False),
                           StructField("FILLER",StringType(),True),   
                           StructField("STORE_NUMBER",IntegerType(),False),
                           StructField("STORE_NAME",StringType(),False),
                           StructField("FILLER1",StringType(),True), 
                           StructField("STORE_TYPE",StringType(),False),
                           StructField("FILLER2",StringType(),True),    
                           StructField("ADDRESS_LINE_1",StringType(),False),
                           StructField("FILLER3",StringType(),True), 
                           StructField("ADDRESS_LINE_2",StringType(),False),
                           StructField("FILLER4",StringType(),True),   
                           StructField("CITY",StringType(),False),
                           StructField("FILLER5",StringType(),True),   
                           StructField("STATE",StringType(),False),
                           StructField("FILLER6",StringType(),True),   
                           StructField("ZIP_CODE",StringType(),False),
                           StructField("PHONE",StringType(),False),
                           StructField("FILLER8",StringType(),True),
                           StructField("MANAGERS_NAME",StringType(),True),  
                           StructField("PHARMACY_INDICATOR",StringType(),False),
                           StructField("FILLER9",StringType(),True),   
                           StructField("CATALOG_ID",IntegerType(),False),
                           StructField("FAX",StringType(),False),
                           StructField("FILLER10",StringType(),True),   
                           StructField("PHONE_PHARM",StringType(),False),
                           StructField("FILLER11",StringType(),True),   
                           StructField("FAX_PHARM",StringType(),False),
                           StructField("FILLER12",StringType(),True),   
                           StructField("SUMMERHOURSYN",StringType(),False),
                           StructField("MONDAY",StringType(),False),
                           StructField("MONDAY_SUMMER",StringType(),False),
                           StructField("MONDAY_PHARM",StringType(),False),
                           StructField("TUESDAY",StringType(),False),
                           StructField("TUESDAY_SUMMER",StringType(),False),
                           StructField("TUESDAY_PHARM",StringType(),False),
                           StructField("WEDNESDAY",StringType(),False), 
                           StructField("WEDNESDAY_SUMMER",StringType(),False), 
                           StructField("WEDNESDAY_PHARM",StringType(),False), 
                           StructField("THURSDAY",StringType(),False), 
                           StructField("THURSDAY_SUMMER",StringType(),False),
                           StructField("THURSDAY_PHARM",StringType(),False), 
                           StructField("FRIDAY",StringType(),False), 
                           StructField("FRIDAY_SUMMER",StringType(),False),
                           StructField("FRIDAY_PHARM",StringType(),False), 
                           StructField("SATURDAY",StringType(),False), 
                           StructField("SATURDAY_SUMMER",StringType(),False), 
                           StructField("SATURDAY_PHARM",StringType(),False), 
                           StructField("SUNDAY",StringType(),False), 
                           StructField("SUNDAY_SUMMER",StringType(),False), 
                           StructField("SUNDAY_PHARM",StringType(),False), 
                           StructField("MLK",StringType(),False), 
                           StructField("MLK_PHARM",StringType(),False), 
                           StructField("PRESIDENTS",StringType(),False), 
                           StructField("PRESIDENTS_PHARM",StringType(),False), 
                           StructField("EASTER",StringType(),False), 
                           StructField("EASTER_PHARM",StringType(),False), 
                           StructField("PATRIOTS",StringType(),False), 
                           StructField("PATRIOTS_PHARM",StringType(),False), 
                           StructField("MEMORIAL",StringType(),False), 
                           StructField("MEMORIAL_PHARM",StringType(),False), 
                           StructField("JULY4",StringType(),False), 
                           StructField("JULY4_PHARM",StringType(),False), 
                           StructField("LABOR",StringType(),False), 
                           StructField("LABOR_PHARM",StringType(),False), 
                           StructField("COLUMBUS",StringType(),False), 
                           StructField("COLUMBUS_PHARM",StringType(),False), 
                           StructField("VETERANS",StringType(),False), 
                           StructField("VETERANS_PHARM",StringType(),False), 
                           StructField("THXGIVING",StringType(),False), 
                           StructField("THXGIVING_PHARM",StringType(),False), 
                           StructField("XMASEVE",StringType(),False), 
                           StructField("XMASEVE_PHARM",StringType(),False), 
                           StructField("XMAS",StringType(),False), 
                           StructField("XMAS_PHARM",StringType(),False), 
                           StructField("NEWYRSEVE",StringType(),False), 
                           StructField("NEWYRSEVE_PHARM",StringType(),False), 
                           StructField("NEWYRS",StringType(),False), 
                           StructField("NEWYRS_PHARM",StringType(),False), 
                           StructField("INDEPENDENT",StringType(),False), 
                           StructField("WEBSITE",StringType(),False), 
                           StructField("ATM",StringType(),False),
                           StructField("FILLER13",StringType(),True),  
                           StructField("CHECK_CASHING",StringType(),False),
                           StructField("INSTORE_BANK",StringType(),False), 
                           StructField("MONEY_ORDERS",StringType(),False), 
                           StructField("MONEY_TRANSFERS",StringType(),False), 
                           StructField("UTILITY_PAYMENTS",StringType(),False), 
                           StructField("POSTAGE_STAMPS",StringType(),False), 
                           StructField("LOTTERY_TICKETS",StringType(),False), 
                           StructField("NEW_YORK_EZPASS",StringType(),False), 
                           StructField("DUMP_PASSES",StringType(),False), 
                           StructField("FISHING_LICENSES",StringType(),False), 
                           StructField("COIN_COUNTING_KIOSK",StringType(),False), 
                           StructField("DVD_RENTAL_KIOSK",StringType(),False), 
                           StructField("BOTTLE_REDEMPTION",StringType(),False), 
                           StructField("CLYNK_REDEMPTION",StringType(),False), 
                           StructField("GREENBACK_REDEMPTION",StringType(),False), 
                           StructField("TOWN_TRASH_BAGS",StringType(),False), 
                           StructField("PROPANE_TANK_EXCHANGE",StringType(),False), 
                           StructField("WATER_COOLER_JUGS",StringType(),False), 
                           StructField("RUG_CLEANER_RENTALS",StringType(),False), 
                           StructField("PARTY_BALLOONS",StringType(),False), 
                           StructField("PARTY_PLATTERS_AND_FRUIT_BAS",StringType(),False), 
                           StructField("CAKE_DECORATING",StringType(),False),  
                           StructField("FLOWERS_AND_FLORAL_ARRANGEMEN",StringType(),False),
                           StructField("SUSHI",StringType(),False),
                           StructField("WINE",StringType(),False),
                           StructField("LIQUOR",StringType(),False),
                           StructField("CAFE",StringType(),False),
                           StructField("SHELLFISH_STEAMER",StringType(),False),
                           StructField("LOBSTER_TANK",StringType(),False),
                           StructField("DRIVE_THRU_RX",StringType(),False),
                           StructField("BLOOD_PRESSURE_MACHINE",StringType(),False),
                           StructField("NUTITIONIST",StringType(),False),
                           StructField("BUS_PARTNERSHIP",StringType(),False),
                           StructField("ISLAND_DELIVERY_SERVICE",StringType(),False),
                           StructField("ONLINE_WINE",StringType(),False),
                           StructField("SELF_SCAN",StringType(),False),
                           StructField("SALAD_BAR",StringType(),False),
                           StructField("OLIVE_BAR",StringType(),False),
                           StructField("WING_BAR",StringType(),False),
                           StructField("BEER",StringType(),False),
                           StructField("BUTCHER_SHOP",StringType(),False),
                           StructField("ISPU_INDICATOR",StringType(),False),
                           StructField("DIRECT_FLAG",StringType(),False),
                           StructField("PICKING_FEE_WAIVED",StringType(),False),
                           StructField("LIQUOR_STORE_UPDATE_FLAG",StringType(),False),
                           StructField("MONDAY_LIQUOR",StringType(),False),
                           StructField("TUESDAY_LIQUOR",StringType(),False),
                           StructField("WEDNESDAY_LIQUOR",StringType(),False),
                           StructField("THURSDAY_LIQUOR",StringType(),False),
                           StructField("FRIDAY_LIQUOR",StringType(),False),
                           StructField("SATURDAY_LIQUOR",StringType(),False),
                           StructField("SUNDAY_LIQUOR",StringType(),False),
                           StructField("PHONE_LIQUOR",StringType(),False),
                           StructField("PHYS_CLSE_DT",StringType(),False),
                           StructField("FIN_CLSE_DT",StringType(),False), 
                           StructField("SYS_XPIR_DT",StringType(),False), 
                           StructField("PHAR_CLSE_DT",StringType(),False) 
   
                           
])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Delta table creation

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
  """.format(StoreDeltaPath))
except Exception as ex:
  ABC(DeltaTableCreateCheck = 0)
  loggerAtt.error(ex)
  err = ErrorReturn('Error', ex,'storeDeltaPath deltaCreator')
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

try:
  ABC(DeltaTableCreateCheck=1)
  spark.sql("""
  CREATE TABLE IF NOT EXISTS StoreTemp (
  SMA_STORE_STATE String,
  STORE_BANNER_ID String,
  STORE_STORE_NUMBER String
  )
  USING delta
  Location '{}'
  PARTITIONED BY (STORE_BANNER_ID)
  """.format(storeEffDeltaPath))
  spark.sql("OPTIMIZE StoreTemp")
except Exception as ex:
  ABC(DeltaTableCreateCheck = 0)
  loggerAtt.error(ex)
  err = ErrorReturn('Error', ex,'storeTemp deltaCreator')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Store Processing

# COMMAND ----------

date_func =  udf (lambda x: datetime.datetime.strptime(str(x), '%Y-%m-%d').date(), DateType())

#Column renaming functions 
def storeflat_storetable(s):
    return Store_Renaming[s]
def change_col_name(s):
    return s in Store_Renaming

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Store Transformation

# COMMAND ----------

def storeTransformation(processed_df, JOB_ID):
  processed_df=processed_df.withColumn("INSERT_TIMESTAMP",current_timestamp())
  processed_df=processed_df.withColumn("LAST_UPDATE_ID",lit('JOB_ID'))
  processed_df=processed_df.withColumn("LAST_UPDATE_TIMESTAMP",current_timestamp())
  processed_df=processed_df.withColumn("INSERT_ID", lit('JOB_ID'))
  processed_df=processed_df.withColumn("PHYS_CLSE_DT",col('PHYS_CLSE_DT').cast(StringType()))
  processed_df=processed_df.withColumn("STORE_NUMBER",lpad(col('STORE_NUMBER'),4,'0'))
  processed_df = processed_df.withColumn('Banner_ID', regexp_replace(col("Banner_ID"), " ", ""))
  
  return processed_df

# COMMAND ----------

def itemMasterTransformation(processed_df):

#   processed_df = processed_df.withColumn('Banner_ID', regexp_replace(col("Banner_ID"), " ", ""))
  return processed_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Store Renaming dictionary

# COMMAND ----------

Store_Renaming = {"STORE_NUM":"STORE_NUMBER", 
                  "POS_TYPE":"STAGING_AREA", 
                  "PRIMARY_WIC_STATE":"WIC_ELIGIBLE_IND", 
                  "WIC_SCNDRY_STATE":"WIC_SCNDRY_ELIGIBLE_STATE", 
                  "STORE_DIVISION":"AREA", 
                  "STORE_ADDRESS":"ADD_1", 
                  "STORE_CITY":"CITY", 
                  "STORE_STATE":"STATE", 
                  "STORE_ZIP":"ZIP", 
                  "STORE_OPEN_DTE":"STORE_OPEN_DATE", 
                  "STORE_CLOSE_DTE":"STORE_CLOSE_DATE",  
                 }

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Store file reading

# COMMAND ----------

def readFile(file_location, infer_schema, first_row_is_header, delimiter,file_type):
  raw_df = spark.read.format(file_type) \
    .option("mode","PERMISSIVE") \
    .option("header", first_row_is_header) \
    .option("dateFormat", "yyyyMMdd") \
    .option("sep", delimiter) \
    .schema(storeRaw_schema) \
    .load(file_location)
  return raw_df

# COMMAND ----------

def insertAholdColumns(processed_df):
  processed_df=processed_df.withColumn("DEST_STORE", lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("BATCH_SERIAL", lit(None).cast(IntegerType()))
  processed_df=processed_df.withColumn("DAYOFWEEK", lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("SUB_DEPT", lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("CHNG_TYP", lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("VEND_BREAK", lit(None).cast(IntegerType()))
  processed_df=processed_df.withColumn("POS_TYPE", lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("WIC_ELIGIBLE_IND", lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("WIC_SCNDRY_ELIGIBLE_STATE", lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("STORE_DIVISION", lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("STORE_OPEN_DATE", lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("STORE_CLOSE_DATE", lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("MAINT_LOOKAHEAD_DAYS", lit(None).cast(IntegerType()))
  processed_df=processed_df.withColumn("RETENTION_DAYS", lit(None).cast(IntegerType()))
  processed_df=processed_df.withColumn("STORE_BANNER", lit(None).cast(IntegerType()))
  processed_df=processed_df.withColumn("LAST_UPDATE_DATE", lit(None).cast(StringType()))
  processed_df=processed_df.withColumnRenamed("PHYS_CLSE_DT", "CLOSED_DATE")
  processed_df=processed_df.withColumn("CLOSED_DATE",date_format(col("CLOSED_DATE"), 'yyyy/MM/dd').cast(StringType()))
  processed_df=processed_df.withColumn("STORE_NUMBER",col("STORE_NUMBER").cast(IntegerType()))
#   processed_df=processed_df.withColumn("CLOSED_DATE",col('CLOSED_DATE').cast(StringType()))
  processed_df=processed_df.withColumn("INSERT_TIMESTAMP",col('INSERT_TIMESTAMP').cast(StringType()))
  processed_df=processed_df.withColumn("LAST_UPDATE_TIMESTAMP",col('LAST_UPDATE_TIMESTAMP').cast(StringType()))
  processed_df=processed_df.select('Banner_ID','STORE_NUMBER', 'STORE_NAME', 'STORE_TYPE', 'ADDRESS_LINE_1', 'ADDRESS_LINE_2', 'CITY', 'STATE', 'ZIP_CODE', 'PHONE', 'MANAGERS_NAME', 'PHARMACY_INDICATOR', 'CATALOG_ID', 'FAX', 'PHONE_PHARM', 'FAX_PHARM', 'SUMMERHOURSYN', 'MONDAY', 'MONDAY_SUMMER', 'MONDAY_PHARM', 'TUESDAY', 'TUESDAY_SUMMER', 'TUESDAY_PHARM', 'WEDNESDAY', 'WEDNESDAY_SUMMER', 'WEDNESDAY_PHARM', 'THURSDAY', 'THURSDAY_SUMMER', 'THURSDAY_PHARM', 'FRIDAY', 'FRIDAY_SUMMER', 'FRIDAY_PHARM', 'SATURDAY', 'SATURDAY_SUMMER', 'SATURDAY_PHARM', 'SUNDAY', 'SUNDAY_SUMMER', 'SUNDAY_PHARM', 'MLK', 'MLK_PHARM', 'PRESIDENTS', 'PRESIDENTS_PHARM', 'EASTER', 'EASTER_PHARM', 'PATRIOTS', 'PATRIOTS_PHARM', 'MEMORIAL', 'MEMORIAL_PHARM', 'JULY4', 'JULY4_PHARM', 'LABOR', 'LABOR_PHARM', 'COLUMBUS', 'COLUMBUS_PHARM', 'VETERANS', 'VETERANS_PHARM', 'THXGIVING', 'THXGIVING_PHARM', 'XMASEVE', 'XMASEVE_PHARM', 'XMAS', 'XMAS_PHARM', 'NEWYRSEVE', 'NEWYRSEVE_PHARM', 'NEWYRS', 'NEWYRS_PHARM', 'INDEPENDENT', 'WEBSITE', 'ATM', 'CHECK_CASHING', 'INSTORE_BANK', 'MONEY_ORDERS', 'MONEY_TRANSFERS', 'UTILITY_PAYMENTS', 'POSTAGE_STAMPS', 'LOTTERY_TICKETS', 'NEW_YORK_EZPASS', 'DUMP_PASSES', 'FISHING_LICENSES', 'COIN_COUNTING_KIOSK', 'DVD_RENTAL_KIOSK', 'BOTTLE_REDEMPTION', 'CLYNK_REDEMPTION', 'GREENBACK_REDEMPTION', 'TOWN_TRASH_BAGS', 'PROPANE_TANK_EXCHANGE', 'WATER_COOLER_JUGS', 'RUG_CLEANER_RENTALS', 'PARTY_BALLOONS', 'PARTY_PLATTERS_AND_FRUIT_BAS', 'CAKE_DECORATING', 'FLOWERS_AND_FLORAL_ARRANGEMEN', 'SUSHI', 'WINE', 'LIQUOR', 'CAFE', 'SHELLFISH_STEAMER', 'LOBSTER_TANK', 'DRIVE_THRU_RX', 'BLOOD_PRESSURE_MACHINE', 'NUTITIONIST', 'BUS_PARTNERSHIP', 'ISLAND_DELIVERY_SERVICE', 'ONLINE_WINE', 'SELF_SCAN', 'SALAD_BAR', 'OLIVE_BAR', 'WING_BAR', 'BEER', 'BUTCHER_SHOP', 'ISPU_INDICATOR', 'DIRECT_FLAG', 'PICKING_FEE_WAIVED', 'LIQUOR_STORE_UPDATE_FLAG', 'MONDAY_LIQUOR', 'TUESDAY_LIQUOR', 'WEDNESDAY_LIQUOR', 'THURSDAY_LIQUOR', 'FRIDAY_LIQUOR', 'SATURDAY_LIQUOR', 'SUNDAY_LIQUOR', 'PHONE_LIQUOR', 'DEST_STORE', 'BATCH_SERIAL', 'DAYOFWEEK', 'SUB_DEPT', 'CHNG_TYP', 'VEND_BREAK', 'POS_TYPE', 'WIC_ELIGIBLE_IND', 'WIC_SCNDRY_ELIGIBLE_STATE', 'STORE_DIVISION', 'STORE_OPEN_DATE', 'STORE_CLOSE_DATE', 'MAINT_LOOKAHEAD_DAYS', 'RETENTION_DAYS', 'LAST_UPDATE_DATE', 'CLOSED_DATE', 'STORE_BANNER', 'INSERT_ID', 'INSERT_TIMESTAMP', 'LAST_UPDATE_ID', 'LAST_UPDATE_TIMESTAMP')
  return processed_df

# COMMAND ----------

def abcFramework(headerFooterRecord, store_duplicate_records, store_raw_df):
  global fileRecordCount
  loggerAtt.info("ABC Framework function initiated")
  try:
    # Fetching all store id records with non integer value
#     headerFooterRecord = store_raw_df.filter(typeCheckUDF(col('BANNER_ID')) == False)
    headerFooterRecord = store_raw_df.filter(store_raw_df["BANNER_ID"].rlike("BOF|EOF"))
    # Fetching EOF and BOF records and checking whether it is of length one each
    eofDf = headerFooterRecord.filter(store_raw_df["BANNER_ID"].rlike("^:EOF"))
    bofDf = headerFooterRecord.filter(store_raw_df["BANNER_ID"].rlike("^:BOF"))
    ABC(BOFEOFCheck=1)
    EOFcnt = eofDf.count()
    BOFcnt = bofDf.count()
    ABC(EOFCount=EOFcnt)
    ABC(BOFCount=BOFcnt)
    loggerAtt.info("EOF file record count is " + str(eofDf.count()))
    loggerAtt.info("BOF file record count is " + str(bofDf.count()))
    
    # If there are multiple header/Footer then the file is invalid
    if ((eofDf.count() != 1) or (bofDf.count() != 1)):
      raise Exception('Error in EOF or BOF value')
    
#     storeDf = store_raw_df.filter(typeCheckUDF(col('BANNER_ID')) == True)
    storeDf = store_raw_df.filter(store_raw_df["BANNER_ID"].rlike("FDLN|HAN"))
    
#     fileRecordCount = int(eofDf.select('BANNER_ID').toPandas()['BANNER_ID'][0].split()[1])
    fileRecordCount = int(re.findall("\d+", eofDf.select('BANNER_ID').toPandas()['BANNER_ID'][0])[0])
    actualRecordCount = int(storeDf.count() + headerFooterRecord.count() - 2)
    
    loggerAtt.info("Record Count mentioned in file: " + str(fileRecordCount))
    loggerAtt.info("Actual no of record in file: " + str(actualRecordCount))
    
    # Checking to see if the count matched the actual record count
    if actualRecordCount != fileRecordCount:
      raise Exception('Record count mismatch. Actual count ' + str(actualRecordCount) + ', file record count ' + str(fileRecordCount))
     
  except Exception as ex:
    ABC(BOFEOFCheck=0)
    EOFcnt = None
    BOFcnt= None
    ABC(EOFCount=EOFcnt)
    ABC(BOFCount=BOFcnt)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'BOFEOFCheck')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
#     err = ErrorReturn('Error', ex,'ABC Framework check Error')
#     err.exit()
  
  try:
    # Exception handling of schema records
    store_nullRows = storeDf.where(reduce(lambda x, y: x | y, (col(x).isNull() for x in storeDf.columns)))
    ABC(NullValueCheck=1)
    loggerAtt.info("Dimension of the Null records:("+str(store_nullRows.count())+"," +str(len(store_nullRows.columns))+")")

    store_raw_dfWithNoNull = storeDf.na.drop()
    ABC(DropNACheck = 1)
    NullValuCnt = store_nullRows.count()
    ABC(NullValuCount = NullValuCnt)
    loggerAtt.info("Dimension of the Not null records:("+str(store_raw_dfWithNoNull.count())+"," +str(len(store_raw_dfWithNoNull.columns))+")")

  except Exception as ex:
    NullValueCheck = 0
    NullValuCnt=''
    ABC(NullValueCheck=0)
    ABC(NullValuCount = NullValuCnt)
    err = ErrorReturn('Error', ex,'NullRecordHandling')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
    
  try:
    # removing duplicate record 
    ABC(DuplicateValueCheck = 1)
    if (store_raw_dfWithNoNull.groupBy(store_raw_dfWithNoNull.columns).count().filter("count > 1").count()) > 0:
      loggerAtt.info("Duplicate records exists")
      store_rawdf_withCount =store_raw_dfWithNoNull.groupBy(store_raw_dfWithNoNull.columns).count();
      store_duplicate_records = store_rawdf_withCount.filter("count > 1").drop('count')
      storeRecords= store_rawdf_withCount.drop('count')
      loggerAtt.info("Duplicate record Exists. No of duplicate records are " + str(store_duplicate_records.count()))
      ABC(DuplicateValueCount=DuplicateValueCnt)
    else:
      loggerAtt.info("No duplicate records")
      storeRecords = store_raw_dfWithNoNull
      ABC(DuplicateValueCount=0)
    
    loggerAtt.info("ABC Framework function ended")
    return headerFooterRecord, store_nullRows, store_duplicate_records, storeRecords
  except Exception as ex:
    ABC(DuplicateValueCheck = 0)
    ABC(DuplicateValueCount='')
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'store_ProblemRecs')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
#     err = ErrorReturn('Error', ex,'ABC Framework check Error')
#     err.exit()

# COMMAND ----------

def itemMasterRecordsModified(itemTempEffDeltaPath, storeEffDeltaPath, StoreDeltaPath, Date, unifiedStoreFields):
  StoreDelta = spark.sql('''select STATE as SMA_STORE_STATE, Banner_ID as STORE_BANNER_ID, STORE_NUMBER AS STORE_STORE_NUMBER from delta.`{}`'''.format(StoreDeltaPath))
  
  itemTempEffDelta = spark.sql('''select SMA_DEST_STORE, BANNER_ID as Item_BANNER_ID from delta.`{}` group by SMA_DEST_STORE, Item_BANNER_ID'''.format(itemTempEffDeltaPath))
  
  StoreDelta = StoreDelta.join(itemTempEffDelta, [itemTempEffDelta.SMA_DEST_STORE == StoreDelta.STORE_STORE_NUMBER, itemTempEffDelta.Item_BANNER_ID == StoreDelta.STORE_BANNER_ID], how='inner').select([col(xx) for xx in StoreDelta.columns])
  
  storeEffTemp = spark.sql('''DELETE FROM delta.`{}`'''.format(storeEffDeltaPath))
  StoreDelta.write.partitionBy('STORE_BANNER_ID').format('delta').mode('append').save(storeEffDeltaPath)
  
  ABC(UnifiedRecordFeedCount = StoreDelta.count())
  ABC(UnifiedRecordItemCount = itemTempEffDelta.count())
  loggerAtt.info("UnifiedRecordStoreCount:" +str(StoreDelta.count()))
  loggerAtt.info("UnifiedRecordItemCount:" +str(itemTempEffDelta.count()))
  loggerAtt.info("itemMasterRecords fetch successful")

# COMMAND ----------

def StoreArchival(StoreDeltaPath,Date,Archivalfilelocation):
  storearchival_df = spark.read.format('delta').load(StoreDeltaPath)
  closedstore_df = storearchival_df.where(col('PHYS_CLSE_DT').isNotNull())
  storearchival_df = storearchival_df.where(col('PHYS_CLSE_DT').isNull())
 
  loggerAtt.info("Number of open stores:" + str(storearchival_df.count()))
  loggerAtt.info("Number of closed stores:" + str(closedstore_df.count()))
  if storearchival_df.count() >0:
    
    storearchival_df.write.mode('Append').format('parquet').save(Archivalfilelocation + "/" +Date+ "/" +"Store_Archival_Records")
    deltaTable = DeltaTable.forPath(spark, StoreDeltaPath)
#     loggerAtt.info("Number of closed stores:" + spark.sql("select count(*) from" + deltaTable + "where PHYS_CLSE_DT is not null"))
    deltaTable.delete(col('PHYS_CLSE_DT').isNotNull())
    
    loggerAtt.info('========Store Records Archival successful ========')
  else:
    loggerAtt.info('======== No Store Records Archival Done ========')
  return storearchival_df

# COMMAND ----------

def upsertStoreRecords(store_valid_Records):
  loggerAtt.info("Merge into Delta table initiated")
  temp_table_name = "storeRecords"

  store_valid_Records.createOrReplaceTempView(temp_table_name)
  try:
    spark.sql('''MERGE INTO delta.`{}` as store
    USING storeRecords 
    ON store.STORE_NUMBER = storeRecords.STORE_NUMBER
    WHEN MATCHED Then 
            Update Set store.Banner_ID=storeRecords.Banner_ID,
                       store.STORE_NUMBER = storeRecords.STORE_NUMBER,
                       store.STORE_NAME=storeRecords.STORE_NAME,
                       store.STORE_TYPE=storeRecords.STORE_TYPE,
                       store.ADDRESS_LINE_1=storeRecords.ADDRESS_LINE_1,
                       store.ADDRESS_LINE_2=storeRecords.ADDRESS_LINE_2,
                       store.CITY=storeRecords.CITY,
                       store.STATE=storeRecords.STATE,
                       store.ZIP_CODE=storeRecords.ZIP_CODE,
                       store.PHONE=storeRecords.PHONE,
                       store.MANAGERS_NAME=storeRecords.MANAGERS_NAME,
                       store.PHARMACY_INDICATOR=storeRecords.PHARMACY_INDICATOR,
                       store.CATALOG_ID=storeRecords.CATALOG_ID,
                       store.FAX=storeRecords.FAX,
                       store.PHONE_PHARM=storeRecords.PHONE_PHARM,
                       store.FAX_PHARM=storeRecords.FAX_PHARM,
                       store.SUMMERHOURSYN=storeRecords.SUMMERHOURSYN,
                       store.MONDAY=storeRecords.MONDAY,
                       store.MONDAY_SUMMER=storeRecords.MONDAY_SUMMER,
                       store.MONDAY_PHARM=storeRecords.MONDAY_PHARM,
                       store.TUESDAY=storeRecords.TUESDAY,
                       store.TUESDAY_SUMMER=storeRecords.TUESDAY_SUMMER,
                       store.TUESDAY_PHARM=storeRecords.TUESDAY_PHARM,
                       store.WEDNESDAY=storeRecords.WEDNESDAY,
                       store.WEDNESDAY_SUMMER=storeRecords.WEDNESDAY_SUMMER,
                       store.WEDNESDAY_PHARM=storeRecords.WEDNESDAY_PHARM,
                       store.THURSDAY=storeRecords.THURSDAY,
                       store.THURSDAY_SUMMER=storeRecords.THURSDAY_SUMMER,
                       store.THURSDAY_PHARM=storeRecords.THURSDAY_PHARM,
                       store.FRIDAY=storeRecords.FRIDAY,
                       store.FRIDAY_SUMMER=storeRecords.FRIDAY_SUMMER,
                       store.FRIDAY_PHARM=storeRecords.FRIDAY_PHARM,
                       store.SATURDAY=storeRecords.SATURDAY,
                       store.SATURDAY_SUMMER=storeRecords.SATURDAY_SUMMER,
                       store.SATURDAY_PHARM=storeRecords.SATURDAY_PHARM,
                       store.SUNDAY=storeRecords.SUNDAY,
                       store.SUNDAY_SUMMER=storeRecords.SUNDAY_SUMMER,
                       store.SUNDAY_PHARM=storeRecords.SUNDAY_PHARM,
                       store.MLK=storeRecords.MLK,
                       store.MLK_PHARM=storeRecords.MLK_PHARM,
                       store.PRESIDENTS=storeRecords.PRESIDENTS,
                       store.PRESIDENTS_PHARM=storeRecords.PRESIDENTS_PHARM,
                       store.EASTER=storeRecords.EASTER,
                       store.EASTER_PHARM=storeRecords.EASTER_PHARM,
                       store.PATRIOTS=storeRecords.PATRIOTS,
                       store.PATRIOTS_PHARM=storeRecords.PATRIOTS_PHARM,
                       store.MEMORIAL=storeRecords.MEMORIAL,
                       store.MEMORIAL_PHARM=storeRecords.MEMORIAL_PHARM,
                       store.JULY4=storeRecords.JULY4,
                       store.JULY4_PHARM=storeRecords.JULY4_PHARM,
                       store.LABOR=storeRecords.LABOR,
                       store.LABOR_PHARM=storeRecords.LABOR_PHARM,
                       store.COLUMBUS=storeRecords.COLUMBUS,
                       store.COLUMBUS_PHARM=storeRecords.COLUMBUS_PHARM,
                       store.VETERANS=storeRecords.VETERANS,
                       store.VETERANS_PHARM=storeRecords.VETERANS_PHARM,
                       store.THXGIVING=storeRecords.THXGIVING,
                       store.THXGIVING_PHARM=storeRecords.THXGIVING_PHARM,
                       store.XMASEVE=storeRecords.XMASEVE,
                       store.XMASEVE_PHARM=storeRecords.XMASEVE_PHARM,
                       store.XMAS=storeRecords.XMAS,
                       store.XMAS_PHARM=storeRecords.XMAS_PHARM,
                       store.NEWYRSEVE=storeRecords.NEWYRSEVE,
                       store.NEWYRSEVE_PHARM=storeRecords.NEWYRSEVE_PHARM,
                       store.NEWYRS=storeRecords.NEWYRS,
                       store.NEWYRS_PHARM=storeRecords.NEWYRS_PHARM,
                       store.INDEPENDENT=storeRecords.INDEPENDENT,
                       store.WEBSITE=storeRecords.WEBSITE,
                       store.ATM=storeRecords.ATM,
                       store.CHECK_CASHING=storeRecords.CHECK_CASHING,
                       store.INSTORE_BANK=storeRecords.INSTORE_BANK,
                       store.MONEY_ORDERS=storeRecords.MONEY_ORDERS,
                       store.MONEY_TRANSFERS=storeRecords.MONEY_TRANSFERS,
                       store.UTILITY_PAYMENTS=storeRecords.UTILITY_PAYMENTS,
                       store.POSTAGE_STAMPS=storeRecords.POSTAGE_STAMPS,
                       store.LOTTERY_TICKETS=storeRecords.LOTTERY_TICKETS,
                       store.NEW_YORK_EZPASS=storeRecords.NEW_YORK_EZPASS,
                       store.DUMP_PASSES=storeRecords.DUMP_PASSES,
                       store.FISHING_LICENSES=storeRecords.FISHING_LICENSES,
                       store.COIN_COUNTING_KIOSK=storeRecords.COIN_COUNTING_KIOSK,
                       store.DVD_RENTAL_KIOSK=storeRecords.DVD_RENTAL_KIOSK,
                       store.BOTTLE_REDEMPTION=storeRecords.BOTTLE_REDEMPTION,
                       store.CLYNK_REDEMPTION=storeRecords.CLYNK_REDEMPTION,
                       store.GREENBACK_REDEMPTION=storeRecords.GREENBACK_REDEMPTION,
                       store.TOWN_TRASH_BAGS=storeRecords.TOWN_TRASH_BAGS,
                       store.PROPANE_TANK_EXCHANGE=storeRecords.PROPANE_TANK_EXCHANGE,
                       store.WATER_COOLER_JUGS=storeRecords.WATER_COOLER_JUGS,
                       store.RUG_CLEANER_RENTALS=storeRecords.RUG_CLEANER_RENTALS,
                       store.PARTY_BALLOONS=storeRecords.PARTY_BALLOONS,
                       store.PARTY_PLATTERS_AND_FRUIT_BAS=storeRecords.PARTY_PLATTERS_AND_FRUIT_BAS,
                       store.CAKE_DECORATING=storeRecords.CAKE_DECORATING,
                       store.FLOWERS_AND_FLORAL_ARRANGEMEN=storeRecords.FLOWERS_AND_FLORAL_ARRANGEMEN,
                       store.SUSHI=storeRecords.SUSHI,
                       store.WINE=storeRecords.WINE,
                       store.LIQUOR=storeRecords.LIQUOR,
                       store.CAFE=storeRecords.CAFE,
                       store.SHELLFISH_STEAMER=storeRecords.SHELLFISH_STEAMER,
                       store.LOBSTER_TANK=storeRecords.LOBSTER_TANK,
                       store.DRIVE_THRU_RX=storeRecords.DRIVE_THRU_RX,
                       store.BLOOD_PRESSURE_MACHINE=storeRecords.BLOOD_PRESSURE_MACHINE,
                       store.NUTITIONIST=storeRecords.NUTITIONIST,
                       store.BUS_PARTNERSHIP=storeRecords.BUS_PARTNERSHIP,
                       store.ISLAND_DELIVERY_SERVICE=storeRecords.ISLAND_DELIVERY_SERVICE,
                       store.ONLINE_WINE=storeRecords.ONLINE_WINE,
                       store.SELF_SCAN=storeRecords.SELF_SCAN,
                       store.SALAD_BAR=storeRecords.SALAD_BAR,
                       store.OLIVE_BAR=storeRecords.OLIVE_BAR,
                       store.WING_BAR=storeRecords.WING_BAR,
                       store.BEER=storeRecords.BEER,
                       store.BUTCHER_SHOP=storeRecords.BUTCHER_SHOP,
                       store.ISPU_INDICATOR=storeRecords.ISPU_INDICATOR,
                       store.DIRECT_FLAG=storeRecords.DIRECT_FLAG,
                       store.PICKING_FEE_WAIVED=storeRecords.PICKING_FEE_WAIVED,
                       store.LIQUOR_STORE_UPDATE_FLAG=storeRecords.LIQUOR_STORE_UPDATE_FLAG,
                       store.MONDAY_LIQUOR=storeRecords.MONDAY_LIQUOR,
                       store.TUESDAY_LIQUOR=storeRecords.TUESDAY_LIQUOR,
                       store.WEDNESDAY_LIQUOR=storeRecords.WEDNESDAY_LIQUOR,
                       store.THURSDAY_LIQUOR=storeRecords.THURSDAY_LIQUOR,
                       store.FRIDAY_LIQUOR=storeRecords.FRIDAY_LIQUOR,
                       store.SATURDAY_LIQUOR=storeRecords.SATURDAY_LIQUOR,
                       store.SUNDAY_LIQUOR=storeRecords.SUNDAY_LIQUOR,
                       store.PHONE_LIQUOR=storeRecords.PHONE_LIQUOR,
                       store.PHYS_CLSE_DT=storeRecords.PHYS_CLSE_DT,
                       store.FIN_CLSE_DT=storeRecords.FIN_CLSE_DT,
                       store.SYS_XPIR_DT=storeRecords.SYS_XPIR_DT,
                       store.PHAR_CLSE_DT=storeRecords.PHAR_CLSE_DT,
                       store.LAST_UPDATE_ID=storeRecords.LAST_UPDATE_ID,
                       store.LAST_UPDATE_TIMESTAMP=storeRecords.LAST_UPDATE_TIMESTAMP


                  WHEN NOT MATCHED THEN INSERT * '''.format(StoreDeltaPath))
    loggerAtt.info("Merge into Delta table successful")

  except Exception as ex:
    loggerAtt.info("Merge into Delta table failed and throwed error")
    loggerAtt.error(str(ex))
    err = ErrorReturn('Error', ex,'MergeDeltaTable')
    err.exit()

# COMMAND ----------

def writeInvalidRecords(store_invalidRecords, Invalid_RecordsPath):
  try:
    if store_invalidRecords.count() > 0:
      store_invalidRecords.write.mode('Append').format('parquet').save(Invalid_RecordsPath + "/" +Date+ "/" + "Store_Invalid_Data")
      loggerAtt.info('======== Invalid Store Records write operation finished ========')

  except Exception as ex:
    ABC(InvalidRecordSaveCheck = 0)
    loggerAtt.error(str(ex))
    loggerAtt.info('======== Invalid record file write to ADLS location failed ========')
    err = ErrorReturn('Error', ex,'Writeoperationfailed for Invalid Record')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

def storeWrite(StoreDeltaPath,storeOutboundPath):
  storeOutputDf = spark.read.format('delta').load(StoreDeltaPath)
#   storeList = list(set(store_valid_Records.select('Banner_ID').toPandas()['Banner_ID']))
#   storeOutputDf = storeOutputDf.filter((col("Banner_ID").isin(storeList))) 
#   ABC(storeOutputFileCount=storeOutputDf.count())
  if storeOutputDf.count() >0:
    storeOutputDf = insertAholdColumns(storeOutputDf)
    ABC(storeOutputFileCount=storeOutputDf.count())
    storeOutputDf = storeOutputDf.withColumn('CHAIN_ID', lit('DELHAIZE'))
    storeOutputDf.write.partitionBy('CHAIN_ID').mode('overwrite').format('parquet').save(storeOutboundPath + "/" +"Store_Output")
    loggerAtt.info('========Store Records Output successful ========')
  else:
    loggerAtt.info('======== No Store Records Output Done ========')
    ABC(storeOutputFileCount=0)

# COMMAND ----------

if __name__ == "__main__":
  
  ## File reading parameters
  loggerAtt.info('======== Input store file processing initiated ========')
  file_location = str(file_location)
  file_type = "csv"
  infer_schema = "false"
  first_row_is_header = "false"
  delimiter = "|"
  store_raw_df = None
  store_duplicate_records = None
  headerFooterRecord = None
  storeRecords = None
  store_valid_Records = None
  PipelineID= str(PipelineID)
  folderDate = Date
  
  if POSemergencyFlag == 'Y':
    try:
      ABC(itemMasterRecordsCheck=1)
      itemMasterRecordsModified(itemTempEffDeltaPath, storeEffDeltaPath, StoreDeltaPath, Date, unifiedStoreFields)  
    except Exception as ex:
      ABC(itemMasterCount='')
      ABC(itemMasterRecordsCheck=0)
      err = ErrorReturn('Error', ex,'itemMasterRecordsModified')
      loggerAtt.error(ex)
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
  else:
    ## Step1: Reading input file
    try:
      store_raw_df = readFile(file_location, infer_schema, first_row_is_header, delimiter, file_type)
      ABC(ReadDataCheck=1)
      RawDataCount = store_raw_df.count()
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
  #     err.exit()

    if store_raw_df is not None:

      ## Step2: Performing ABC framework check
      headerFooterRecord, store_nullRows, store_duplicate_records, storeRecords = abcFramework(headerFooterRecord, store_duplicate_records, store_raw_df)

      ## renaming the vendor column names
      storeRecords = quinn.with_some_columns_renamed(storeflat_storetable, change_col_name)(storeRecords) ##

      ## Step3: Performing Transformation
      try:  
        if storeRecords is not None: 
  #         storeRecords=storeRecords.withColumn("STORE_NUMBER",col("STORE_NUMBER").cast(IntegerType()))
          store_valid_Records = storeTransformation(storeRecords, PipelineID)
          ABC(TransformationCheck=1)
      except Exception as ex:
        ABC(TransformationCheck = 0)
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'storeTransformation')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
        err = ErrorReturn('Error', ex,'storeTransformation')
        err.exit()

      ## Step4: Combining all duplicate records
      try:
        ## Combining Duplicate, null record
        if store_duplicate_records is not None:
          invalidRecordsList = [headerFooterRecord, store_duplicate_records, store_nullRows]
          store_invalidRecords = reduce(DataFrame.unionAll, invalidRecordsList)
          ABC(InvalidRecordSaveCheck = 1)
          ABC(InvalidRecordCount = store_invalidRecords.count())
        else:
          invalidRecordsList = [headerFooterRecord, store_nullRows]
          store_invalidRecords = reduce(DataFrame.unionAll, invalidRecordsList)
      except Exception as ex:
        ABC(InvalidRecordSaveCheck = 0)
        ABC(InvalidRecordCount='')
        err = ErrorReturn('Error', ex,'Grouping Invalid record function')
        err = ErrorReturn('Error', ex,'InvalidRecordsSave')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
  #       err.exit()    

      loggerAtt.info("No of valid records: " + str(store_valid_Records.count()))
      loggerAtt.info("No of invalid records: " + str(store_invalidRecords.count()))

      ## Step5: Merging records into delta table
      if store_valid_Records is not None:
          upsertStoreRecords(store_valid_Records)

      ## Step6: Writing Invalid records to ADLS location
      if store_invalidRecords is not None:
        writeInvalidRecords(store_invalidRecords, Invalid_RecordsPath)

      try:
        ABC(itemMasterRecordsCheck=1)
        itemMasterRecordsModified(itemTempEffDeltaPath, storeEffDeltaPath, StoreDeltaPath, Date, unifiedStoreFields)  
      except Exception as ex:
        ABC(itemMasterCount='')
        ABC(itemMasterRecordsCheck=0)
        err = ErrorReturn('Error', ex,'itemMasterRecordsModified')
        loggerAtt.error(ex)
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
        
      try:
        loggerAtt.info("Store records Archival records initated")
        ABC(archivalCheck = 1)
        StoreArchival(StoreDeltaPath,Date,Archival_filePath)
      except Exception as ex:
        ABC(archivalCheck = 0)
        ABC(archivalInitCount='')
        ABC(archivalAfterCount='')
        err = ErrorReturn('Error', ex,'storeArchival')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
        
           ## Step 8: Write output file
      try:
        ABC(itemWriteCheck=1)
        storeWrite(StoreDeltaPath,storeOutboundPath)
      except Exception as ex:
        ABC(itemOutputFileCount='')
        ABC(itemWriteCheck=0)
        err = ErrorReturn('Error', ex,'itemMasterWrite')
        loggerAtt.error(ex)
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
 

    else:
      loggerAtt.info("Error in input file reading")
    
     
    
loggerAtt.info('======== Input store file processing ended ========')

# COMMAND ----------

# try:
#   loggerAtt.info("Store records Archival records initated")
#   StoreArchival(StoreDeltaPath,Date,Archival_filePath)
# except Exception as ex:
#   loggerAtt.info("store archival in store feed script throwed error")
#   loggerAtt.error(str(ex))
#   err = ErrorReturn('Error', ex,'Store Archival')
  
  

# COMMAND ----------

# display(storeOutputDf)

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

