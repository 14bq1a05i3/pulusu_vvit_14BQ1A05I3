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
from pyspark.sql import functions as F

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
# MAGIC ## ABC FRAMEWORK

# COMMAND ----------

ABCChecks = {}
def ABC(**kwargs):
  for key, value in kwargs.items():
      #loggerAtt.info("The value of {} is {}".format(key, value))
      ABCChecks[key] = value


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

def Merge(dict1, dict2):
    res = {**dict1, **dict2}
    return res

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ##Widgets for getting dynamic paramters from ADF 

# COMMAND ----------

dbutils.widgets.removeAll()
loggerAtt.info("========Widgets call initiated==========")
dbutils.widgets.text("outputDirectory","")
dbutils.widgets.text("fileName","")
dbutils.widgets.text("filePath","")
dbutils.widgets.text("directory","")
dbutils.widgets.text("Container","")
dbutils.widgets.text("pipelineID","")
dbutils.widgets.text("MountPoint","")
dbutils.widgets.text("deltaPath","")
dbutils.widgets.text("archivalFilePath","")
# dbutils.widgets.text("Store_OutboundPath","")
dbutils.widgets.text("logFilesPath","")
dbutils.widgets.text("invalidRecordsPath","")
dbutils.widgets.text("clientId","")
dbutils.widgets.text("keyVaultName","")


FileName=dbutils.widgets.get("fileName")
Filepath=dbutils.widgets.get("filePath")
Directory=dbutils.widgets.get("directory")

outputDirectory=dbutils.widgets.get("outputDirectory")
PipelineID=dbutils.widgets.get("pipelineID")
container=dbutils.widgets.get("Container")
mount_point=dbutils.widgets.get("MountPoint")
StoreDeltaPath=dbutils.widgets.get("deltaPath")
Archival_filePath=dbutils.widgets.get("archivalFilePath")
# Store_OutboundPath=dbutils.widgets.get("Store_OutboundPath")
Log_FilesPath=dbutils.widgets.get("logFilesPath")
Invalid_RecordsPath=dbutils.widgets.get("invalidRecordsPath")
clientId=dbutils.widgets.get("clientId")
keyVaultName=dbutils.widgets.get("keyVaultName")
PipelineID =dbutils.widgets.get("pipelineID")
Date = datetime.datetime.now(timezone("America/Halifax")).strftime("%Y-%m-%d")
inputSource= 'abfss://' + Directory + '@' + container + '.dfs.core.windows.net/'
outputSource= 'abfss://' + outputDirectory + '@' + container + '.dfs.core.windows.net/'
file_location = '/mnt' + '/' + Directory + '/' + Filepath +'/' + FileName 
Store_OutboundPath= '/mnt' + '/' + outputDirectory + '/Store/Outbound/CDM'
# source= "abfss://ahold-centralized-price-promo@rs06ue2dmasadata02.dfs.core.windows.net/"

# PipelineID = "temp"
# StoreDeltaPath='/mnt/ahold-centralized-price-promo/Store/Outbound/SDM/Store_delta'

# custom_logfile_Name ='store_dailymaintainence_customlog'
# Archival_filePath= '/mnt/ahold-centralized-price-promo/Store/Outbound/SDM/ArchivalRecords'
# mount_point = "/mnt/ahold-centralized-price-promo"
# Store_OutboundPath = "mnt/ahold-centralized-price-promo/Store/Outbound/CDM"
# Log_FilesPath = "/mnt/ahold-centralized-price-promo/Store/Outbound/SDM/Logfiles"
# source= "abfss://ahold-centralized-price-promo@rs06ue2dmasadata02.dfs.core.windows.net/"
# Invalid_RecordsPath = "/mnt/ahold-centralized-price-promo/Store/Outbound/SDM/InvalidRecords"
# file_location ="Fullstore/Inbound/RDS/2021/04/27/Test/TEST.NQ.GM.SMA.DB.STORE.LOAD.ZIP/TEST.NQ.GM.SMA.DB.STORE.LOAD.txt"

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

processing_file='Delta'
# if Directory.find('\COD') !=-1:
#   processing_file ='COD'
# elif Directory.find('\Full')!=-1:
#   processing_file ='FullStore'
# else:
#   processing_file

# COMMAND ----------

#Defining the schema for Store Table
loggerAtt.info('========Schema definition initiated ========')
storeRaw_schema = StructType([
                           StructField("DEST_STORE",StringType(),False),
                           StructField("STORE_NUM",IntegerType(),False),
                           StructField("BATCH_SERIAL",StringType(),False),
                           StructField("DAYOFWEEK",IntegerType(),False),
                           StructField("SUB_DEPT",IntegerType(),False),
                           StructField("CHNG_TYP",IntegerType(),False),
                           StructField("VEND_BREAK",IntegerType(),False),
                           StructField("POS_TYPE",StringType(),False),
                           StructField("PRIMARY_WIC_STATE",StringType(),False),
                           StructField("WIC_SCNDRY_STATE",StringType(),True),
                           StructField("STORE_DIVISION",IntegerType(),False),
                           StructField("STORE_ADDRESS",StringType(),False),
                           StructField("STORE_CITY",StringType(),False),
                           StructField("STORE_STATE",StringType(),False),
                           StructField("STORE_ZIP",IntegerType(),False),
                           StructField("STORE_OPEN_DTE",StringType(),False),
                           StructField("STORE_CLOSE_DTE",StringType(),False),
                           StructField("MAINT_LOOKAHEAD_DAYS",IntegerType(),False),
                           StructField("RETENTION_DAYS",IntegerType(),False),
                           StructField("STORE_BANNER",IntegerType(),False),
                           StructField("LAST_UPDATE_DATE",StringType(),False),
                           StructField("FILLER",StringType(),True) 
])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Delta table creation

# COMMAND ----------

try:
  ABC(DeltaTableCreateCheck=1)
  spark.sql("""
  CREATE TABLE IF NOT EXISTS Store_delta (
  DEST_STORE INTEGER,
  STORE_NUMBER String,
  BATCH_SERIAL INTEGER,
  DAYOFWEEK INTEGER,
  SUB_DEPT INTEGER,
  CHNG_TYP INTEGER,
  VEND_BREAK INTEGER,
  STAGING_AREA STRING,
  WIC_ELIGIBLE_IND STRING,
  WIC_SCNDRY_ELIGIBLE_STATE STRING,
  AREA INTEGER,
  ADD_1 STRING,
  CITY STRING,
  STATE STRING,
  ZIP INTEGER,
  STORE_OPEN_DATE DATE,
  STORE_CLOSE_DATE DATE,
  MAINT_LOOKAHEAD_DAYS INTEGER,
  RETENTION_DAYS INTEGER,
  STORE_BANNER INTEGER,
  LAST_UPDATE_DATE DATE,
  INSERT_ID STRING,
  INSERT_TIMESTAMP TIMESTAMP,
  LAST_UPDATE_ID STRING,
  LAST_UPDATE_TIMESTAMP TIMESTAMP,
  BANNER STRING
  )
  USING delta
  Location '{}'
  PARTITIONED BY (STORE_NUMBER)
  """.format(StoreDeltaPath))
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
# MAGIC ## Store Processing

# COMMAND ----------

def typeCheck(s):
  try: 
      int(s)
      return True
  except ValueError:
      return False

typeCheckUDF = udf(typeCheck)

date_func =  udf (lambda x: datetime.datetime.strptime(str(x), '%Y-%m-%d').date(), DateType())

#Column renaming functions 
def storeflat_storetable(s):
    return Store_Renaming[s]
def change_col_name(s):
    return s in Store_Renaming
  
def emptyCheck(s):

  if len(s) == 0 :
    return True
  else: return False
 
emptyCheckUDF = udf(emptyCheck)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Store Transformation

# COMMAND ----------

def storeTransformation(processed_df, JOB_ID):
  processed_df=processed_df.withColumn("WIC_ELIGIBLE_IND",when(col("WIC_ELIGIBLE_IND")==None, 'N').when(emptyCheckUDF(trim(col("WIC_ELIGIBLE_IND")))==True, 'N').otherwise('Y'))
#   processed_df=processed_df.withColumn("WIC_ELIGIBLE_IND",when(col("WIC_ELIGIBLE_IND")==None, 'Y').when(emptyCheckUDF(trim(col("WIC_ELIGIBLE_IND")))==True, 'Y').otherwise('N'))
  processed_df=processed_df.withColumn("AREA",col("AREA").cast(IntegerType()))
  processed_df=processed_df.withColumn("STORE_OPEN_DATE",when(col("STORE_OPEN_DATE")=='0000-00-00', lit(None)).otherwise(date_format(date_func(col("STORE_OPEN_DATE")), 'yyyy-MM-dd')).cast(DateType()))
  processed_df=processed_df.withColumn('DEST_STORE', lpad(col('DEST_STORE'),4,'0')) 
  processed_df=processed_df.withColumn('STORE_NUMBER', lpad(col('STORE_NUMBER'),4,'0'))                                      
#   processed_df=processed_df.withColumn("STORE_CLOSE_DATE",when(col("STORE_CLOSE_DATE")!='0000-00-00', date_format(date_func(col("STORE_CLOSE_DATE")), 'yyyy-MM-dd')).otherwise(lit(None)).cast(DateType()))
  processed_df = processed_df.withColumn("STORE_CLOSE_DATE",when(col("STORE_CLOSE_DATE")=="0000-00-00", lit(None)).when( col("STORE_CLOSE_DATE")!=lit(None),date_format(date_func(col("STORE_CLOSE_DATE")), 'yyyy-MM-dd')).cast(DateType()))
     
  processed_df=processed_df.withColumn("INSERT_TIMESTAMP",current_timestamp())
  processed_df=processed_df.withColumn("LAST_UPDATE_ID",lit(JOB_ID))
  processed_df=processed_df.withColumn("LAST_UPDATE_TIMESTAMP",current_timestamp())
  processed_df=processed_df.withColumn("INSERT_ID", lit(JOB_ID))
  processed_df=processed_df.withColumn("LAST_UPDATE_DATE",when(col("LAST_UPDATE_DATE")=='0000-00-00', lit(None)).otherwise(date_format(date_func(col("LAST_UPDATE_DATE")), 'yyyy-MM-dd')).cast(DateType()))
  processed_df=processed_df.withColumn("BANNER", lit('AHOLD'))
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

def StoreArchival(StoreDeltaPath,Date,Archivalfilelocation):
  storearchival_df = spark.read.format('delta').load(StoreDeltaPath)
  storearchival_df = storearchival_df.filter((datediff(to_date(current_date()),col('STORE_CLOSE_DATE')) >=90))
  if storearchival_df.count() >0:
    storearchival_df=storearchival_df.withColumn("STORE_CLOSE_DATE",col('STORE_CLOSE_DATE').cast(StringType()))
    storearchival_df=storearchival_df.withColumn("STORE_OPEN_DATE",col('STORE_OPEN_DATE').cast(StringType()))
    storearchival_df=storearchival_df.withColumn("LAST_UPDATE_DATE",col('LAST_UPDATE_DATE').cast(StringType()))
   
    storearchival_df.write.mode('Append').format('parquet').save(Archivalfilelocation + "/" +Date+ "/" +"Store_Archival_Records")
    deltaTable = DeltaTable.forPath(spark, StoreDeltaPath)
    deltaTable.delete((datediff(to_date(current_date()),col("STORE_CLOSE_DATE")) >=90))
#     add count
    loggerAtt.info('========Store Records Archival successful ========')
  else:
    loggerAtt.info('======== No Store Records Archival Done ========')
  return storearchival_df

# COMMAND ----------

def insertDelhaizeColumns(processed_df):
  processed_df=processed_df.withColumn("STORE_NAME", lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("STORE_TYPE", lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("ADDRESS_LINE_2", lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("PHONE", lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("MANAGERS_NAME", lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("PHARMACY_INDICATOR", lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("CATALOG_ID", lit(0))
  processed_df=processed_df.withColumn("FAX",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("PHONE_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("FAX_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("SUMMERHOURSYN",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("MONDAY",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("MONDAY_SUMMER",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("MONDAY_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("TUESDAY",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("TUESDAY_SUMMER",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("TUESDAY_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("WEDNESDAY",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("WEDNESDAY_SUMMER",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("WEDNESDAY_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("THURSDAY",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("THURSDAY_SUMMER",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("THURSDAY_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("FRIDAY",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("FRIDAY_SUMMER",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("FRIDAY_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("SATURDAY",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("SATURDAY_SUMMER",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("SATURDAY_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("SUNDAY",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("SUNDAY_SUMMER",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("SUNDAY_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("MLK",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("MLK_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("PRESIDENTS",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("PRESIDENTS_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("EASTER",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("EASTER_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("PATRIOTS",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("PATRIOTS_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("MEMORIAL",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("MEMORIAL_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("JULY4",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("JULY4_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("LABOR",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("LABOR_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("COLUMBUS",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("COLUMBUS_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("VETERANS",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("VETERANS_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("THXGIVING",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("THXGIVING_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("XMASEVE",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("XMASEVE_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("XMAS",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("XMAS_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("NEWYRSEVE",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("NEWYRSEVE_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("NEWYRS",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("NEWYRS_PHARM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("INDEPENDENT",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("WEBSITE",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("ATM",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("CHECK_CASHING",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("INSTORE_BANK",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("MONEY_ORDERS",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("MONEY_TRANSFERS",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("UTILITY_PAYMENTS",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("POSTAGE_STAMPS",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("LOTTERY_TICKETS",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("NEW_YORK_EZPASS",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("DUMP_PASSES",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("FISHING_LICENSES",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("COIN_COUNTING_KIOSK",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("DVD_RENTAL_KIOSK",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("BOTTLE_REDEMPTION",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("CLYNK_REDEMPTION",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("GREENBACK_REDEMPTION",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("TOWN_TRASH_BAGS",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("PROPANE_TANK_EXCHANGE",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("WATER_COOLER_JUGS",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("RUG_CLEANER_RENTALS",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("PARTY_BALLOONS",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("PARTY_PLATTERS_AND_FRUIT_BAS",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("CAKE_DECORATING",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("FLOWERS_AND_FLORAL_ARRANGEMEN",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("SUSHI",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("WINE",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("LIQUOR",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("CAFE",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("SHELLFISH_STEAMER",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("LOBSTER_TANK",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("DRIVE_THRU_RX",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("BLOOD_PRESSURE_MACHINE",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("NUTITIONIST",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("BUS_PARTNERSHIP",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("ISLAND_DELIVERY_SERVICE",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("ONLINE_WINE",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("SELF_SCAN",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("SALAD_BAR",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("OLIVE_BAR",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("WING_BAR",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("BEER",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("BUTCHER_SHOP",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("ISPU_INDICATOR",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("DIRECT_FLAG",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("PICKING_FEE_WAIVED",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("LIQUOR_STORE_UPDATE_FLAG",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("MONDAY_LIQUOR",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("TUESDAY_LIQUOR",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("WEDNESDAY_LIQUOR",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("THURSDAY_LIQUOR",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("FRIDAY_LIQUOR",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("SATURDAY_LIQUOR",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("SUNDAY_LIQUOR",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("PHONE_LIQUOR",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("PHYS_CLSE_DT",lit(None).cast(StringType()))
  processed_df=processed_df.withColumn("PHYS_CLSE_DT",date_format(col("PHYS_CLSE_DT"), 'yyyy/MM/dd').cast(StringType()))
  processed_df=processed_df.withColumnRenamed("PHYS_CLSE_DT", "CLOSED_DATE")
  processed_df=processed_df.withColumnRenamed("BANNER", "Banner_ID")
  processed_df=processed_df.withColumnRenamed("STAGING_AREA", "POS_TYPE")
  processed_df=processed_df.withColumnRenamed("AREA", "STORE_DIVISION")
  processed_df=processed_df.withColumnRenamed("ADD_1", "ADDRESS_LINE_1")
#   processed_df=processed_df.withColumn("INSERT_TIMESTAMP",col('INSERT_TIMESTAMP').cast(StringType()))
#   processed_df=processed_df.withColumn("LAST_UPDATE_TIMESTAMP",col('LAST_UPDATE_TIMESTAMP').cast(StringType()))
  processed_df=processed_df.withColumn("STORE_OPEN_DATE",date_format(col("STORE_OPEN_DATE"), 'yyyy/MM/dd').cast(StringType()))
  processed_df=processed_df.withColumn("STORE_CLOSE_DATE",date_format(col("STORE_CLOSE_DATE"), 'yyyy/MM/dd').cast(StringType()))
  processed_df=processed_df.withColumn("LAST_UPDATE_DATE",date_format(col("LAST_UPDATE_DATE"), 'yyyy/MM/dd').cast(StringType()))
  processed_df=processed_df.withColumn("DEST_STORE",col('DEST_STORE').cast(StringType()))
  processed_df=processed_df.withColumn("DAYOFWEEK",col('DAYOFWEEK').cast(StringType()))
  processed_df=processed_df.withColumn("SUB_DEPT",col('SUB_DEPT').cast(StringType()))
  processed_df=processed_df.withColumn("CHNG_TYP",col('CHNG_TYP').cast(StringType()))
  processed_df=processed_df.withColumn("POS_TYPE",col('POS_TYPE').cast(StringType()))
  processed_df=processed_df.withColumn("STORE_DIVISION",col('STORE_DIVISION').cast(StringType()))
  processed_df=processed_df.withColumn("STORE_NUMBER",col('STORE_NUMBER').cast(StringType()))
  processed_df=processed_df.withColumn("STORE_NUMBER",lpad(col('STORE_NUMBER'),4,'0'))
  processed_df=processed_df.select('Banner_ID', 'STORE_NUMBER', 'STORE_NAME', 'STORE_TYPE', 'ADDRESS_LINE_1', 'ADDRESS_LINE_2', 'CITY', 'STATE', 'ZIP', 'PHONE', 'MANAGERS_NAME', 'PHARMACY_INDICATOR', 'CATALOG_ID', 'FAX', 'PHONE_PHARM', 'FAX_PHARM', 'SUMMERHOURSYN', 'MONDAY', 'MONDAY_SUMMER', 'MONDAY_PHARM', 'TUESDAY', 'TUESDAY_SUMMER', 'TUESDAY_PHARM', 'WEDNESDAY', 'WEDNESDAY_SUMMER', 'WEDNESDAY_PHARM', 'THURSDAY', 'THURSDAY_SUMMER', 'THURSDAY_PHARM', 'FRIDAY', 'FRIDAY_SUMMER', 'FRIDAY_PHARM', 'SATURDAY', 'SATURDAY_SUMMER', 'SATURDAY_PHARM', 'SUNDAY', 'SUNDAY_SUMMER', 'SUNDAY_PHARM', 'MLK', 'MLK_PHARM', 'PRESIDENTS', 'PRESIDENTS_PHARM', 'EASTER', 'EASTER_PHARM', 'PATRIOTS', 'PATRIOTS_PHARM', 'MEMORIAL', 'MEMORIAL_PHARM', 'JULY4', 'JULY4_PHARM', 'LABOR', 'LABOR_PHARM', 'COLUMBUS', 'COLUMBUS_PHARM', 'VETERANS', 'VETERANS_PHARM', 'THXGIVING', 'THXGIVING_PHARM', 'XMASEVE', 'XMASEVE_PHARM', 'XMAS', 'XMAS_PHARM', 'NEWYRSEVE', 'NEWYRSEVE_PHARM', 'NEWYRS', 'NEWYRS_PHARM', 'INDEPENDENT', 'WEBSITE', 'ATM', 'CHECK_CASHING', 'INSTORE_BANK', 'MONEY_ORDERS', 'MONEY_TRANSFERS', 'UTILITY_PAYMENTS', 'POSTAGE_STAMPS', 'LOTTERY_TICKETS', 'NEW_YORK_EZPASS', 'DUMP_PASSES', 'FISHING_LICENSES', 'COIN_COUNTING_KIOSK', 'DVD_RENTAL_KIOSK', 'BOTTLE_REDEMPTION', 'CLYNK_REDEMPTION', 'GREENBACK_REDEMPTION', 'TOWN_TRASH_BAGS', 'PROPANE_TANK_EXCHANGE', 'WATER_COOLER_JUGS', 'RUG_CLEANER_RENTALS', 'PARTY_BALLOONS', 'PARTY_PLATTERS_AND_FRUIT_BAS', 'CAKE_DECORATING', 'FLOWERS_AND_FLORAL_ARRANGEMEN', 'SUSHI', 'WINE', 'LIQUOR', 'CAFE', 'SHELLFISH_STEAMER', 'LOBSTER_TANK', 'DRIVE_THRU_RX', 'BLOOD_PRESSURE_MACHINE', 'NUTITIONIST', 'BUS_PARTNERSHIP', 'ISLAND_DELIVERY_SERVICE', 'ONLINE_WINE', 'SELF_SCAN', 'SALAD_BAR', 'OLIVE_BAR', 'WING_BAR', 'BEER', 'BUTCHER_SHOP', 'ISPU_INDICATOR', 'DIRECT_FLAG', 'PICKING_FEE_WAIVED', 'LIQUOR_STORE_UPDATE_FLAG', 'MONDAY_LIQUOR', 'TUESDAY_LIQUOR', 'WEDNESDAY_LIQUOR', 'THURSDAY_LIQUOR', 'FRIDAY_LIQUOR', 'SATURDAY_LIQUOR', 'SUNDAY_LIQUOR', 'PHONE_LIQUOR', 'DEST_STORE', 'BATCH_SERIAL', 'DAYOFWEEK', 'SUB_DEPT', 'CHNG_TYP', 'VEND_BREAK', 'POS_TYPE', 'WIC_ELIGIBLE_IND', 'WIC_SCNDRY_ELIGIBLE_STATE', 'STORE_DIVISION', 'STORE_OPEN_DATE', 'STORE_CLOSE_DATE', 'MAINT_LOOKAHEAD_DAYS', 'RETENTION_DAYS', 'LAST_UPDATE_DATE', 'CLOSED_DATE', 'STORE_BANNER', 'INSERT_ID', 'INSERT_TIMESTAMP','LAST_UPDATE_ID', 'LAST_UPDATE_TIMESTAMP')
  return processed_df

# COMMAND ----------

def storeWrite(StoreDeltaPath,storeOutboundPath):
  storeOutputDf = spark.read.format('delta').load(StoreDeltaPath)

  if storeOutputDf.count() >0:
    storeOutputDf = insertDelhaizeColumns(storeOutputDf)
  
    ABC(ValidRecordCount=storeOutputDf.count())
    storeOutputDf = storeOutputDf.withColumn('CHAIN_ID', lit('AHOLD'))
    storeOutputDf.write.partitionBy('CHAIN_ID').mode('overwrite').format('parquet').save(storeOutboundPath + "/" +"Store_Output")
    loggerAtt.info('========Store Records Output successful ========')
  else:
    loggerAtt.info('======== No Store Records Output Done ========')
    ABC(ValidRecordCount=0)

# COMMAND ----------

# storeOutputDf = spark.read.format('delta').load(StoreDeltaPath)
# storeOutputDf = insertDelhaizeColumns(storeOutputDf)
# # len(storeOutputDf.columns)
# storeOutputDf.printSchema

# COMMAND ----------

def upsertStoreRecords(store_valid_Records):
  loggerAtt.info("Merge into Delta table initiated")
  temp_table_name = "storeRecords"

  store_valid_Records.createOrReplaceTempView(temp_table_name)
  try:
#   add before count
    initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(StoreDeltaPath))
    loggerAtt.info(f"Initial count of records in Delta Table: {initial_recs.head(1)}")
    initial_recs = initial_recs.head(1)
    ABC(DeltaTableInitCount=initial_recs[0][0])
    spark.sql('''MERGE INTO delta.`{}` as store
    USING storeRecords 
    ON store.STORE_NUMBER = storeRecords.STORE_NUMBER
    WHEN MATCHED Then 
            Update Set store.DEST_STORE=storeRecords.DEST_STORE,
                      store.STORE_NUMBER=storeRecords.STORE_NUMBER,
                      store.BATCH_SERIAL=storeRecords.BATCH_SERIAL,
                      store.DAYOFWEEK=storeRecords.DAYOFWEEK,
                      store.SUB_DEPT=storeRecords.SUB_DEPT,
                      store.CHNG_TYP=storeRecords.CHNG_TYP,
                      store.VEND_BREAK=storeRecords.VEND_BREAK,
                      store.STAGING_AREA=storeRecords.STAGING_AREA,
                      store.WIC_ELIGIBLE_IND=storeRecords.WIC_ELIGIBLE_IND,
                      store.WIC_SCNDRY_ELIGIBLE_STATE=storeRecords.WIC_SCNDRY_ELIGIBLE_STATE,
                      store.AREA=storeRecords.AREA,
                      store.ADD_1=storeRecords.ADD_1,
                      store.CITY=storeRecords.CITY,
                      store.STATE=storeRecords.STATE,
                      store.ZIP=storeRecords.ZIP,
                      store.STORE_OPEN_DATE=storeRecords.STORE_OPEN_DATE,
                      store.STORE_CLOSE_DATE=storeRecords.STORE_CLOSE_DATE,
                      store.MAINT_LOOKAHEAD_DAYS=storeRecords.MAINT_LOOKAHEAD_DAYS,
                      store.RETENTION_DAYS=storeRecords.RETENTION_DAYS,
                      store.STORE_BANNER=storeRecords.STORE_BANNER,
                      store.LAST_UPDATE_DATE=storeRecords.LAST_UPDATE_DATE,
                      store.INSERT_ID=storeRecords.INSERT_ID,
                      store.INSERT_TIMESTAMP=storeRecords.INSERT_TIMESTAMP,
                      store.LAST_UPDATE_ID=storeRecords.LAST_UPDATE_ID,
                      store.LAST_UPDATE_TIMESTAMP=storeRecords.LAST_UPDATE_TIMESTAMP,
                      store.BANNER=storeRecords.BANNER


                  WHEN NOT MATCHED THEN INSERT * '''.format(StoreDeltaPath))
#   add abc after count
    appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(StoreDeltaPath))
    loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
    appended_recs = appended_recs.head(1)
    ABC(DeltaTableFinalCount=appended_recs[0][0])
    loggerAtt.info("Merge into Delta table successful")

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

# COMMAND ----------

def writeInvalidRecords(store_invalidRecords, Invalid_RecordsPath):
  ABC(InvalidRecordSaveCheck = 1)
  try:
    if store_invalidRecords.count() > 0:
      store_invalidRecords.write.mode('Append').format('parquet').save(Invalid_RecordsPath + "/" +Date+ "/" + "Store_Invalid_Data")
      ABC(InvalidRecordCount = store_invalidRecords.count())
      loggerAtt.info('======== Invalid store Records write operation finished ========')

  except Exception as ex:
    ABC(InvalidRecordSaveCheck = 0)
    loggerAtt.error(str(ex))
    loggerAtt.info('======== Invalid record file write to ADLS location failed ========')
    err = ErrorReturn('Error', ex,'Writeoperationfailed for Invalid Record')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

def abcFramework(footerRecord, store_duplicate_records, store_raw_df):
  global fileRecordCount
  loggerAtt.info("ABC Framework function initiated")
  try:
    
    # Fetching EOF and BOF records and checking whether it is of length one each
    footerRecord = store_raw_df.filter(store_raw_df["DEST_STORE"].rlike("TRAILER"))

    ABC(EOFCheck=1)
    EOFcnt = footerRecord.count()

    ABC(EOFCount=EOFcnt)

    loggerAtt.info("EOF file record count is " + str(footerRecord.count()))
    
    # If there are multiple Footer then the file is invalid
    if (footerRecord.count() != 1):
      raise Exception('Error in EOF value')
    
    storeDf = store_raw_df.filter(typeCheckUDF(col('DEST_STORE')) == True)
#     StoreCnt =storeDf.count()
#     ABC(StoreCount = )

    

    footerRecord = footerRecord.withColumn('BATCH_SERIAL', substring('BATCH_SERIAL', 1, 8))
    fileRecordCount = int(footerRecord.select('BATCH_SERIAL').toPandas()['BATCH_SERIAL'][0])

    actualRecordCount = int(storeDf.count() + footerRecord.count() - 1)
    
    loggerAtt.info("Record Count mentioned in file: " + str(fileRecordCount))
    loggerAtt.info("Actual no of record in file: " + str(actualRecordCount))
    
    # Checking to see if the count matched the actual record count
    if actualRecordCount != fileRecordCount:
      raise Exception('Record count mismatch. Actual count ' + str(actualRecordCount) + ', file record count ' + str(fileRecordCount))
     
  except Exception as ex:
    ABC(BOFEOFCheck=0)
    EOFcnt = None

    ABC(EOFCount=EOFcnt)

    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'BOFEOFCheck')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

  
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
    return footerRecord, store_nullRows, store_duplicate_records, storeRecords
  except Exception as ex:
    ABC(DuplicateValueCheck = 0)
    ABC(DuplicateValueCount='')
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'store_ProblemRecs')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))


# COMMAND ----------

# footerRecord = store_raw_df.filter(store_raw_df["DEST_STORE"].rlike("TRAILER"))
# display(footerRecord)

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
  footerRecord = None
  storeRecords = None
  store_valid_Records = None
  PipelineID= str(PipelineID)
  folderDate = Date

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

  
  if store_raw_df is not None:

    ## Step2: Performing ABC framework check
 
    footerRecord, store_nullRows, store_duplicate_records, storeRecords = abcFramework(footerRecord, store_duplicate_records, store_raw_df)

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
    
    ## Step4: Combining all duplicate records
    try:
      ## Combining Duplicate, null record
      if store_duplicate_records is not None:
        invalidRecordsList = [footerRecord, store_duplicate_records, store_nullRows]
        store_invalidRecords = reduce(DataFrame.unionAll, invalidRecordsList)
        ABC(InvalidRecordSaveCheck = 1)
        ABC(InvalidRecordCount = store_invalidRecords.count())
      else:
        invalidRecordsList = [footerRecord, store_nullRows]
        store_invalidRecords = reduce(DataFrame.unionAll, invalidRecordsList)
     
    except Exception as ex:
      ABC(InvalidRecordSaveCheck = 0)
      ABC(InvalidRecordCount='')
      err = ErrorReturn('Error', ex,'Grouping Invalid record function')
      err = ErrorReturn('Error', ex,'InvalidRecordsSave')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
    store_valid_Records=store_valid_Records.withColumn("STORE_CLOSE_DATE",col('STORE_CLOSE_DATE').cast(StringType()))
    store_valid_Records=store_valid_Records.withColumn("STORE_OPEN_DATE",col('STORE_OPEN_DATE').cast(StringType()))
    store_valid_Records=store_valid_Records.withColumn("LAST_UPDATE_DATE",col('LAST_UPDATE_DATE').cast(StringType()))
    store_invalidRecords=store_invalidRecords.withColumn("DEST_STORE",col('DEST_STORE').cast(IntegerType()))
    store_invalidRecords=store_invalidRecords.withColumn("STORE_OPEN_DTE",col('STORE_OPEN_DTE').cast(StringType()))
    store_invalidRecords=store_invalidRecords.withColumn("LAST_UPDATE_DATE",col('LAST_UPDATE_DATE').cast(StringType()))
    store_invalidRecords=store_invalidRecords.withColumn("LAST_UPDATE_DATE",col('LAST_UPDATE_DATE').cast(StringType()))
    loggerAtt.info("No of valid records: " + str(store_valid_Records.count()))
    loggerAtt.info("No of invalid records: " + str(store_invalidRecords.count()))
    
    ## Step5: Merging records into delta table
    if store_valid_Records is not None:
        upsertStoreRecords(store_valid_Records)
        store_valid_Records = insertDelhaizeColumns(store_valid_Records)
    ## Step6: Writing Invalid records to ADLS location
    if store_invalidRecords is not None:
      try:
        writeInvalidRecords(store_invalidRecords, Invalid_RecordsPath)
      except Exception as ex:
        ABC(InvalidRecordSaveCheck = 0)
        ABC(InvalidRecordCount = '')
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'writeInvalidRecord')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
#     try:
#       ABC(itemMasterRecordsCheck=1)
#       itemMasterRecordsModified(itemTempEffDeltaPath, storeEffDeltaPath, StoreDeltaPath, Date, unifiedStoreFields)  
#     except Exception as ex:
#       ABC(itemMasterCount='')
#       ABC(itemMasterRecordsCheck=0)
#       err = ErrorReturn('Error', ex,'itemMasterRecordsModified')
#       loggerAtt.error(ex)
#       errJson = jsonpickle.encode(err)
#       errJson = json.loads(errJson)
#       dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
      
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
      ABC(storeWriteCheck=1)
      storeWrite(StoreDeltaPath,Store_OutboundPath)
    except Exception as ex:
      ABC(ValidRecordCount='')
      ABC(storeWriteCheck=0)
      err = ErrorReturn('Error', ex,'storeWrite')
      loggerAtt.error(ex)
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
  else:
    loggerAtt.info("Error in input file reading")
    
     
    
loggerAtt.info('======== Input store file processing ended ========')

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

updateDeltaVersioning('Ahold', 'store', PipelineID, Filepath, FileName)

# COMMAND ----------

# MAGIC  %md 
# MAGIC  
# MAGIC  ##write processed store files to ADLS 

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

df = spark.read.format('parquet').load("/mnt/ahold-centralized-price-promo/Store/Outbound/CDM" +"/2021-02-04/" + "Store_Maintainence_Data")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Store_delta

# COMMAND ----------

