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

custom_logfile_Name ='coupon_dailymaintainence_customlog'
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
dbutils.widgets.text("fileName","")
dbutils.widgets.text("filePath","")
dbutils.widgets.text("directory","")
dbutils.widgets.text("outputDirectory","")
dbutils.widgets.text("Container","")
dbutils.widgets.text("pipelineID","")
dbutils.widgets.text("MountPoint","")
dbutils.widgets.text("deltaPath","")
dbutils.widgets.text("archivalFilePath","")
dbutils.widgets.text("couponOutboundPath","")
dbutils.widgets.text("logFilesPath","")
dbutils.widgets.text("invalidRecordsPath","")
dbutils.widgets.text("clientId","")
dbutils.widgets.text("keyVaultName","")

FileName=dbutils.widgets.get("fileName")
Filepath=dbutils.widgets.get("filePath")
inputDirectory=dbutils.widgets.get("directory")
outputDirectory=dbutils.widgets.get("outputDirectory")
container=dbutils.widgets.get("Container")
PipelineID=dbutils.widgets.get("pipelineID")
mount_point=dbutils.widgets.get("MountPoint")
CouponDeltaPath=dbutils.widgets.get("deltaPath")
Archivalfilelocation=dbutils.widgets.get("archivalFilePath")
Coupon_OutboundPath=dbutils.widgets.get("couponOutboundPath")
Log_FilesPath=dbutils.widgets.get("logFilesPath")
Invalid_RecordsPath=dbutils.widgets.get("invalidRecordsPath")
Date = datetime.datetime.now(timezone("America/Halifax")).strftime("%Y-%m-%d")
file_location = '/mnt' + '/' + inputDirectory + '/' + Filepath +'/' + FileName 
clientId=dbutils.widgets.get("clientId")
keyVaultName=dbutils.widgets.get("keyVaultName")

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
# MAGIC ## Declarations

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Field Declaration

# COMMAND ----------

## File reading parameters
file_location = str(file_location)
file_type = "csv"
infer_schema = "false"
first_row_is_header = "true"
delimiter = "|"
Coupon_transformed_df = None
raw_df = None
coupon_duplicate_records = None
invalid_transformed_df = None
PipelineID= str(PipelineID)

p_filename = "coupon_dailymaintainence_custom_log"
folderDate = Date


processing_file='Delta'
if Filepath.find('COD') !=-1:
  processing_file ='COD'
elif Filepath.find('Full')!=-1:
  processing_file ='FullCoupon'
else:
  processing_file

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Coupon Schema definition

# COMMAND ----------

#Defining the schema for Coupon Table
loggerAtt.info('========Schema definition initiated ========')
couponRaw_schema = StructType([
                           StructField("DEST_STR",StringType(),False),
                           StructField("STORE",IntegerType(),False),
                           StructField("BATCH_SERIAL",StringType(),False),
                           StructField("DAYOFWEEK",IntegerType(),False),
                           StructField("SUB DEPT",IntegerType(),False),
                           StructField("CHANGE_TYPE_NotUsed",IntegerType(),False),
                           StructField("VENDOR",IntegerType(),False),
                           StructField("CPN HDR LOCATION",LongType(),False),
                           StructField("POS_SYSTEM",StringType(),True),
                           StructField("RCRD_TYPE",IntegerType(),False),
                           StructField("MAINT TYPE",IntegerType(),False),
                           StructField("CPN_APPLY_DATE",IntegerType(),False),  #date or integer
                           StructField("CPN_APPLY_TIME",StringType(),False),  #date or integer
                           StructField("CPN HDR COUPON",LongType(),False),
                           StructField("DMD PROMO ID",LongType(),False),
                           StructField("CPN START DATE",StringType(),False),
                           StructField("CPN END DATE",StringType(),False),
                           StructField("CPN DELETE DATE",StringType(),False),
                           StructField("DT PERF DTL SUB TYPE",IntegerType(),False),
                           StructField("CHG TYPE",StringType(),False),
                           StructField("NUMBER TO BUY 1",IntegerType(),False),
                           StructField("VALUE TO BUY 1",DoubleType(),False),
                           StructField("FILLER1",StringType(),True),
                           StructField("COUPON_VALUE",DoubleType(),False), # String type
                           StructField("LIMIT",StringType(),False),
                           StructField("SUB_DEPARTMENT",IntegerType(),False),
                           StructField("FAMCD1 OR PROMOCD1",IntegerType(),False),
                           StructField("DESCRIPTION",StringType(),True),
                           StructField("VALUE REQUIRED IND",StringType(),True),
                           StructField("CARD_REQUIRED_IND",StringType(),True),
                           StructField("CPN WEIGHT LIMIT IND",StringType(),True),
                           StructField("SELL BY WEIGHT IND",StringType(),False),
                           StructField("QTY_NOT_ALLOWED_IND",StringType(),True),
                           StructField("MULTIPLE_NOT_ALLOWED_IND",StringType(),True),
                           StructField("DISCOUNT_NOT_ALLOWED_IND",StringType(),True),
                           StructField("SEP MIN PURCH IND",StringType(),True),
                           StructField("EXCLUDE MIN PURCH IND",StringType(),True),
                           StructField("FSA_ELIGIBLE_IND",StringType(),True),
                           StructField("FOOD STAMP IND",StringType(),True),
                           #StructField("CURRENCY_CODE",StringType(),True),
                           StructField("FUEL_ITEM_FLAG",StringType(),True),
                           StructField("WIC_FLAG",StringType(),True),
                           StructField("WIC_DISPLAY_FLAG",StringType(),True),
                           StructField("PROMO_COMP_TYPE",IntegerType(),True),
                           StructField("APPLY_TO_CODE",IntegerType(),True),
                           StructField("SELLING_UOM",StringType(),True),
                           StructField("GC OPT4 NETAMT IND",StringType(),True),
                           StructField("GC MIN AMT MFG IND",StringType(),True),
                           StructField("GC MIN AMT DEPT IND",StringType(),True),
                           StructField("GC MFG NUMBER",IntegerType(),False),
                           StructField("GC VENDOR FAMCD1",IntegerType(),False),
                           StructField("GC VENDOR FAMCD2",IntegerType(),False),
                           StructField("TAX PLAN 1 IND",StringType(),True),
                           StructField("TAX PLAN 2 IND",StringType(),True),
                           StructField("TAX PLAN 3 IND",StringType(),True),
                           StructField("TAX PLAN 4 IND",StringType(),True),
                           StructField("TAX PLAN 5 IND",StringType(),True),
                           StructField("TAX PLAN 6 IND",StringType(),True),
                           StructField("TAX PLAN 7 IND",StringType(),True),
                           StructField("TAX PLAN 8 IND",StringType(),True),
                           StructField("BTGT COUPON",LongType(),True),
                           StructField("GC LOG EXCEPTIONS",StringType(),True),
                           StructField("MUST_BUY_IND",StringType(),True),
                           StructField("Filler",StringType(),True)
])
loggerAtt.info('========Schema definition ended ========')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Delta table creation

# COMMAND ----------

# %sql

# select * from delta.`/mnt/ahold-centralized-price-promo/Coupon/Outbound/SDM/Coupon_Delta`

# COMMAND ----------

# spark.sql("""DELETE FROM CouponAholdTable;""")
# spark.sql("""DROP TABLE IF EXISTS CouponAholdTable;""")
# dbutils.fs.rm('/mnt/ahold-centralized-price-promo/Coupon/Outbound/SDM/Coupon_Delta',recurse=True)

# COMMAND ----------

# %sql

# select * from CouponAholdTable where store=0020 and coupon_no in (41100247495, 41100240836, 41100242292, 41100240949, 41100212779)

# COMMAND ----------

try:
    ABC(DeltaTableCreateCheck=1)
    spark.sql("""
    CREATE TABLE IF NOT EXISTS CouponAholdTable (
    DEST_STR INTEGER,
    STORE INTEGER,
    BATCH_SERIAL INTEGER,
    DAYOFWEEK INTEGER,
    DIVISION INTEGER,
    CHANGE_TYPE_NotUsed INTEGER,
    VENDOR INTEGER,
    LOCATION STRING,
    POS_SYSTEM STRING,
    RCRD_TYPE INTEGER,
    STATUS STRING,
    CPN_APPLY_DATE DATE,
    CPN_APPLY_TIME STRING,
    COUPON_NO LONG,
    AHO_PERF_DETAIL_ID LONG,
    START_DATE DATE,
    END_DATE DATE,
    DEL_DATE DATE,
    PERF_DETL_SUB_TYPE INTEGER,
    CHANGE_TYPE STRING,
    NUM_TO_BUY_1 STRING,
    VAL_TO_BUY_1 DOUBLE,
    COUPON_VALUE DOUBLE,
    LIMIT STRING,
    SUB_DEPARTMENT INTEGER,
    VALIDATION_CODE_1 INTEGER,
    DESCRIPTION STRING,
    VALUE_REQUIRED STRING,
    CARD_REQUIRED_IND STRING,
    CPN_WGT_LIMIT_FLG STRING,
    SELL_BY_WEIGHT_IND STRING,
    QTY_NOT_ALLOWED_IND STRING,
    MULTIPLE_NOT_ALLOWED_IND STRING,
    DISCOUNT_NOT_ALLOWED_IND STRING,
    SEP_MIN_PURCH_SEP STRING,
    EXCLUDE_MIN_PURCH_IND STRING,
    FSA_ELIGIBLE_IND STRING,
    FOOD_STAMP_IND STRING,
    FUEL_ITEM_FLAG STRING,
    WIC_FLAG STRING,
    WIC_DISPLAY_FLAG STRING,
    PROMO_COMP_TYPE INTEGER,
    APPLY_TO_CODE INTEGER,
    SELLING_UOM STRING,
    NET_AMT_FLG STRING,
    MIN_MFG_FLG STRING,
    MIN_DEPT_FLG STRING,
    MFG_NUM INTEGER,
    VEN_CPN_FAMCODE_1 INTEGER,
    VEN_CPN_FAMCODE_2 INTEGER,
    TAX_PLAN1 STRING,
    TAX_PLAN2 STRING,
    TAX_PLAN3 STRING,
    TAX_PLAN4 STRING,
    TAX_PLAN5 STRING,
    TAX_PLAN6 STRING,
    TAX_PLAN7 STRING,
    TAX_PLAN8 STRING,
    BTGT_CPN_LINK LONG,
    LOG_EXCEPTIONS STRING,
    MUST_BUY_IND STRING,
    CHANGE_AMOUNT DOUBLE,
    CHANGE_PERCENT DOUBLE,
    CHANGE_CURRENCY STRING,
    CLUB_CARD STRING,
    CHANGE_AMOUNT_PCT DOUBLE,
    MIN_QUANTITY INTEGER,
    BUY_QUANTITY INTEGER,
    GET_QUANTITY INTEGER,
    SALE_QUANTITY INTEGER,
    INSERT_ID STRING,
    INSERT_TIMESTAMP TIMESTAMP,
    LAST_UPDATE_ID STRING,
    LAST_UPDATE_TIMESTAMP TIMESTAMP,
    BANNER STRING
    )
    USING delta
    Location '{}'
    PARTITIONED BY (LOCATION)
    """.format(CouponDeltaPath))
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
# MAGIC ### UDF

# COMMAND ----------

date_func =  udf (lambda x: datetime.datetime.strptime(str(x), '%Y%m%d'), DateType())

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

def typeCheck(s):
  try: 
      int(s)
      return True
  except ValueError:
      return False

typeCheckUDF = udf(typeCheck)

#Column renaming functions 
def couponflat_coupontable(s):
    return Coupon_Renaming[s]
def change_col_name(s):
    return s in Coupon_Renaming

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Coupon transformation

# COMMAND ----------

def couponTransformation(processed_df,pipelineid):
  invalid_transformation_df=None
  processed_df=processed_df.withColumn("VEN_CPN_FAMCODE_2",when(col("VEN_CPN_FAMCODE_2")=='000',lit(None)).otherwise(col("VEN_CPN_FAMCODE_2")))
  processed_df=processed_df.withColumn("MFG_NUM",when(col("MFG_NUM")== '000',lit(None)).otherwise(col("MFG_NUM")))
  processed_df=processed_df.withColumn("NUM_TO_BUY_1",when(col("NUM_TO_BUY_1")=='000',lit(None)).otherwise(col("NUM_TO_BUY_1")))
  processed_df=processed_df.withColumn("VALIDATION_CODE_1",when(col("VALIDATION_CODE_1")=='000000',lit(None)).otherwise(col("VALIDATION_CODE_1")))
  processed_df=processed_df.withColumn("BTGT_CPN_LINK",when(col("BTGT_CPN_LINK")=='00000000000000',lit(None)).otherwise(col("BTGT_CPN_LINK")))
  processed_df=processed_df.withColumn('STATUS', statusChangeUDF(processed_df['STATUS']))
  invalid_transformation_df=processed_df.filter((col("STATUS")!= 'D') & (col("STATUS")!= 'C') & (col("STATUS")!= 'M'))
  processed_df=processed_df.filter((col("STATUS")== 'D') | (col("STATUS")== 'C') | (col("STATUS")== 'M'))
  processed_df=processed_df.withColumn('START_DATE', to_date(date_format(date_func(col('START_DATE')), 'yyyy-MM-dd')))
  processed_df=processed_df.withColumn('END_DATE',to_date(date_format(date_func(col('END_DATE')), 'yyyy-MM-dd')))
  processed_df=processed_df.withColumn('CPN_APPLY_DATE',to_date(date_format(date_func(col('CPN_APPLY_DATE')), 'yyyy-MM-dd')))
  processed_df=processed_df.withColumn("CPN_APPLY_TIME", from_unixtime(unix_timestamp(lpad(col('CPN_APPLY_TIME'),6,'0'), "HHmmss"),"HH:mm:ss"))
  processed_df=processed_df.withColumn('LOCATION', lpad(col('LOCATION'),4,'0')) #imported sql function  lpad to perform left padding 
  processed_df=processed_df.withColumn('DEL_DATE', to_date(date_format(date_func(col('DEL_DATE')), 'yyyy-MM-dd')))
  processed_df=processed_df.withColumn('CHANGE_TYPE', when(processed_df['STATUS'] == 'D',lit(None)).otherwise(processed_df['CHANGE_TYPE']))
  processed_df=processed_df.withColumn('CHANGE_AMOUNT',when(col('PERF_DETL_SUB_TYPE') !=4,col('COUPON_VALUE')).otherwise(lit(None)))
  processed_df=processed_df.withColumn('CHANGE_PERCENT',when(col('PERF_DETL_SUB_TYPE') == 4,col('COUPON_VALUE')).otherwise(lit(None)))
  processed_df=processed_df.withColumn('CHANGE_CURRENCY',concat(col('FUEL_ITEM_FLAG'),lit('.'), col('WIC_FLAG'),lit('.'),col('WIC_DISPLAY_FLAG')))
  processed_df=processed_df.withColumn('VEN_CPN_FAMCODE_1',when(col('VEN_CPN_FAMCODE_1') == '000',lit(None)).otherwise(col('VEN_CPN_FAMCODE_1')))
  processed_df=processed_df.withColumn('AHO_PERF_DETAIL_ID',when(col('AHO_PERF_DETAIL_ID') == '0000000000',lit(None)).otherwise(col('AHO_PERF_DETAIL_ID')))
  processed_df=processed_df.withColumn('CLUB_CARD',when(col('COUPON_NO').between('41100000001','41100799999'),'Y').otherwise('N'))
  processed_df=processed_df.withColumn('DESCRIPTION',lit(None).cast(StringType()))
  #   Ecommerce calcuated Attributes
  processed_df=processed_df.withColumn("CHANGE_AMOUNT_PCT",when(col('PERF_DETL_SUB_TYPE') ==4,col('CHANGE_PERCENT')).otherwise(col("CHANGE_AMOUNT")))
  processed_df=processed_df.withColumn("CHANGE_AMOUNT_PCT",col('CHANGE_AMOUNT_PCT').cast(DoubleType()))
  processed_df=processed_df.withColumn("MIN_QUANTITY",when(col('PERF_DETL_SUB_TYPE') == 9,col("NUM_TO_BUY_1")).otherwise('1').cast(IntegerType()))
  processed_df=processed_df.withColumn("BUY_QUANTITY",when(col('PERF_DETL_SUB_TYPE') == 9,(col("NUM_TO_BUY_1")-1)).otherwise('0').cast(IntegerType()))
  processed_df=processed_df.withColumn("GET_QUANTITY",when(col('PERF_DETL_SUB_TYPE') == 9,'1').otherwise('0').cast(IntegerType()))
  processed_df=processed_df.withColumn("SALE_QUANTITY",when((col('PERF_DETL_SUB_TYPE') == 8) | (col('PERF_DETL_SUB_TYPE') ==9),col("NUM_TO_BUY_1")).otherwise('1').cast(IntegerType()))
  processed_df=processed_df.withColumn("INSERT_ID",lit(pipelineid))
  processed_df=processed_df.withColumn("INSERT_TIMESTAMP",current_timestamp())
  processed_df=processed_df.withColumn("LAST_UPDATE_ID",lit(pipelineid))
  processed_df=processed_df.withColumn("LAST_UPDATE_TIMESTAMP",current_timestamp())
  processed_df=processed_df.withColumn("BANNER",lit('AHOLD'))
  processed_df=processed_df.drop('FILLER1')
  processed_df=processed_df.drop('Filler')
  return processed_df, invalid_transformation_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Coupon renaming dictionary

# COMMAND ----------

Coupon_Renaming = {
                    "CPN HDR COUPON": "COUPON_NO",
                    "CPN HDR LOCATION": "LOCATION",
                    "CPN START DATE": "START_DATE",
                    "CPN END DATE": "END_DATE",
                    "CPN DELETE DATE": "DEL_DATE",
                    "MAINT TYPE": "STATUS",
                    "DT PERF DTL SUB TYPE": "PERF_DETL_SUB_TYPE",
                    "CHG TYPE":"CHANGE_TYPE",
                    "DMD PROMO ID": "AHO_PERF_DETAIL_ID",
                    "NUMBER TO BUY 1": "NUM_TO_BUY_1",
                    "SUB DEPT": "DIVISION",
                    "SELL BY WEIGHT IND": "SELL_BY_WEIGHT_IND",
                    "TAX PLAN 1 IND": "TAX_PLAN1",
                    "TAX PLAN 2 IND": "TAX_PLAN2",
                    "TAX PLAN 3 IND": "TAX_PLAN3",
                    "TAX PLAN 4 IND": "TAX_PLAN4",
                    "TAX PLAN 5 IND": "TAX_PLAN5",
                    "TAX PLAN 6 IND": "TAX_PLAN6",
                    "TAX PLAN 7 IND": "TAX_PLAN7",
                    "TAX PLAN 8 IND": "TAX_PLAN8",
                    "CPN WEIGHT LIMIT IND": "CPN_WGT_LIMIT_FLG",
                    "SEP MIN PURCH IND": "SEP_MIN_PURCH_SEP",
                    "EXCLUDE MIN PURCH IND": "EXCLUDE_MIN_PURCH_IND",
                    "GC MIN AMT DEPT IND": "MIN_DEPT_FLG",
                    "GC MIN AMT MFG IND": "MIN_MFG_FLG",
                    "GC OPT4 NETAMT IND": "NET_AMT_FLG",
                    "GC LOG EXCEPTIONS": "LOG_EXCEPTIONS",
                    "GC MFG NUMBER": "MFG_NUM",
                    "GC VENDOR FAMCD1" : "VEN_CPN_FAMCODE_1",
                    "GC VENDOR FAMCD2": "VEN_CPN_FAMCODE_2",
                    "FAMCD1 OR PROMOCD1" : "VALIDATION_CODE_1",
                    "BTGT COUPON" : "BTGT_CPN_LINK",
                    "VALUE TO BUY 1": "VAL_TO_BUY_1",
                    "FOOD STAMP IND":"FOOD_STAMP_IND",
                    "VALUE REQUIRED IND":"VALUE_REQUIRED"
          }


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Coupon file reading

# COMMAND ----------

def readFile(file_location, infer_schema, first_row_is_header, delimiter,file_type):
  raw_df = spark.read.format(file_type) \
    .option("mode","PERMISSIVE") \
    .option("header", first_row_is_header) \
    .option("dateFormat", "yyyyMMdd") \
    .option("sep", delimiter) \
    .schema(couponRaw_schema) \
    .load(file_location)
  
  ABC(ReadDataCheck=1)
  RawDataCount = raw_df.count()
  ABC(RawDataCount=RawDataCount)
  loggerAtt.info(f"Count of Records in the File: {RawDataCount}")
  
  return raw_df


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write invalid records into ADLS

# COMMAND ----------

def writeInvalidRecord(coupon_invalidRecords, Invalid_RecordsPath, Date):
  ABC(InvalidRecordSaveCheck = 1)
  if coupon_invalidRecords is not None:
    if coupon_invalidRecords.count() > 0:
      invalidCount = coupon_invalidRecords.count()
      loggerAtt.info(f"Count of Invalid Records in the File: {invalidCount}")
      ABC(InvalidRecordCount = invalidCount)
      coupon_invalidRecords=coupon_invalidRecords.withColumn("START_DATE",date_format(col("START_DATE"), 'yyyy/MM/dd').cast(StringType()))
      coupon_invalidRecords=coupon_invalidRecords.withColumn("END_DATE",date_format(col("END_DATE"), 'yyyy/MM/dd').cast(StringType()))
      coupon_invalidRecords=coupon_invalidRecords.withColumn("DEL_DATE",date_format(col("DEL_DATE"), 'yyyy/MM/dd').cast(StringType()))
      coupon_invalidRecords=coupon_invalidRecords.withColumn('COUPON_NO', lpad(col('COUPON_NO'),14,'0') )

      coupon_invalidRecords.write.mode('Append').format('parquet').save(Invalid_RecordsPath + "/" +Date+ "/" + "Invalid_Data")
      loggerAtt.info('======== Invalid Coupon Records write operation finished ========')
    else:
      loggerAtt.info('======== No Invalid Coupon Records ========')
  else:
    loggerAtt.info('======== No Invalid Coupon Records ========')


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SQL Functions

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Fetch coupon delete status record

# COMMAND ----------

def fetchCouponDeleteRec(pipelineID,CouponDeltaPath):
  # spark.sql('''select * from delta '/mnt/ahold-centralized-price-promo/Coupon/DeltaTable''')
  couponDelta_df = spark.read.format('delta').load(CouponDeltaPath)
  if couponDelta_df is not None:
    if couponDelta_df.count() > 0:
      couponDelta_df = couponDelta_df.filter((col("STATUS")!= 'D') & (col('DEL_DATE') <= current_date()))
      couponDelta_df = couponDelta_df.withColumn("STATUS",when(col("STATUS")!= 'D','D').otherwise(col("STATUS")))
      couponDelta_df = couponDelta_df.withColumn("LAST_UPDATE_TIMESTAMP",current_timestamp())
      couponDelta_df = couponDelta_df.withColumn("LAST_UPDATE_ID",lit(pipelineID))
      deleteCount = couponDelta_df.count()
      ABC(DeleteCouponCount = deleteCount)
      loggerAtt.info(f"No of Coupon delte records are {deleteCount}")
  return couponDelta_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Coupon archival

# COMMAND ----------

def CouponArchival(CouponDeltaPath,Date,Archivalfilelocation):
  ABC(CouponArchivalCheck=1)
  couponarchival_df = spark.read.format('delta').load(CouponDeltaPath)
  if couponarchival_df is not None:
    
    initial_recs = couponarchival_df.count()
    loggerAtt.info(f"Initial count of records in delta table: {initial_recs}")
    ABC(archivalInitCount=initial_recs)
    
    couponarchival_df = couponarchival_df.filter((col("STATUS")== 'D') &  (datediff(to_date(current_date()),col('DEL_DATE')) >=7)) 
    if couponarchival_df.count() >0:
      couponarchival_df=couponarchival_df.withColumn("START_DATE",date_format(col("START_DATE"), 'yyyy/MM/dd').cast(StringType()))
      couponarchival_df=couponarchival_df.withColumn("END_DATE",date_format(col("END_DATE"), 'yyyy/MM/dd').cast(StringType()))
      couponarchival_df=couponarchival_df.withColumn("DEL_DATE",date_format(col("DEL_DATE"), 'yyyy/MM/dd').cast(StringType()))
      couponarchival_df=couponarchival_df.withColumn('COUPON_NO', lpad(col('COUPON_NO'),14,'0'))
      couponarchival_df.write.mode('Append').format('parquet').save(Archivalfilelocation + "/" +Date+ "/" +"Coupon_Archival_Records")
      deltaTable = DeltaTable.forPath(spark, CouponDeltaPath)
      deltaTable.delete((col("STATUS") == "D") &  (datediff(to_date(current_date()),col("DEL_DATE")) >=7))
      
      after_recs = spark.read.format('delta').load(CouponDeltaPath).count()
      loggerAtt.info(f"After count of records in delta table: {after_recs}")
      ABC(archivalAfterCount=after_recs)
      
      loggerAtt.info('========Coupon Records Archival successful ========')
    else:
      loggerAtt.info('======== No Coupon Records Archival Done ========')
  else:
    loggerAtt.info('======== No Coupon Records Archival Done ========')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge into Delta table 

# COMMAND ----------

def upsertCouponRecords(coupon_raw_df, CouponDeltaPath):
  loggerAtt.info("Merge into Delta table initiated")
  temp_table_name = "couponRecords"

  coupon_raw_df.createOrReplaceTempView(temp_table_name)
  
  initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(CouponDeltaPath))
  loggerAtt.info(f"Initial count of records in Delta Table: {initial_recs.head(1)}")
  initial_recs = initial_recs.head(1)

  ABC(DeltaTableInitCount=initial_recs[0][0])
  
  spark.sql('''MERGE INTO delta.`{}` as coupon
  USING couponRecords 
  ON coupon.COUPON_NO = couponRecords.COUPON_NO and coupon.LOCATION = couponRecords.LOCATION
  WHEN MATCHED Then 
          Update Set coupon.DEST_STR=couponRecords.DEST_STR,
                    coupon.STORE=couponRecords.STORE,
                    coupon.BATCH_SERIAL=couponRecords.BATCH_SERIAL,
                    coupon.DAYOFWEEK=couponRecords.DAYOFWEEK,
                    coupon.DIVISION=couponRecords.DIVISION,
                    coupon.VENDOR=couponRecords.VENDOR,
                    coupon.LOCATION=couponRecords.LOCATION,
                    coupon.POS_SYSTEM=couponRecords.POS_SYSTEM,
                    coupon.RCRD_TYPE=couponRecords.RCRD_TYPE,
                    coupon.STATUS=couponRecords.STATUS,
                    coupon.CPN_APPLY_DATE=couponRecords.CPN_APPLY_DATE,
                    coupon.CPN_APPLY_TIME=couponRecords.CPN_APPLY_TIME,
                    coupon.COUPON_NO=couponRecords.COUPON_NO,
                    coupon.AHO_PERF_DETAIL_ID=couponRecords.AHO_PERF_DETAIL_ID,
                    coupon.START_DATE=couponRecords.START_DATE,
                    coupon.END_DATE=couponRecords.END_DATE,
                    coupon.DEL_DATE=couponRecords.DEL_DATE,
                    coupon.PERF_DETL_SUB_TYPE=couponRecords.PERF_DETL_SUB_TYPE,
                    coupon.CHANGE_TYPE=couponRecords.CHANGE_TYPE,
                    coupon.NUM_TO_BUY_1=couponRecords.NUM_TO_BUY_1,
                    coupon.VAL_TO_BUY_1=couponRecords.VAL_TO_BUY_1,
                    coupon.COUPON_VALUE=couponRecords.COUPON_VALUE,
                    coupon.LIMIT=couponRecords.LIMIT,
                    coupon.SUB_DEPARTMENT=couponRecords.SUB_DEPARTMENT,
                    coupon.VALIDATION_CODE_1=couponRecords.VALIDATION_CODE_1,
                    coupon.DESCRIPTION=couponRecords.DESCRIPTION,
                    coupon.VALUE_REQUIRED=couponRecords.VALUE_REQUIRED,
                    coupon.CARD_REQUIRED_IND=couponRecords.CARD_REQUIRED_IND,
                    coupon.CPN_WGT_LIMIT_FLG=couponRecords.CPN_WGT_LIMIT_FLG,
                    coupon.SELL_BY_WEIGHT_IND=couponRecords.SELL_BY_WEIGHT_IND,
                    coupon.QTY_NOT_ALLOWED_IND=couponRecords.QTY_NOT_ALLOWED_IND,
                    coupon.MULTIPLE_NOT_ALLOWED_IND=couponRecords.MULTIPLE_NOT_ALLOWED_IND,
                    coupon.DISCOUNT_NOT_ALLOWED_IND=couponRecords.DISCOUNT_NOT_ALLOWED_IND,
                    coupon.SEP_MIN_PURCH_SEP=couponRecords.SEP_MIN_PURCH_SEP,
                    coupon.EXCLUDE_MIN_PURCH_IND=couponRecords.EXCLUDE_MIN_PURCH_IND,
                    coupon.FSA_ELIGIBLE_IND=couponRecords.FSA_ELIGIBLE_IND,
                    coupon.FOOD_STAMP_IND=couponRecords.FOOD_STAMP_IND,
                    coupon.FUEL_ITEM_FLAG=couponRecords.FUEL_ITEM_FLAG,
                    coupon.WIC_FLAG=couponRecords.WIC_FLAG,
                    coupon.WIC_DISPLAY_FLAG=couponRecords.WIC_DISPLAY_FLAG,
                    coupon.PROMO_COMP_TYPE=couponRecords.PROMO_COMP_TYPE,
                    coupon.APPLY_TO_CODE=couponRecords.APPLY_TO_CODE,
                    coupon.SELLING_UOM=couponRecords.SELLING_UOM,
                    coupon.NET_AMT_FLG=couponRecords.NET_AMT_FLG,
                    coupon.MIN_MFG_FLG=couponRecords.MIN_MFG_FLG,
                    coupon.MIN_DEPT_FLG=couponRecords.MIN_DEPT_FLG,
                    coupon.MFG_NUM=couponRecords.MFG_NUM,
                    coupon.VEN_CPN_FAMCODE_1=couponRecords.VEN_CPN_FAMCODE_1,
                    coupon.VEN_CPN_FAMCODE_2=couponRecords.VEN_CPN_FAMCODE_2,
                    coupon.TAX_PLAN1=couponRecords.TAX_PLAN1,
                    coupon.TAX_PLAN2=couponRecords.TAX_PLAN2,
                    coupon.TAX_PLAN3=couponRecords.TAX_PLAN3,
                    coupon.TAX_PLAN4=couponRecords.TAX_PLAN4,
                    coupon.TAX_PLAN5=couponRecords.TAX_PLAN5,
                    coupon.TAX_PLAN6=couponRecords.TAX_PLAN6,
                    coupon.TAX_PLAN7=couponRecords.TAX_PLAN7,
                    coupon.TAX_PLAN8=couponRecords.TAX_PLAN8,
                    coupon.BTGT_CPN_LINK=couponRecords.BTGT_CPN_LINK,
                    coupon.LOG_EXCEPTIONS=couponRecords.LOG_EXCEPTIONS,
                    coupon.MUST_BUY_IND=couponRecords.MUST_BUY_IND,
                    coupon.CHANGE_AMOUNT=couponRecords.CHANGE_AMOUNT,
                    coupon.CHANGE_PERCENT=couponRecords.CHANGE_PERCENT,
                    coupon.CHANGE_CURRENCY=couponRecords.CHANGE_CURRENCY,
                    coupon.CLUB_CARD=couponRecords.CLUB_CARD,
                    coupon.CHANGE_AMOUNT_PCT=couponRecords.CHANGE_AMOUNT_PCT,
                    coupon.MIN_QUANTITY=couponRecords.MIN_QUANTITY,
                    coupon.BUY_QUANTITY=couponRecords.BUY_QUANTITY,
                    coupon.GET_QUANTITY=couponRecords.GET_QUANTITY,
                    coupon.SALE_QUANTITY=couponRecords.SALE_QUANTITY,
                    coupon.LAST_UPDATE_ID=couponRecords.LAST_UPDATE_ID,
                    coupon.LAST_UPDATE_TIMESTAMP=couponRecords.LAST_UPDATE_TIMESTAMP
                WHEN NOT MATCHED THEN INSERT * '''.format(CouponDeltaPath))
    
  appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(CouponDeltaPath))
  loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
  appended_recs = appended_recs.head(1)
  ABC(DeltaTableFinalCount=appended_recs[0][0])

  spark.catalog.dropTempView(temp_table_name)
  loggerAtt.info("Merge into Delta table successful")


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write coupon output

# COMMAND ----------

def writeValidRecord(storeList, CouponDeltaPath, processing_file, Coupon_OutboundPath):
  ABC(ValidRecordSaveCheck = 1)
  couponOutputDf = spark.read.format('delta').load(CouponDeltaPath)
  if couponOutputDf is not None:
    if processing_file == 'COD':
      couponOutputDf = couponOutputDf.filter((col("LOCATION").isin(storeList))) 
    validCount = couponOutputDf.count()
    loggerAtt.info(f"Count of Valid Records in the File: {validCount}")
    ABC(ValidRecordCount = validCount)
    if validCount >0:
      couponOutputDf = couponOutputDf.withColumn("START_DATE",date_format(col("START_DATE"), 'yyyy/MM/dd').cast(StringType()))
      couponOutputDf = couponOutputDf.withColumn("END_DATE",date_format(col("END_DATE"), 'yyyy/MM/dd').cast(StringType()))
      couponOutputDf = couponOutputDf.withColumn("DEL_DATE",date_format(col("DEL_DATE"), 'yyyy/MM/dd').cast(StringType()))
      couponOutputDf = couponOutputDf.withColumn("CPN_APPLY_DATE",date_format(col("CPN_APPLY_DATE"), 'yyyy/MM/dd').cast(StringType()))
      couponOutputDf = couponOutputDf.withColumn('COUPON_NO', lpad(col('COUPON_NO'),14,'0'))
      couponOutputDf = couponOutputDf.withColumn('DEST_STR', lpad(col('DEST_STR'),4,'0'))
      couponOutputDf = couponOutputDf.withColumn('STORE', lpad(col('STORE'),4,'0'))
      couponOutputDf = couponOutputDf.withColumn('LOCATION', lpad(col('LOCATION'),4,'0'))
      couponOutputDf = couponOutputDf.withColumnRenamed('BANNER', 'BANNER_ID')
      
      couponOutputDf = couponOutputDf.withColumn('CHAIN_ID', lit('AHOLD'))
      
      couponOutputDf.write.partitionBy('CHAIN_ID', 'LOCATION').mode('overwrite').format('parquet').save(Coupon_OutboundPath + "/" + "Coupon_Output")
      loggerAtt.info('======== Valid Coupon Records write operation finished ========')
    else:
      loggerAtt.info('======== No Valid Coupon Records ========')
  else:
    loggerAtt.info('======== No Valid Coupon Records ========')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Main Function Processing

# COMMAND ----------

if __name__ == "__main__":
  loggerAtt.info('======== Input Coupon file processing initiated ========')
  ## Step 1: File Reading
  loggerAtt.info("File Read check")
  try:
    coupon_raw_df = readFile(file_location, infer_schema, first_row_is_header, delimiter, file_type)
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
  
  if coupon_raw_df is not None:
    loggerAtt.info("ABC Framework processing initiated")
    loggerAtt.info("Renaming columns")
    ## Step 2: Renaming
    try:
      ABC(RenamingCheck=1)
      coupon_raw_df = quinn.with_some_columns_renamed(couponflat_coupontable, change_col_name)(coupon_raw_df)
    except Exception as ex:
      ABC(RenamingCheck=0)
      err = ErrorReturn('Error', ex,'Renaming columns check Error')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
    loggerAtt.info("EOF count check")
    ## Step 4: EOF file count 
    try:
      ABC(BOFEOFCheck=1)
      
      eofDf = coupon_raw_df.filter(coupon_raw_df["DEST_STR"].rlike("^TRAILER"))

      loggerAtt.info("EOF file record count is " + str(eofDf.count()))
      EOFcnt=eofDf.count()
      ABC(EOFCount=EOFcnt)
      
      # If there are multiple header/Footer then the file is invalid
      if (eofDf.count() != 1):
        raise Exception('Error in EOF value')
      
      fileRecordCount = eofDf.withColumn("BATCH_SERIAL",trim(col("BATCH_SERIAL"))).select('BATCH_SERIAL')
      actualRecordCount = int(coupon_raw_df.count() + eofDf.count() - 2)
      fileRecordCount = fileRecordCount.groupBy("BATCH_SERIAL").mean().collect()[0]["BATCH_SERIAL"]
      fileRecordCount = re.sub('\W+','', fileRecordCount)
      fileRecordCount = int(fileRecordCount)

      actualRecordCount = int(coupon_raw_df.count() + eofDf.count() - 2)

      loggerAtt.info("Record Count mentioned in file: " + str(fileRecordCount))
      loggerAtt.info("Actual no of record in file: " + str(actualRecordCount))
      
      if fileRecordCount != actualRecordCount:
        raise Exception('Error in record count value')
      
    except Exception as ex:
      ABC(BOFEOFCheck=0)
      ABC(EOFCount=0)
      err = ErrorReturn('Error', ex,'EOF count check Error')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
    ## Step 3: Removing Null records
    loggerAtt.info("Removing Null records check")
    try:
      ABC(NullValueCheck=1)
      coupon_raw_df = coupon_raw_df.withColumn("BATCH_SERIAL",col('BATCH_SERIAL').cast(IntegerType()))
      
      coupon_nullRows = coupon_raw_df.where(reduce(lambda x, y: x | y, (col(x).isNull() for x in coupon_raw_df.columns)))

      loggerAtt.info("Dimension of the Null records:("+str(coupon_nullRows.count())+"," +str(len(coupon_nullRows.columns))+")")

      ABC(DropNACheck = 1)
      coupon_raw_df = coupon_raw_df.na.drop()

      loggerAtt.info("Dimension of the Not null records:("+str(coupon_raw_df.count())+"," +str(len(coupon_raw_df.columns))+")")
      
      ABC(NullValuCount = coupon_nullRows.count())
    except Exception as ex:
      ABC(NullValueCheck=0)
      ABC(DropNACheck=0)
      ABC(NullValuCount='')
      err = ErrorReturn('Error', ex,'Renaming columns check Error')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))    
    
    ## Step 5: Removing duplicate record 
    loggerAtt.info("Removing duplicate record check")
    try:
      ABC(DuplicateValueCheck = 1)
      if (coupon_raw_df.groupBy(coupon_raw_df.columns).count().filter("count > 1").count()) > 0:
        coupon_raw_df = coupon_raw_df.groupBy(coupon_raw_df.columns).count();
        coupon_duplicate_records = coupon_raw_df.filter("count > 1").drop('count')
        DuplicateValueCnt = coupon_duplicate_records.count()
        ABC(DuplicateValueCount=DuplicateValueCnt)
        loggerAtt.info("Duplicate record Exists. No of duplicate records are " + str(DuplicateValueCnt))
        coupon_raw_df= coupon_raw_df.filter("count == 1").drop('count')
      else:
        loggerAtt.info("No duplicate records")
        ABC(DuplicateValueCount=0)

    except Exception as ex:
      ABC(DuplicateValueCheck=0)
      ABC(DuplicateValueCount='')
      err = ErrorReturn('Error', ex,'Removing duplicate record Error')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))     
        
    loggerAtt.info("ABC Framework processing ended")

    ## Step 6: Filtering coupon records based on coupon number condition
    loggerAtt.info("Filtering coupon records based on coupon number condition check")
    try:
      invalidCoupon_df = coupon_raw_df.filter((col('COUPON_NO') < 41100000000 ) | (col('COUPON_NO') > 41100999999))
      coupon_raw_df = coupon_raw_df.filter((col('COUPON_NO') >= 41100000000 ) & (col('COUPON_NO') <= 41100999999))
    except Exception as ex:
      loggerAtt.error(ex)
      err = ErrorReturn('Error', ex,'couponFiltering')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))  
    
    ## Step 7: Applying Coupon transformation
    try:
      loggerAtt.info("Coupon transformation check")
      ABC(TransformationCheck = 1)
      coupon_raw_df,invalid_transformed_df = couponTransformation(coupon_raw_df,PipelineID)
      loggerAtt.info("Invalid Transformed records are " + str(invalid_transformed_df.count()))
      loggerAtt.info("Valid Transformed records are " + str(coupon_raw_df.count()))
    except Exception as ex:
      ABC(TransformationCheck = 0)
      loggerAtt.error(ex)
      err = ErrorReturn('Error', ex,'couponTransformation')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))  
    
    ## Step 8.1: Adding delete coupon records to Input coupon records
    if processing_file =='Delta':
      loggerAtt.info("Fetch Coupon Delete Rec check")
      ABC(DeleteCouponCheck = 1)
      try:
        couponDelta_df = fetchCouponDeleteRec(PipelineID,CouponDeltaPath)
#         couponDelta_df = None
#         pass
      except Exception as ex:
        if 'is not a Delta table' in str(ex):
          couponDelta_df = None
        else:
          ABC(DeleteCouponCheck = 0)
          ABC(DeleteCouponCount = '')
          err = ErrorReturn('Error', ex,'fetchCouponDeleteRec')
          errJson = jsonpickle.encode(err)
          errJson = json.loads(errJson)
          dbutils.notebook.exit(Merge(ABCChecks,errJson))

      ## Step 8.2: Merging delete coupon record and incoming coupon record
      loggerAtt.info("Combining Delete record with incoming record check")      
      try:
        ABC(couponValidUnionCheck = 1)
        if couponDelta_df is not None:
          if couponDelta_df.count() > 0:
            validRecordsList = [couponDelta_df, coupon_raw_df]
            coupon_raw_df = reduce(DataFrame.unionAll, validRecordsList)
      except Exception as ex:
        ABC(couponValidUnionCheck = 0)
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'combining delete records with input records')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
          
    ## Step 9: Combining Duplicate, null record and invalid coupon records
    loggerAtt.info("Combining invalid records check")      
    try:
      ABC(couponInvalidUnionCheck = 1)
      if coupon_duplicate_records is not None:
        invalidRecordsList = [invalidCoupon_df, coupon_duplicate_records, coupon_nullRows,invalid_transformed_df]
        coupon_invalidRecords = reduce(DataFrame.unionAll, invalidRecordsList)
      else:
        invalidRecordsList = [invalidCoupon_df, coupon_nullRows,invalid_transformed_df]
        coupon_invalidRecords = reduce(DataFrame.unionAll, invalidRecordsList)
    except Exception as ex:
        ABC(couponInvalidUnionCheck = 0)
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'combining invalid records error')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
    
    ## Step 10: Coupon Archival
    if processing_file =='Delta':
      loggerAtt.info("Coupon Archival check")
      try:
        CouponArchival(CouponDeltaPath,Date,Archivalfilelocation)
      except Exception as ex:
        ABC(CouponArchivalCheck = 0)
        ABC(archivalInitCount='')
        ABC(archivalAfterCount='')
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'CouponArchival')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
    
    ## Step 11: Merge Coupon records
    loggerAtt.info("Upsert Coupon Records check")
    try:
      ABC(DeltaTableCreateCheck = 1)
      upsertCouponRecords(coupon_raw_df, CouponDeltaPath)
    except Exception as ex:
      ABC(DeltaTableCreateCheck = 0)
      ABC(DeltaTableInitCount='')
      ABC(DeltaTableFinalCount='')
      loggerAtt.error(ex)
      err = ErrorReturn('Error', ex,'upsertCouponRecords')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
    
    ## Step 12: Write output file to ADLS location
    loggerAtt.info("Write Valid Record check")
    try:
      writeValidRecord(list(coupon_raw_df.select('LOCATION').toPandas()['LOCATION']), CouponDeltaPath, processing_file, Coupon_OutboundPath)
    except Exception as ex:
      ABC(ValidRecordSaveCheck = 0)
      ABC(ValidRecordCount = '')
      loggerAtt.error(ex)
      err = ErrorReturn('Error', ex,'writeValidRecord')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
    
    ## Step 13: Write Invalid records to ADLS location
    loggerAtt.info("Write Invalid Record check")
    try:
      writeInvalidRecord(coupon_invalidRecords, Invalid_RecordsPath, Date)
    except Exception as ex:
      ABC(InvalidRecordSaveCheck = 0)
      ABC(InvalidRecordCount = '')
      loggerAtt.error(ex)
      err = ErrorReturn('Error', ex,'writeInvalidRecord')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
    
  else:
    loggerAtt.info("Error in input file reading")
    
loggerAtt.info('======== Input Coupon file processing ended ========')


  

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

updateDeltaVersioning('Ahold', 'coupon', PipelineID, Filepath, FileName)

# COMMAND ----------

#updateDeltaVersioning('Ahold', 'Coupon', PipelineID, Filepath, FileName)

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