# Databricks notebook source
from pyspark.sql.types import * 
from pyspark.sql import *
import json
import quinn
from pyspark.sql.functions  import *
from pytz import timezone
import datetime
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

custom_logfile_Name ='taxholiday_customlog'
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
dbutils.widgets.text("logFilesPath","")
dbutils.widgets.text("invalidRecordsPath","")
dbutils.widgets.text("clientId","")
dbutils.widgets.text("keyvaultSecretName","")


FileName=dbutils.widgets.get("fileName")
Filepath=dbutils.widgets.get("filePath")
Directory=dbutils.widgets.get("directory")

outputDirectory=dbutils.widgets.get("outputDirectory")
PipelineID=dbutils.widgets.get("pipelineID")
container=dbutils.widgets.get("Container")
mount_point=dbutils.widgets.get("MountPoint")
taxHolidayDeltaPath=dbutils.widgets.get("deltaPath")
Archival_filePath=dbutils.widgets.get("archivalFilePath")
Log_FilesPath=dbutils.widgets.get("logFilesPath")
Invalid_RecordsPath=dbutils.widgets.get("invalidRecordsPath")
clientId=dbutils.widgets.get("clientId")
keyVaultName=dbutils.widgets.get("keyvaultSecretName")
PipelineID =dbutils.widgets.get("pipelineID")
Date = datetime.datetime.now(timezone("America/Halifax")).strftime("%Y-%m-%d")
inputSource= 'abfss://' + Directory + '@' + container + '.dfs.core.windows.net/'
outputSource= 'abfss://' + outputDirectory + '@' + container + '.dfs.core.windows.net/'
file_location = '/mnt' + '/' + Directory + '/' + Filepath +'/' + FileName 
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

# source= "abfss://ahold-centralized-price-promo@rs06ue2dmasadata02.dfs.core.windows.net/"
# Date = datetime.datetime.now(timezone("America/Indianapolis")).strftime("%Y-%m-%d")
# PipelineID = "temp"
# taxHolidayDeltaPath='/mnt/ahold-centralized-price-promo/Tax/Outbound/SDM/taxHoliday'

# custom_logfile_Name ='taxHolidaymaintainence_customlog'
# mount_point = "/mnt/ahold-centralized-price-promo"
# Log_FilesPath = "/mnt/ahold-centralized-price-promo/Tax/Outbound/SDM/Logfiles"
# source= "abfss://ahold-centralized-price-promo@rs06ue2dmasadata02.dfs.core.windows.net/"
# Invalid_RecordsPath = "/mnt/ahold-centralized-price-promo/Tax/Outbound/SDM/InvalidRecords"
# file_location ="/mnt/ahold-centralized-price-promo/TAX/Inbound/RDS/2021/06/11/unzipped/TEST.NQ.CI.SMA.DB.TAX.MAINT.ZIP/TEST.NQ.CI.SMA.DB.TAX.MAINT.txt"

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

loggerAtt.info('========Schema definition initiated ========')
taxHolidayRaw_schema = StructType([
                           StructField("DB_UPC",StringType(),False),
                           StructField("DB_FILLER1",StringType(),True),
                           StructField("DB_STORE",StringType(),False),
                           StructField("DB_FILLER2",StringType(),True),
                           StructField("DB_HOLIDAY_TYPE",StringType(),False),
                           StructField("DB_FILLER3",StringType(),True),
                           StructField("DB_HOLIDAY_BEGIN_DATE",StringType(),False),
                           StructField("DB_FILLER4",StringType(),True),
                           StructField("DB_HOLIDAY_END_DATE",StringType(),False),
                           StructField("DB_FILLER5",StringType(),True),
                           StructField("DB_HOLIDAY_TAX_1",StringType(),False),
                           StructField("DB_HOLIDAY_TAX_2",StringType(),False),
                           StructField("DB_HOLIDAY_TAX_3",StringType(),False),
                           StructField("DB_HOLIDAY_TAX_4",StringType(),False),
                           StructField("DB_HOLIDAY_TAX_5",StringType(),False),
                           StructField("DB_HOLIDAY_TAX_6",StringType(),False),
                           StructField("DB_HOLIDAY_TAX_7",StringType(),False),
                           StructField("DB_HOLIDAY_TAX_8",StringType(),False),
                           StructField("DB_WIC_ALT",StringType(),False),
                           StructField("DB_WIC_IND",StringType(),False),
                           StructField("DB_SNAP_IND",StringType(),False),
                           StructField("DB_HIP_IND",StringType(),False),
                           StructField("DB_GIFT_CARD_RES",StringType(),False),
                           StructField("DB_LINK_PLU",StringType(),False),
                           StructField("DB_FILLER6",StringType(),True) 
])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Delta table creation

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
except Exception as ex:
  ABC(DeltaTableCreateCheck = 0)
  loggerAtt.error(ex)
  err = ErrorReturn('Error', ex,'taxHolidayDeltaPath deltaCreator')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))   

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## taxHoliday Processing

# COMMAND ----------

def typeCheck(s):
  try: 
      int(s)
      return True
  except ValueError:
      return False

typeCheckUDF = udf(typeCheck)

#date_func =  udf (lambda x: datetime.datetime.strptime(str(x), '%Y%m%d').date(), DateType())
date_func =  udf (lambda x: datetime.datetime.strptime(str(x), '%Y%M%d').date(), DateType())


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## taxHoliday Transformation

# COMMAND ----------

  def taxHolidayTransformation(processed_df, JOB_ID):
    processed_df=processed_df.withColumn("DB_HOLIDAY_BEGIN_DATE",when(col("DB_HOLIDAY_BEGIN_DATE")=='0000-00-00', lit(None)).otherwise(date_format(date_func(col("DB_HOLIDAY_BEGIN_DATE")), 'yyyy-MM-dd')).cast(DateType()))
    processed_df=processed_df.withColumn("DB_HOLIDAY_END_DATE",when(col("DB_HOLIDAY_END_DATE")=='0000-00-00', lit(None)).otherwise(date_format(date_func(col("DB_HOLIDAY_END_DATE")), 'yyyy-MM-dd')).cast(DateType()))
    processed_df=processed_df.withColumn("DB_UPC",lpad(col('DB_UPC'),14,'0').cast(LongType()))
    processed_df=processed_df.withColumn("DB_STORE",lpad(col('DB_STORE'),4,'0'))
    processed_df=processed_df.withColumn("INSERT_TIMESTAMP",current_timestamp())
    processed_df=processed_df.withColumn("LAST_UPDATE_ID",lit(JOB_ID))
    processed_df=processed_df.withColumn("LAST_UPDATE_TIMESTAMP",current_timestamp())
    processed_df=processed_df.withColumn("INSERT_ID", lit(JOB_ID))
    processed_df=processed_df.withColumn("BANNER", lit('AHOLD'))    
    processed_df=processed_df.select('DB_UPC', 'DB_STORE', 'DB_HOLIDAY_TYPE', 'DB_HOLIDAY_BEGIN_DATE', 'DB_HOLIDAY_END_DATE', 'DB_HOLIDAY_TAX_1', 'DB_HOLIDAY_TAX_2', 'DB_HOLIDAY_TAX_3', 'DB_HOLIDAY_TAX_4', 'DB_HOLIDAY_TAX_5', 'DB_HOLIDAY_TAX_6', 'DB_HOLIDAY_TAX_7', 'DB_HOLIDAY_TAX_8', 'DB_WIC_ALT', 'DB_WIC_IND',' DB_SNAP_IND', 'DB_HIP_IND', 'DB_GIFT_CARD_RES', 'DB_LINK_PLU', 'INSERT_ID', 'INSERT_TIMESTAMP', 'LAST_UPDATE_ID', 'LAST_UPDATE_TIMESTAMP')

    return processed_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## taxHoliday file reading

# COMMAND ----------

def readFile(file_location, infer_schema, first_row_is_header, delimiter,file_type):
  raw_df = spark.read.format(file_type) \
    .option("mode","PERMISSIVE") \
    .option("header", first_row_is_header) \
    .option("dateFormat", "yyyyMMdd") \
    .option("sep", delimiter) \
    .schema(taxHolidayRaw_schema) \
    .load(file_location)
  return raw_df

# COMMAND ----------

# file_location = str(file_location)
# file_type = "csv"
# infer_schema = "false"
# first_row_is_header = "true"
# delimiter = "|"

# COMMAND ----------

# taxHoliday_raw_df = readFile(file_location, infer_schema, first_row_is_header, delimiter,file_type)
# date_func =  udf (lambda x: datetime.datetime.strptime(str(x), '%Y%M%d').date(), DateType())
# taxHoliday_raw_df = taxHoliday_raw_df.withColumn("DB_HOLIDAY_BEGIN_DATE",when(col("DB_HOLIDAY_BEGIN_DATE")=='0000-00-00', lit(None)).otherwise(date_format(date_func(col("DB_HOLIDAY_END_DATE")), 'yyyy-MM-dd')).cast(DateType()))
# taxHoliday_raw_df = taxHoliday_raw_df.select("DB_HOLIDAY_BEGIN_DATE")
# taxHoliday_raw_df.display()

# COMMAND ----------

def upserttaxHolidayRecords(taxHoliday_valid_Records):
  loggerAtt.info("Merge into Delta table initiated")
  temp_table_name = "taxHolidayRecords"

  taxHoliday_valid_Records.createOrReplaceTempView(temp_table_name)
  try:
#   add before count
    initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(taxHolidayDeltaPath))
    loggerAtt.info(f"Initial count of records in Delta Table: {initial_recs.head(1)}")
    initial_recs = initial_recs.head(1)
    ABC(DeltaTableInitCount=initial_recs[0][0])
    spark.sql('''MERGE INTO delta.`{}` as taxHoliday
    USING taxHolidayRecords 
    ON taxHoliday.DB_UPC = taxHolidayRecords.DB_UPC
	and taxHoliday.DB_STORE = taxHolidayRecords.DB_STORE
    WHEN MATCHED Then 
            Update Set taxHoliday.DB_UPC=taxHolidayRecords.DB_UPC,
                      taxHoliday.DB_STORE=taxHolidayRecords.DB_STORE,
                      taxHoliday.DB_HOLIDAY_TYPE=taxHolidayRecords.DB_HOLIDAY_TYPE,
                      taxHoliday.DB_HOLIDAY_BEGIN_DATE=taxHolidayRecords.DB_HOLIDAY_BEGIN_DATE,
                      taxHoliday.DB_HOLIDAY_END_DATE=taxHolidayRecords.DB_HOLIDAY_END_DATE,
                      taxHoliday.DB_HOLIDAY_TAX_1=taxHolidayRecords.DB_HOLIDAY_TAX_1,
                      taxHoliday.DB_HOLIDAY_TAX_2=taxHolidayRecords.DB_HOLIDAY_TAX_2,
                      taxHoliday.DB_HOLIDAY_TAX_3=taxHolidayRecords.DB_HOLIDAY_TAX_3,
                      taxHoliday.DB_HOLIDAY_TAX_4=taxHolidayRecords.DB_HOLIDAY_TAX_4,
                      taxHoliday.DB_HOLIDAY_TAX_5=taxHolidayRecords.DB_HOLIDAY_TAX_5,
                      taxHoliday.DB_HOLIDAY_TAX_6=taxHolidayRecords.DB_HOLIDAY_TAX_6,
                      taxHoliday.DB_HOLIDAY_TAX_7=taxHolidayRecords.DB_HOLIDAY_TAX_7,
                      taxHoliday.DB_HOLIDAY_TAX_8=taxHolidayRecords.DB_HOLIDAY_TAX_8,
                      taxHoliday.DB_WIC_ALT=taxHolidayRecords.DB_WIC_ALT,
                      taxHoliday.DB_WIC_IND=taxHolidayRecords.DB_WIC_IND,
                      taxHoliday.DB_SNAP_IND=taxHolidayRecords.DB_SNAP_IND,
                      taxHoliday.DB_HIP_IND=taxHolidayRecords.DB_HIP_IND,
                      taxHoliday.DB_GIFT_CARD_RES=taxHolidayRecords.DB_GIFT_CARD_RES,
					  taxHoliday.DB_LINK_PLU=taxHolidayRecords.DB_LINK_PLU,
                      taxHoliday.INSERT_ID=taxHolidayRecords.INSERT_ID,
                      taxHoliday.INSERT_TIMESTAMP=taxHolidayRecords.INSERT_TIMESTAMP,
                      taxHoliday.LAST_UPDATE_ID=taxHolidayRecords.LAST_UPDATE_ID,
                      taxHoliday.LAST_UPDATE_TIMESTAMP=taxHolidayRecords.LAST_UPDATE_TIMESTAMP

                  WHEN NOT MATCHED THEN INSERT * '''.format(taxHolidayDeltaPath))
#   add abc after count
    appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(taxHolidayDeltaPath))
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

def writeInvalidRecords(taxHoliday_invalidRecords, Invalid_RecordsPath):
  ABC(InvalidRecordSaveCheck = 1)
  try:
    if taxHoliday_invalidRecords.count() > 0:
      taxHoliday_invalidRecords.write.mode('Append').format('parquet').save(Invalid_RecordsPath + "/" +Date+ "/" + "taxHoliday_Invalid_Data")
      ABC(InvalidRecordCount = taxHoliday_invalidRecords.count())
      loggerAtt.info('======== Invalid taxHoliday Records write operation finished ========')

  except Exception as ex:
    ABC(InvalidRecordSaveCheck = 0)
    loggerAtt.error(str(ex))
    loggerAtt.info('======== Invalid record file write to ADLS location failed ========')
    err = ErrorReturn('Error', ex,'Writeoperationfailed for Invalid Record')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

def abcFramework(footerRecord, taxHoliday_duplicate_records, taxHoliday_raw_df):
  global fileRecordCount
  loggerAtt.info("ABC Framework function initiated")
  try:
    
    # Fetching EOF and BOF records and checking whether it is of length one each
    footerRecord = taxHoliday_raw_df.filter(taxHoliday_raw_df["DB_UPC"].rlike("TRAILER"))

    ABC(EOFCheck=1)
    EOFcnt = footerRecord.count()

    ABC(EOFCount=EOFcnt)

    loggerAtt.info("EOF file record count is " + str(footerRecord.count()))
    
    # If there are multiple Footer then the file is invalid
    if (footerRecord.count() != 1):
      raise Exception('Error in EOF value')
    
    taxHolidayDf = taxHoliday_raw_df.filter(typeCheckUDF(col('DB_UPC')) == True)
#     taxHolidayCnt =taxHolidayDf.count()
#     ABC(taxHolidayCount = )

    
#	matching instead of BATCH_SERIAL
    footerRecord = footerRecord.withColumn('DB_UPC',lpad(col('DB_UPC'),14,'0'))
    fileRecordCount = int(footerRecord.select('DB_STORE').toPandas()['DB_STORE'][0])

    actualRecordCount = int(taxHolidayDf.count() + footerRecord.count() - 1)
    
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
    taxHoliday_nullRows = taxHolidayDf.where(reduce(lambda x, y: x | y, (col(x).isNull() for x in taxHolidayDf.columns)))
    ABC(NullValueCheck=1)
    loggerAtt.info("Dimension of the Null records:("+str(taxHoliday_nullRows.count())+"," +str(len(taxHoliday_nullRows.columns))+")")

    taxHoliday_raw_dfWithNoNull = taxHolidayDf.na.drop()
    ABC(DropNACheck = 1)
    NullValuCnt = taxHoliday_nullRows.count()
    ABC(NullValuCount = NullValuCnt)
    loggerAtt.info("Dimension of the Not null records:("+str(taxHoliday_raw_dfWithNoNull.count())+"," +str(len(taxHoliday_raw_dfWithNoNull.columns))+")")

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
    if (taxHoliday_raw_dfWithNoNull.groupBy(taxHoliday_raw_dfWithNoNull.columns).count().filter("count > 1").count()) > 0:
      loggerAtt.info("Duplicate records exists")
      taxHoliday_rawdf_withCount =taxHoliday_raw_dfWithNoNull.groupBy(taxHoliday_raw_dfWithNoNull.columns).count();
      taxHoliday_duplicate_records = taxHoliday_rawdf_withCount.filter("count > 1").drop('count')
      taxHolidayRecords= taxHoliday_rawdf_withCount.drop('count')
      loggerAtt.info("Duplicate record Exists. No of duplicate records are " + str(taxHoliday_duplicate_records.count()))
      ABC(DuplicateValueCount=DuplicateValueCnt)
    else:
      loggerAtt.info("No duplicate records")
      taxHolidayRecords = taxHoliday_raw_dfWithNoNull
      ABC(DuplicateValueCount=0)
    
    loggerAtt.info("ABC Framework function ended")
    return footerRecord, taxHoliday_nullRows, taxHoliday_duplicate_records, taxHolidayRecords
  except Exception as ex:
    ABC(DuplicateValueCheck = 0)
    ABC(DuplicateValueCount='')
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'taxHoliday_ProblemRecs')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))


# COMMAND ----------

if __name__ == "__main__":
  
  ## File reading parameters
  loggerAtt.info('======== Input taxHoliday file processing initiated ========')
  file_location = str(file_location)
  file_type = "csv"
  infer_schema = "false"
  first_row_is_header = "false"
  delimiter = "|"
  taxHoliday_raw_df = None
  taxHoliday_duplicate_records = None
  footerRecord = None
  taxHolidayRecords = None
  taxHoliday_valid_Records = None
  PipelineID= str(PipelineID)
  folderDate = Date

  ## Step1: Reading input file
  try:
    taxHoliday_raw_df = readFile(file_location, infer_schema, first_row_is_header, delimiter, file_type)
    ABC(ReadDataCheck=1)
    RawDataCount = taxHoliday_raw_df.count()
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

  
  if taxHoliday_raw_df is not None:

    ## Step2: Performing ABC framework check
 
    footerRecord, taxHoliday_nullRows, taxHoliday_duplicate_records, taxHolidayRecords = abcFramework(footerRecord, taxHoliday_duplicate_records, taxHoliday_raw_df)

    ## Step3: Performing Transformation
    try:  
      if taxHolidayRecords is not None: 
        taxHoliday_valid_Records = taxHolidayTransformation(taxHolidayRecords, PipelineID)
        
        ABC(TransformationCheck=1)
    except Exception as ex:
      ABC(TransformationCheck = 0)
      loggerAtt.error(ex)
      err = ErrorReturn('Error', ex,'taxHolidayTransformation')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
    
    ## Step4: Combining all duplicate records
    try:
      ## Combining Duplicate, null record
      if taxHoliday_duplicate_records is not None:
        invalidRecordsList = [footerRecord, taxHoliday_duplicate_records, taxHoliday_nullRows]
        taxHoliday_invalidRecords = reduce(DataFrame.unionAll, invalidRecordsList)
        ABC(InvalidRecordSaveCheck = 1)
        ABC(InvalidRecordCount = taxHoliday_invalidRecords.count())
      else:
        invalidRecordsList = [footerRecord, taxHoliday_nullRows]
        taxHoliday_invalidRecords = reduce(DataFrame.unionAll, invalidRecordsList)
     
    except Exception as ex:
      ABC(InvalidRecordSaveCheck = 0)
      ABC(InvalidRecordCount='')
      err = ErrorReturn('Error', ex,'Grouping Invalid record function')
      err = ErrorReturn('Error', ex,'InvalidRecordsSave')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
    loggerAtt.info("No of valid records: " + str(taxHoliday_valid_Records.count()))
    loggerAtt.info("No of invalid records: " + str(taxHoliday_invalidRecords.count()))
    
    ## Step5: Merging records into delta table
    if taxHoliday_valid_Records is not None:
        taxHoliday_valid_Records = upserttaxHolidayRecords(taxHoliday_valid_Records)
    ## Step6: Writing Invalid records to ADLS location
    if taxHoliday_invalidRecords is not None:
      try:
        writeInvalidRecords(taxHoliday_invalidRecords, Invalid_RecordsPath)
      except Exception as ex:
        ABC(InvalidRecordSaveCheck = 0)
        ABC(InvalidRecordCount = '')
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'writeInvalidRecord')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
#     
  else:
    loggerAtt.info("Error in input file reading")
    
     
    
loggerAtt.info('======== Input taxHoliday file processing ended ========')

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