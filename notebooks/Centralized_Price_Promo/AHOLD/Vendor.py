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

custom_logfile_Name ='vendor_dailymaintainence_customlog'
loggerAtt, p_logfile, file_date = logger(custom_logfile_Name, '/tmp/')

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
dbutils.widgets.text("storeDeltaPath","")

dbutils.widgets.text("archivalFilePath","")
dbutils.widgets.text("outputDirectory","")

outputDirectory=dbutils.widgets.get("outputDirectory")

Archival_filePath=dbutils.widgets.get("archivalFilePath")
StoreDeltaPath = dbutils.widgets.get("storeDeltaPath")
FileName=dbutils.widgets.get("fileName")
Filepath=dbutils.widgets.get("filePath")
Directory=dbutils.widgets.get("directory")
container=dbutils.widgets.get("Container")
PipelineID=dbutils.widgets.get("pipelineID")
mount_point=dbutils.widgets.get("MountPoint")
VendorDeltaPath=dbutils.widgets.get("deltaPath")
Log_FilesPath=dbutils.widgets.get("logFilesPath")
Invalid_RecordsPath=dbutils.widgets.get("invalidRecordsPath")
Vendor_OutboundPath= '/mnt' + '/' + outputDirectory + '/Vendor/Outbound/CDM'
outputSource= 'abfss://' + outputDirectory + '@' + container + '.dfs.core.windows.net/'
Date = datetime.datetime.now(timezone("America/Halifax")).strftime("%Y-%m-%d")
file_location = '/mnt' + '/' + Directory + '/' + Filepath +'/' + FileName 
inputSource= 'abfss://' + Directory + '@' + container + '.dfs.core.windows.net/'
source= 'abfss://' + Directory + '@' + container + '.dfs.core.windows.net/'
clientId=dbutils.widgets.get("clientId")
keyVaultName=dbutils.widgets.get("keyVaultName")
PipelineID =dbutils.widgets.get("pipelineID")
# source= "abfss://ahold-centralized-price-promo@rs06ue2dmasadata02.dfs.core.windows.net/"
# PipelineID = "temp"
# VendorDeltaPath='/mnt/ahold-centralized-price-promo/Vendor/Outbound/SDM/Vendor_delta'
# custom_logfile_Name ='vendor_dailymaintainence_customlog'
# Archival_filePath= '/mnt/ahold-centralized-price-promo/Vendor/Outbound/SDM/ArchivalRecords'
# mount_point = "/mnt/ahold-centralized-price-promo"
# Vendor_OutboundPath = "mnt/ahold-centralized-price-promo/Vendor/Outbound/CDM"
# Log_FilesPath = "/mnt/ahold-centralized-price-promo/Vendor/Outbound/SDM/Logfiles"
# source= "abfss://ahold-centralized-price-promo@rs06ue2dmasadata02.dfs.core.windows.net/"
# Invalid_RecordsPath = "/mnt/ahold-centralized-price-promo/Vendor/Outbound/SDM/InvalidRecords"
# file_location ="Fullvendor/Inbound/RDS/2021/04/27/Test/TEST.NQ.GM.SMA.DB.VENDOR.LOAD.ZIP/TEST.NQ.GM.SMA.DB.VENDOR.LOAD.txt"

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
  err = ErrorReturn('Error', ex,'Mounting input')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

#Defining the schema for Vendor Table
loggerAtt.info('========Schema definition initiated ========')
vendorRaw_schema = StructType([
                          StructField("DEST_STORE",StringType(),False),
                           StructField("STORE_NUM",IntegerType(),False),
                           StructField("BATCH_SERIAL",StringType(),False),
                           StructField("DAYOFWEEK",IntegerType(),False),
                           StructField("SUB_DEPT",IntegerType(),False),
                           StructField("CHNG_TYP",IntegerType(),False),
                           StructField("VEND_NUM",StringType(),False),
                           StructField("VEND_NAME",StringType(),False),
                           StructField("VEND_HANG_TAGS",StringType(), False),
                           StructField("LAST_UPDATE_DATE",StringType(),True),
                           StructField("FILLER",StringType(),True)     
])

# COMMAND ----------

try:
  ABC(DeltaTableCreateCheck=1)
  spark.sql("""
  CREATE TABLE IF NOT EXISTS Vendor_delta (
  DEST_STORE INTEGER,
  STORE_NUM String,
  BATCH_SERIAL INTEGER,
  DAYOFWEEK INTEGER,
  SUB_DEPT INTEGER,
  CHNG_TYP INTEGER,
  VENDOR_NUMBER INTEGER,
  VENDOR_NAME STRING,
  VEND_HANG_TAGS STRING,
  LAST_UPDATE_DATE DATE,
  INSERT_ID STRING,
  INSERT_TIMESTAMP TIMESTAMP,
  LAST_UPDATE_ID STRING,
  LAST_UPDATE_TIMESTAMP TIMESTAMP,
  BANNER STRING
  )
  USING delta
  Location '{}'
  PARTITIONED BY (VENDOR_NUMBER)
  """.format(VendorDeltaPath))
except Exception as ex:
  ABC(DeltaTableCreateCheck = 0)
  loggerAtt.error(ex)
  err = ErrorReturn('Error', ex,'storeDeltaPath deltaCreator')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))  

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
def vendorflat_vendortable(s):
    return Vendor_Renaming[s]
def change_col_name(s):
    return s in Vendor_Renaming

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Vendor Transformation

# COMMAND ----------

def vendorTransformation(processed_df, JOB_ID):
  processed_df=processed_df.withColumn("VENDOR_NAME",when(col("VENDOR_NAME")==lit(None),"NO VENDOR NAME AVAILABLE").otherwise(col("VENDOR_NAME")))
  processed_df=processed_df.withColumn('DEST_STORE', lpad(col('DEST_STORE'),4,'0'))
  processed_df=processed_df.withColumn('STORE_NUM', lpad(col('STORE_NUM'),4,'0'))
  processed_df=processed_df.withColumn("VEND_HANG_TAGS",when(col("VEND_HANG_TAGS")==lit(None), 'Y').otherwise('N'))
  processed_df=processed_df.withColumn("INSERT_TIMESTAMP",current_timestamp())
  processed_df=processed_df.withColumn("LAST_UPDATE_ID",lit('JOB_ID'))
  processed_df=processed_df.withColumn("LAST_UPDATE_TIMESTAMP",current_timestamp())   
  processed_df=processed_df.withColumn("INSERT_ID", lit('JOB_ID'))   
  processed_df=processed_df.withColumn("BANNER", lit('AHOLD'))
  
  return processed_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Vendor Renaming dictionary

# COMMAND ----------

Vendor_Renaming = {"VEND_NUM":"VENDOR_NUMBER", "VEND_NAME":"VENDOR_NAME"}

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Vendor file reading

# COMMAND ----------

def readFile(file_location, infer_schema, first_row_is_header, delimiter,file_type):
  raw_df = spark.read.format(file_type) \
    .option("mode","PERMISSIVE") \
    .option("header", first_row_is_header) \
    .option("dateFormat", "yyyyMMdd") \
    .option("sep", delimiter) \
    .schema(vendorRaw_schema) \
    .load(file_location)
  return raw_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##Merge into Delta table 

# COMMAND ----------

def upsertVendorRecords(vendor_valid_Records):
  loggerAtt.info("Merge into Delta table initiated")
  temp_table_name = "vendorRecords"

  vendor_valid_Records.createOrReplaceTempView(temp_table_name)
  try:
    initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(VendorDeltaPath))
    loggerAtt.info(f"Initial count of records in Delta Table: {initial_recs.head(1)}")
    initial_recs = initial_recs.head(1)
    ABC(DeltaTableInitCount=initial_recs[0][0])
    spark.sql('''MERGE INTO delta.`{}` as vendor
    USING vendorRecords 
    ON vendor.VENDOR_NUMBER = vendorRecords.VENDOR_NUMBER and vendor.STORE_NUM = vendorRecords.STORE_NUM
    WHEN MATCHED Then 
            Update Set VENDOR.DEST_STORE=vendorRecords.DEST_STORE,
                      vendor.STORE_NUM=vendorRecords.STORE_NUM,
                      vendor.BATCH_SERIAL=vendorRecords.BATCH_SERIAL,
                      vendor.DAYOFWEEK=vendorRecords.DAYOFWEEK,
                      vendor.SUB_DEPT=vendorRecords.SUB_DEPT,
                      vendor.CHNG_TYP=vendorRecords.CHNG_TYP,
                      vendor.VENDOR_NUMBER=vendorRecords.VENDOR_NUMBER,
                      vendor.VENDOR_NAME=vendorRECORDS.VENDOR_NAME,
                      vendor.VEND_HANG_TAGS=vendorRECORDS.VEND_HANG_TAGS,
                      vendor.LAST_UPDATE_DATE=vendorRecords.LAST_UPDATE_DATE,
                      vendor.INSERT_ID=vendorRecords.INSERT_ID,
                      vendor.INSERT_TIMESTAMP=vendorRecords.INSERT_TIMESTAMP,
                      vendor.LAST_UPDATE_ID=vendorRecords.LAST_UPDATE_ID,
                      vendor.LAST_UPDATE_TIMESTAMP=vendorRecords.LAST_UPDATE_TIMESTAMP,
                      vendor.BANNER=vendorRecords.BANNER


                  WHEN NOT MATCHED THEN INSERT * '''.format(VendorDeltaPath))
    loggerAtt.info("Merge into Delta table successful")
    appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(VendorDeltaPath))
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

# MAGIC  %md 
# MAGIC  
# MAGIC  ##write processed vendor files to ADLS 

# COMMAND ----------

def writeInvalidRecords(vendor_invalidRecords, Invalid_RecordsPath):
  ABC(InvalidRecordSaveCheck = 1)
  try:
    if vendor_invalidRecords.count() > 0:
      vendor_invalidRecords.write.mode('Append').format('parquet').save(Invalid_RecordsPath + "/" +Date+ "/" + "Vendor_Invalid_Data")
      ABC(InvalidRecordCount = vendor_invalidRecords.count())
      loggerAtt.info('======== Invalid vendor Records write operation finished ========')
      
  except Exception as ex:
    ABC(InvalidRecordSaveCheck = 0)
    loggerAtt.error(str(ex))
    loggerAtt.info('======== Invalid record file write to ADLS location failed ========')
    err = ErrorReturn('Error', ex,'Writeoperationfailed for Invalid Record')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

def vendorWrite(VendorDeltaPath,vendorOutboundPath):
  vendorOutputDf = spark.read.format('delta').load(VendorDeltaPath)

  if vendorOutputDf.count() >0:
    vendorOutputDf=vendorOutputDf.withColumn('DEST_STORE', lpad(col('DEST_STORE'),4,'0'))
    vendorOutputDf=vendorOutputDf.withColumn('STORE_NUM', lpad(col('STORE_NUM'),4,'0'))
  
    ABC(ValidRecordCount=vendorOutputDf.count())
    vendorOutputDf = vendorOutputDf.withColumn('CHAIN_ID', lit('AHOLD'))
    vendorOutputDf.write.partitionBy('CHAIN_ID', 'STORE_NUM').mode('overwrite').format('parquet').save(Vendor_OutboundPath + "/" +"Vendor_Output")
    loggerAtt.info('========Vendor Records Output successful ========')
  else:
    loggerAtt.info('======== No Vendor Records Output Done ========')
    ABC(ValidRecordCount=0)

# COMMAND ----------

def VendorArchival(VendorDeltaPath,Date,Archivalfilelocation):

  StoreDelta = spark.sql('''select DEST_STORE as STORE, STORE_NUMBER from delta.`{}`'''.format(StoreDeltaPath))
  vendorarchival_df = spark.read.format('delta').load(VendorDeltaPath)
  
  vendorarchival_df = vendorarchival_df.join(StoreDelta, [StoreDelta.STORE == vendorarchival_df.DEST_STORE], how='left').select([col(xx) for xx in vendorarchival_df.columns] + ["STORE_NUMBER"])

  vendorarchival_df = vendorarchival_df.filter("STORE_NUMBER is NULL")
  if vendorarchival_df.count() >0:

    vendorarchival_df.write.mode('Append').format('parquet').save(Archivalfilelocation + "/" +Date+ "/" +"Store_Archival_Records")
#     deltaTable = DeltaTable.forPath(spark, VendorDeltaPath)
#     deltaTable.delete((datediff(to_date(current_date()),col("STORE_CLOSE_DATE")) >=90))
#     add count
    loggerAtt.info('========vendor Records Archival successful ========')
  else:
    loggerAtt.info('======== No vendor Records Archival Done ========')
  return vendorarchival_df

# COMMAND ----------

 
#   vendorarchival_df = spark.read.format('delta').load('/mnt/ahold-centralized-price-promo/Vendor/Outbound/SDM/Vendor_delta')
  
#   vendorarchival_df = vendorarchival_df.join(StoreDelta, [StoreDelta.STORE == vendorarchival_df.DEST_STORE], how='left').select([col(xx) for xx in vendorarchival_df.columns] + ["STORE_NUMBER"])
  
#   vendorarchival_df = vendorarchival_df.filter("STORE_NUMBER is NULL")
#   display(vendorarchival_df)

# COMMAND ----------

def abcFramework(footerRecord, vendor_duplicate_records, vendor_raw_df):
  global fileRecordCount
  loggerAtt.info("ABC Framework function initiated")
  try:


    # Fetching EOF records and checking whether it is of length one each
    footerRecord = vendor_raw_df.filter(vendor_raw_df["DEST_STORE"].rlike("TRAILER"))

    ABC(EOFCheck=1)
    EOFcnt = footerRecord.count()

    ABC(EOFCount=EOFcnt)

    loggerAtt.info("EOF file record count is " + str(footerRecord.count()))
    
    # If there are multiple Footer then the file is invalid
    if (footerRecord.count() != 1):
      raise Exception('Error in EOF value')
    
    vendorDf = vendor_raw_df.filter(typeCheckUDF(col('DEST_STORE')) == True)
#     VendorCnt =vendorDf.count()
#     ABC(VendorCount = VendorCnt)


    footerRecord = footerRecord.withColumn('BATCH_SERIAL', substring('BATCH_SERIAL', 1, 8))
    fileRecordCount = int(footerRecord.select('BATCH_SERIAL').toPandas()['BATCH_SERIAL'][0])

    actualRecordCount = int(vendorDf.count() + footerRecord.count() - 1)
    
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
    vendor_nullRows = vendorDf.where(reduce(lambda x, y: x | y, (col(x).isNull() for x in vendorDf.columns)))
    ABC(NullValueCheck=1)
    loggerAtt.info("Dimension of the Null records:("+str(vendor_nullRows.count())+"," +str(len(vendor_nullRows.columns))+")")

    vendor_raw_dfWithNoNull = vendorDf.na.drop()
    ABC(DropNACheck = 1)
    NullValuCnt = vendor_nullRows.count()
    ABC(NullValuCount = NullValuCnt)
    loggerAtt.info("Dimension of the Not null records:("+str(vendor_raw_dfWithNoNull.count())+"," +str(len(vendor_raw_dfWithNoNull.columns))+")")

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
    if (vendor_raw_dfWithNoNull.groupBy(vendor_raw_dfWithNoNull.columns).count().filter("count > 1").count()) > 0:
      loggerAtt.info("Duplicate records exists")
      vendor_rawdf_withCount =vendor_raw_dfWithNoNull.groupBy(vendor_raw_dfWithNoNull.columns).count();
      vendor_duplicate_records = vendor_rawdf_withCount.filter("count > 1").drop('count')
      vendorRecords= vendor_rawdf_withCount.drop('count')
      loggerAtt.info("Duplicate record Exists. No of duplicate records are " + str(vendor_duplicate_records.count()))
      ABC(DuplicateValueCount=DuplicateValueCnt)
    else:
      loggerAtt.info("No duplicate records")
      vendorRecords = vendor_raw_dfWithNoNull
      ABC(DuplicateValueCount=0)
    
    loggerAtt.info("ABC Framework function ended")
    return footerRecord,vendor_nullRows, vendor_duplicate_records, vendorRecords
  except Exception as ex:
    ABC(DuplicateValueCheck = 0)
    ABC(DuplicateValueCount='')
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'vendor_ProblemRecs')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))


# COMMAND ----------

if __name__ == "__main__":
  
  ## File reading parameters
  loggerAtt.info('======== Input vendor file processing initiated ========')
  file_location = str(file_location)
  file_type = "csv"
  infer_schema = "false"
  first_row_is_header = "false"
  delimiter = "|"
  vendor_raw_df = None
  vendor_duplicate_records = None
  footerRecord = None
  vendorRecords = None
  vendor_valid_Records = None
  PipelineID= str(PipelineID)
  folderDate = Date

  ## Step1: Reading input file
  try:
    vendor_raw_df = readFile(file_location, infer_schema, first_row_is_header, delimiter, file_type)
    ABC(ReadDataCheck=1)
    RawDataCount = vendor_raw_df.count()
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

  
  if vendor_raw_df is not None:

    ## Step2: Performing ABC framework check
 
    footerRecord, vendor_nullRows, vendor_duplicate_records, vendorRecords = abcFramework(footerRecord, vendor_duplicate_records, vendor_raw_df)

     ## renaming the vendor column names
    vendorRecords = quinn.with_some_columns_renamed(vendorflat_vendortable, change_col_name)(vendorRecords) ##
    
    ## Step3: Performing Transformation
    try:  
      if vendorRecords is not None: 

        vendor_valid_Records = vendorTransformation(vendorRecords, PipelineID)

        ABC(TransformationCheck=1)
    except Exception as ex:
      ABC(TransformationCheck = 0)
      loggerAtt.error(ex)
      err = ErrorReturn('Error', ex,'vendorTransformation')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
    
    ## Step4: Combining all duplicate records
    try:
      ## Combining Duplicate, null record
      if vendor_duplicate_records is not None:
        invalidRecordsList = [footerRecord, vendor_duplicate_records, vendor_nullRows]
        vendor_invalidRecords = reduce(DataFrame.unionAll, invalidRecordsList)
        ABC(InvalidRecordSaveCheck = 1)
        
      else:
        invalidRecordsList = [footerRecord, vendor_nullRows]
        vendor_invalidRecords = reduce(DataFrame.unionAll, invalidRecordsList)
    except Exception as ex:
      ABC(InvalidRecordSaveCheck = 0)
      ABC(InvalidRecordCount='')
      err = ErrorReturn('Error', ex,'Grouping Invalid record function')
      err = ErrorReturn('Error', ex,'InvalidRecordsSave')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
    vendor_valid_Records=vendor_valid_Records.withColumn("LAST_UPDATE_DATE",col('LAST_UPDATE_DATE').cast(StringType()))
    vendor_invalidRecords=vendor_invalidRecords.withColumn("LAST_UPDATE_DATE",col('LAST_UPDATE_DATE').cast(StringType()))
    vendor_valid_Records=vendor_valid_Records.withColumn("INSERT_TIMESTAMP",col('INSERT_TIMESTAMP').cast(StringType()))
    vendor_valid_Records=vendor_valid_Records.withColumn("LAST_UPDATE_TIMESTAMP",col('LAST_UPDATE_TIMESTAMP').cast(StringType()))
    vendor_valid_Records=vendor_valid_Records.withColumn("DEST_STORE",col('DEST_STORE').cast(IntegerType()))
    loggerAtt.info("No of valid records: " + str(vendor_valid_Records.count()))
    loggerAtt.info("No of invalid records: " + str(vendor_invalidRecords.count()))
    ABC(InvalidRecordCount = vendor_valid_Records.count())
    ## Step5: Merging records into delta table
    if vendor_valid_Records is not None:
        upsertVendorRecords(vendor_valid_Records)
    
    ## Step6: Writing Invalid records to ADLS location
    if vendor_invalidRecords is not None:
      try:
        writeInvalidRecords(vendor_invalidRecords, Invalid_RecordsPath)
      except Exception as ex:
        ABC(InvalidRecordSaveCheck = 0)
        ABC(InvalidRecordCount = '')
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'writeInvalidRecord')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))

      
    try:
      loggerAtt.info("vendor records Archival records initated")
      ABC(archivalCheck = 1)
      VendorArchival(VendorDeltaPath,Date,Archival_filePath)
    except Exception as ex:
      ABC(archivalCheck = 0)
      ABC(archivalInitCount='')
      ABC(archivalAfterCount='')
      err = ErrorReturn('Error', ex,'vendorArchival')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
      # Step 8: Write output file
    try:
      ABC(vendorWriteCheck=1)
      vendorWrite(VendorDeltaPath,Vendor_OutboundPath)
    except Exception as ex:
      ABC(ValidRecordCount='')
      ABC(vendorWriteCheck=0)
      err = ErrorReturn('Error', ex,'vendorWrite')
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

updateDeltaVersioning('Ahold', 'vendor', PipelineID, Filepath, FileName)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Archiving Vendor records

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