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
# from pyspark.sql.functions import substring, length, col, expr

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

custom_logfile_Name ='weeklyad_dailymaintainence_customlog'
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
dbutils.widgets.text("keyVaultName","")


FileName=dbutils.widgets.get("fileName")
Filepath=dbutils.widgets.get("filePath")
Directory=dbutils.widgets.get("directory")

outputDirectory=dbutils.widgets.get("outputDirectory")
PipelineID=dbutils.widgets.get("pipelineID")
container=dbutils.widgets.get("Container")
mount_point=dbutils.widgets.get("MountPoint")
WeeklyAdDeltaPath=dbutils.widgets.get("deltaPath")
Archival_filePath=dbutils.widgets.get("archivalFilePath")

Log_FilesPath=dbutils.widgets.get("logFilesPath")
Invalid_RecordsPath=dbutils.widgets.get("invalidRecordsPath")
clientId=dbutils.widgets.get("clientId")
keyVaultName=dbutils.widgets.get("keyVaultName")
PipelineID =dbutils.widgets.get("pipelineID")
Date = datetime.datetime.now(timezone("America/Halifax")).strftime("%Y-%m-%d")
inputSource= 'abfss://' + Directory + '@' + container + '.dfs.core.windows.net/'
outputSource= 'abfss://' + outputDirectory + '@' + container + '.dfs.core.windows.net/'
file_location = '/mnt' + '/' + Directory + '/' + Filepath +'/' + FileName 
WeeklyAd_OutboundPath= '/mnt' + '/' + outputDirectory + '/WeeklyAd/Outbound/CDM'


# source= "abfss://ahold-centralized-price-promo@rs06ue2dmasadata02.dfs.core.windows.net/"

# PipelineID = "temp"
# weeklyAdDeltaPath='/mnt/ahold-centralized-price-promo/WeeklyAdfeed/Outbound/SDM/WeeklyAd_delta'

# custom_logfile_Name ='weeklyad_dailymaintainence_customlog'
# Archival_filePath= '/mnt/ahold-centralized-price-promo/WeeklyAdfeed/Outbound/SDM/ArchivalRecords'
# mount_point = "/mnt/ahold-centralized-price-promo"
# WeeklyAd_OutboundPath = "mnt/ahold-centralized-price-promo/WeeklyAdfeed/Outbound/CDM"
# Log_FilesPath = "/mnt/ahold-centralized-price-promo/WeeklyAdfeed/Outbound/SDM/Logfiles"
# source= "abfss://ahold-centralized-price-promo@rs06ue2dmasadata02.dfs.core.windows.net/"
# Invalid_RecordsPath = "/mnt/ahold-centralized-price-promo/WeeklyAdfeed/Outbound/SDM/InvalidRecords"
# file_location = "WeeklyAdfeed/Inbound/RDS/2022/01/10/DT_WEEKLY_AD_FEED.TEXT"

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

#Defining the schema for WeeklyAd Table
loggerAtt.info('========Schema definition initiated ========')
weeklyAdRaw_schema = StructType([
                          StructField("EVENT_NAME",StringType(),False),
                          StructField("AD_WEEK",IntegerType(),True),
                          StructField("BRAND_DIVISION",StringType(),True),
                          StructField("MARKET_SEGMENT",StringType(),True),
                          StructField("ATTR_PAGE",IntegerType(),True),
                          StructField("ATTR_BLOCK",IntegerType(),True),
                          StructField("ATTR_AD_PLACEMENT",StringType(),True),
                          StructField("ATTR_AD_FORMAT",StringType(),True),
                          StructField("ATTR_HEADLINE",StringType(), True),
                          StructField("ATTR_SUBHEADLINE",StringType(),True),
                          StructField("ATTR_AUSA_ITEM_NO",StringType(),False),
                          StructField("ATTR_PRICE_TYPE_DESC",StringType(),True),
                          StructField("AD_COMMENTS",StringType(),True),
                          StructField("SID_COPYCARD",IntegerType(),True),
                          StructField("ADDITIONAL_SID",StringType(),True),
                          StructField("SOURCE_PROMOTIONID",IntegerType(),True),
                          StructField("CUSTOMERLOCATIONKEY",IntegerType(),False)
])

# COMMAND ----------

try:
  ABC(DeltaTableCreateCheck=1)
  spark.sql("""
  CREATE TABLE IF NOT EXISTS WeeklyAd_delta (
    EVENT_NAME STRING,
    AD_WEEK INTEGER,
    DIVISION STRING,
    MARKET STRING,
    ATTR_PAGE INTEGER,
    ATTR_BLOCK INTEGER,
    ATTR_AD_PLACEMENT STRING,
    ATTR_AD_FORMAT STRING,
    ATTR_HEADLINE STRING,
    ATTR_SUBHEADLINE STRING,
    ITEM_NUMBER STRING,
    ATTR_PRICE_TYPE_DESC STRING,
    AD_COMMENTS STRING,
    SID_COPYCARD INTEGER,
    ADDITIONAL_SID STRING,
    SOURCE_PROMOTIONID INTEGER,
    STORE_NUMBER STRING,
    AD_PROMO_START_DATE DATE,
    AD_PROMO_END_DATE DATE,
    DEMANDTEC_PRISM_SID INTEGER,
    INSERT_ID STRING,
    INSERT_TIMESTAMP TIMESTAMP
  )
  USING delta
  Location '{}'
  """.format(WeeklyAdDeltaPath))
except Exception as ex:
  ABC(DeltaTableCreateCheck = 0)
  loggerAtt.error(ex)
  err = ErrorReturn('Error', ex,'WeeklyAdDeltaPath deltaCreator')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

date_func =  udf (lambda x: datetime.datetime.strptime(str(x), '%m-%d-%Y').date(), DateType())



#Column renaming functions 
def weeklyadflat_weeklyadtable(s):
    return WeeklyAd_Renaming[s]
def change_col_name(s):
    return s in WeeklyAd_Renaming

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Weekly Ad transformation

# COMMAND ----------

WeeklyAd_Renaming = {"BRAND_DIVISION":"DIVISION", 
                  "MARKET_SEGMENT":"MARKET", 
                  "ATTR_AUSA_ITEM_NO":"ITEM_NUMBER", 
                  "CUSTOMERLOCATIONKEY":"STORE_NUMBER" 
                 }

# COMMAND ----------

def weeklyAdTransformation(processed_df,pipelineid):
  processed_df=processed_df.withColumn("ITEM_NUMBER",expr("substring(ITEM_NUMBER, 1, length(ITEM_NUMBER)-4)"))
  processed_df=processed_df.withColumn('ITEM_NUMBER', lpad(col('ITEM_NUMBER'),12,'0')) 
  processed_df=processed_df.withColumn('STORE_NUMBER', lpad(col('STORE_NUMBER'),4,'0')) 
  
  processed_df=processed_df.withColumn('AD_PROMO_START_DATE',regexp_replace(col("EVENT_NAME"), "[a-zA-Z\s]+", ""))
  processed_df=processed_df.withColumn('AD_PROMO_START_DATE',date_func(col("AD_PROMO_START_DATE")).cast(DateType()))
  processed_df=processed_df.withColumn('AD_PROMO_END_DATE', date_add(processed_df['AD_PROMO_START_DATE'], 6)) 
  processed_df=processed_df.withColumn("DEMANDTEC_PRISM_SID",when(col("SID_COPYCARD").isNull(),col("SOURCE_PROMOTIONID")).otherwise(col("SID_COPYCARD")))
  processed_df=processed_df.withColumn("INSERT_ID", lit(pipelineid))
  processed_df=processed_df.withColumn("INSERT_TIMESTAMP",current_timestamp())
  
  return processed_df

# COMMAND ----------

def readFile(file_location, infer_schema, first_row_is_header, delimiter,file_type):
  raw_df = spark.read.format(file_type) \
    .option("mode","PERMISSIVE") \
    .option("header", first_row_is_header) \
    .option("dateFormat", "yyyyMMdd") \
    .option("sep", delimiter) \
    .schema(weeklyAdRaw_schema) \
    .load(file_location)
  return raw_df

# COMMAND ----------

def WeeklyAdArchival(WeeklyAdDeltaPath,Date,Archivalfilelocation):
  weeklyadarchival_df = spark.read.format('delta').load(WeeklyAdDeltaPath)
  weeklyadarchival_df = weeklyadarchival_df.filter((datediff(to_date(current_date()),col('AD_PROMO_START_DATE')) >=183))
  if weeklyadarchival_df.count() >0:
 
    weeklyadarchival_df.write.mode('Append').format('parquet').save(Archivalfilelocation + "/" +Date+ "/" +"WeeklyAd_Archival_Records")
    deltaTable = DeltaTable.forPath(spark, WeeklyAdDeltaPath)
    deltaTable.delete((datediff(to_date(current_date()),col('AD_PROMO_START_DATE')) >=183))
    loggerAtt.info('========WeeklyAd Records Archival successful ========')
  else:
    loggerAtt.info('======== No WeeklyAd Records Archival Done ========')
  return weeklyadarchival_df

# COMMAND ----------

def weeklyAdWrite(WeeklyAdDeltaPath,WeeklyAd_OutboundPath):
  weeklyAdOutputDf = spark.read.format('delta').load(WeeklyAdDeltaPath)

  if weeklyAdOutputDf.count() >0:
    ABC(ValidRecordCount=weeklyAdOutputDf.count())
#     weeklyAdOutputDf = weeklyAdOutputDf.withColumn('CHAIN_ID', lit('AHOLD'))
    weeklyAdOutputDf.write.mode('overwrite').format('parquet').save(WeeklyAd_OutboundPath + "/" +"WeeklyAd_Output")
    loggerAtt.info('========WeeklyAd Records Output successful ========')
  else:
    loggerAtt.info('======== No WeeklyAd Records Output Done ========')
    ABC(ValidRecordCount=0)

# COMMAND ----------

def insertWeeklyAdRecords(weeklyAd_valid_Records):
  loggerAtt.info("Insert into Delta table initiated")
  temp_table_name = "weeklyAdRecords"

  weeklyAd_valid_Records.createOrReplaceTempView(temp_table_name)
  try:
    initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(WeeklyAdDeltaPath))
    loggerAtt.info(f"Initial count of records in Delta Table: {initial_recs.head(1)}")
    initial_recs = initial_recs.head(1)
    ABC(DeltaTableInitCount=initial_recs[0][0])
    spark.sql('''INSERT INTO delta.`{}` select * from weeklyAdRecords'''.format(WeeklyAdDeltaPath))
    appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(WeeklyAdDeltaPath))
    loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
    appended_recs = appended_recs.head(1)
    ABC(DeltaTableFinalCount=appended_recs[0][0])
    loggerAtt.info("Insert into Delta table successful")

  except Exception as ex:
    loggerAtt.info("Insert into Delta table failed and throwed error")
    loggerAtt.error(str(ex))
    ABC(DeltaTableCreateCheck = 0)
    ABC(DeltaTableInitCount='')
    ABC(DeltaTableFinalCount='')
    err = ErrorReturn('Error', ex,'InsertDeltaTable')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

def writeInvalidRecords(weeklyAd_invalidRecords, Invalid_RecordsPath):
  ABC(InvalidRecordSaveCheck = 1)
  try:
    if weeklyAd_invalidRecords.count() > 0:
      weeklyAd_invalidRecords.write.mode('Append').format('parquet').save(Invalid_RecordsPath + "/" +Date+ "/" + "WeeklyAd_invalidRecords_Invalid_Data")
      ABC(InvalidRecordCount = weeklyAd_invalidRecords.count())
      loggerAtt.info('======== Invalid weeklyAd Records write operation finished ========')

  except Exception as ex:
    ABC(InvalidRecordSaveCheck = 0)
    loggerAtt.error(str(ex))
    loggerAtt.info('======== Invalid record file write to ADLS location failed ========')
    err = ErrorReturn('Error', ex,'Writeoperationfailed for Invalid Record')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

def abcFramework(weeklyAd_duplicate_records, weeklyAd_raw_df):
  global fileRecordCount
  loggerAtt.info("ABC Framework function initiated")

  
  try:
    # Exception handling of schema records
    weeklyAdDf = weeklyAd_raw_df
    weeklyAd_nullRows = weeklyAdDf.where(reduce(lambda x, y: x & y, (col(x).isNull() for x in weeklyAdDf.columns)))
   
    ABC(NullValueCheck=1)
    loggerAtt.info("Dimension of the Null records:("+str(weeklyAd_nullRows.count())+"," +str(len(weeklyAd_nullRows.columns))+")")

    weeklyAd_raw_dfWithNoNull = weeklyAdDf.na.drop('all')
 
    ABC(DropNACheck = 1)
    NullValuCnt = weeklyAd_nullRows.count()
    ABC(NullValuCount = NullValuCnt)
    loggerAtt.info("Dimension of the Not null records:("+str(weeklyAd_raw_dfWithNoNull.count())+"," +str(len(weeklyAd_raw_dfWithNoNull.columns))+")")

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
    if (weeklyAd_raw_dfWithNoNull.groupBy(weeklyAd_raw_dfWithNoNull.columns).count().filter("count > 1").count()) > 0:
      loggerAtt.info("Duplicate records exists")
      weeklyAd_rawdf_withCount =weeklyAd_raw_dfWithNoNull.groupBy(weeklyAd_raw_dfWithNoNull.columns).count();
      weeklyAd_duplicate_records = weeklyAd_rawdf_withCount.filter("count > 1").drop('count')
      weeklyAdRecords= weeklyAd_rawdf_withCount.drop('count')

      DuplicateValueCnt = weeklyAd_duplicate_records.count()
      loggerAtt.info("Duplicate record Exists. No of duplicate records are " + str(weeklyAd_duplicate_records.count()))
      ABC(DuplicateValueCount=DuplicateValueCnt)
    else:
      loggerAtt.info("No duplicate records")
      weeklyAdRecords = weeklyAd_raw_dfWithNoNull  
      ABC(DuplicateValueCount=0)
    
    loggerAtt.info("ABC Framework function ended")
    weeklyAdRecords = weeklyAdRecords.dropDuplicates(['ATTR_AUSA_ITEM_NO', 'CUSTOMERLOCATIONKEY', 'SID_COPYCARD'])
    return weeklyAd_nullRows, weeklyAd_duplicate_records, weeklyAdRecords
  except Exception as ex:
    ABC(DuplicateValueCheck = 0)
    ABC(DuplicateValueCount='')
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'weeklyAd_ProblemRecs')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))


# COMMAND ----------

if __name__ == "__main__":
  
  ## File reading parameters
  loggerAtt.info('======== Input weeklyAd file processing initiated ========')
  file_location = str(file_location)
  file_type = "csv"
  infer_schema = "false"
  first_row_is_header = "true"
  delimiter = "|"
  weeklyAd_raw_df = None
  weeklyAd_duplicate_records = None
  footerRecord = None
  weeklyAdRecords = None
  weeklyAd_valid_Records = None
  PipelineID= str(PipelineID)
  folderDate = Date

  ## Step1: Reading input file
  try:
    weeklyAd_raw_df = readFile(file_location, infer_schema, first_row_is_header, delimiter, file_type)
    ABC(ReadDataCheck=1)
    RawDataCount = weeklyAd_raw_df.count()
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

  
  if weeklyAd_raw_df is not None:

    ## Step2: Performing ABC framework check
 
    weeklyAd_nullRows, weeklyAd_duplicate_records, weeklyAdRecords = abcFramework(weeklyAd_duplicate_records, weeklyAd_raw_df)

     ## renaming the vendor column names
    weeklyAdRecords = quinn.with_some_columns_renamed(weeklyadflat_weeklyadtable, change_col_name)(weeklyAdRecords) ##
    
    ## Step3: Performing Transformation
    try:  
      if weeklyAdRecords is not None: 

        weeklyAd_valid_Records = weeklyAdTransformation(weeklyAdRecords, PipelineID)
        
        ABC(TransformationCheck=1)
    except Exception as ex:
      ABC(TransformationCheck = 0)
      loggerAtt.error(ex)
      err = ErrorReturn('Error', ex,'weeklyAdTransformation')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
    
    ## Step4: Combining all duplicate records
    try:
      ## Combining Duplicate, null record
      if weeklyAd_duplicate_records is not None:
        invalidRecordsList = [weeklyAd_duplicate_records, weeklyAd_nullRows]
        weeklyAd_invalidRecords = reduce(DataFrame.unionAll, invalidRecordsList)
        ABC(InvalidRecordSaveCheck = 1)
        ABC(InvalidRecordCount = weeklyAd_invalidRecords.count())
      else:
        invalidRecordsList = [weeklyAd_nullRows]
        weeklyAd_invalidRecords = reduce(DataFrame.unionAll, invalidRecordsList)
     
    except Exception as ex:
      ABC(InvalidRecordSaveCheck = 0)
      ABC(InvalidRecordCount='')
      err = ErrorReturn('Error', ex,'Grouping Invalid record function')
      err = ErrorReturn('Error', ex,'InvalidRecordsSave')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
      

    loggerAtt.info("No of valid records: " + str(weeklyAd_valid_Records.count()))
    loggerAtt.info("No of invalid records: " + str(weeklyAd_invalidRecords.count()))
    
    ## Step5: Merging records into delta table
    if weeklyAd_valid_Records is not None:
      insertWeeklyAdRecords(weeklyAd_valid_Records)
       
    ## Step6: Writing Invalid records to ADLS location
    if weeklyAd_invalidRecords is not None:
      try:
        writeInvalidRecords(weeklyAd_invalidRecords, Invalid_RecordsPath)
      except Exception as ex:
        ABC(InvalidRecordSaveCheck = 0)
        ABC(InvalidRecordCount = '')
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'writeInvalidRecord')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
    try:
      loggerAtt.info("weeklyAd records Archival records initated")
      ABC(archivalCheck = 1)
      WeeklyAdArchival(WeeklyAdDeltaPath,Date,Archival_filePath)
    except Exception as ex:
      ABC(archivalCheck = 0)
      ABC(archivalInitCount='')
      ABC(archivalAfterCount='')
      err = ErrorReturn('Error', ex,'weeklyAdArchival')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
      ## Step 8: Write output file
    try:
      ABC(weeklyAdWriteCheck=1)
      weeklyAdWrite(WeeklyAdDeltaPath,WeeklyAd_OutboundPath)
    except Exception as ex:
      ABC(ValidRecordCount='')
      ABC(weeklyAdWriteCheck=0)
      err = ErrorReturn('Error', ex,'weeklyAdWrite')
      loggerAtt.error(ex)
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
  else:
    loggerAtt.info("Error in input file reading")
    
     
    
loggerAtt.info('======== Input weeklyAd file processing ended ========')

# COMMAND ----------

# MAGIC %sql select * from weeklyad_delta where ITEM_NUMBER = 0000000005 and STORE_NUMBER = 1

# COMMAND ----------

# spark.sql("""DROP TABLE IF EXISTS WeeklyAd_delta;""")
# dbutils.fs.rm('/mnt/ahold-centralized-price-promo/WeeklyAdfeed/Outbound/SDM/WeeklyAd_delta'
# ,recurse=True)

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

# df = readFile('/mnt/ahold-centralized-price-promo/WeeklyAdfeed/Inbound/RDS/2021/12/20/DT.WEEKLY.AD.FEED.TEXT', "false", "true", '|', "csv")
# df=df.withColumn('AD_PROMO_START_DATE',regexp_replace(col("EVENT_NAME"), "[a-zA-Z\s]+", ""))
# df=df.withColumn('AD_PROMO_START_DATE',date_func(col("AD_PROMO_START_DATE")).cast(DateType()))
# df=df.withColumn('AD_PROMO_END_DATE', date_add(df['AD_PROMO_START_DATE'], 7))

# display(df)

# COMMAND ----------

