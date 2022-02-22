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

custom_logfile_Name ='upcdelete_customlog'
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
# dbutils.widgets.text("outputDirectory","")
dbutils.widgets.text("fileName","")
dbutils.widgets.text("filePath","")
dbutils.widgets.text("directory","")
dbutils.widgets.text("Container","")
dbutils.widgets.text("pipelineID","")
dbutils.widgets.text("MountPoint","")
dbutils.widgets.text("deltaPath","")
dbutils.widgets.text("archivalFilePath","")
dbutils.widgets.text("logFilesPath","")
# dbutils.widgets.text("invalidRecordsPath","")
dbutils.widgets.text("clientId","")
dbutils.widgets.text("keyVaultName","")


FileName=dbutils.widgets.get("fileName")
Filepath=dbutils.widgets.get("filePath")
Directory=dbutils.widgets.get("directory")

# outputDirectory=dbutils.widgets.get("outputDirectory")
PipelineID=dbutils.widgets.get("pipelineID")
container=dbutils.widgets.get("Container")
mount_point=dbutils.widgets.get("MountPoint")
DeltaPath=dbutils.widgets.get("deltaPath")
Archival_filePath=dbutils.widgets.get("archivalFilePath")
Log_FilesPath=dbutils.widgets.get("logFilesPath")
# Invalid_RecordsPath=dbutils.widgets.get("invalidRecordsPath")
clientId=dbutils.widgets.get("clientId")
keyVaultName=dbutils.widgets.get("keyVaultName")
PipelineID =dbutils.widgets.get("pipelineID")
Date = datetime.datetime.now(timezone("America/Halifax")).strftime("%Y-%m-%d")
inputSource= 'abfss://' + Directory + '@' + container + '.dfs.core.windows.net/'
# outputSource= 'abfss://' + outputDirectory + '@' + container + '.dfs.core.windows.net/'
file_location = '/mnt' + '/' + Directory + '/' + Filepath +'/' + FileName 


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

def readFile(file_location):
  schemaUPCDelete = StructType([
    StructField('NO', LongType(), True),
    StructField('UPC', LongType(), False),
    StructField('STORE_NO', StringType(), False)
    ])


  raw_df = spark.read.format('csv') \
      .option("mode","PERMISSIVE") \
      .option("header", 'false') \
      .option("delimiter", '|') \
      .schema(schemaUPCDelete) \
      .load(file_location)
  raw_df = raw_df.withColumn('STORE_NO', lpad(col('STORE_NO'),4,'0').cast(StringType()))

#   itemMain = spark.sql("""select * from delta.`{}`;""".format(deltaPath))

  # itemMain = itemMain.withColumn('SMA_STORE', col('SMA_STORE').cast(IntegerType()))

  # itemMainTemp = itemMain.join(raw_df, [1390050001 == itemMain.SMA_GTIN_NUM, '0001' == itemMain.SMA_STORE], how='=outer').select([col(xx) for xx in itemMain.columns]).


  temp_table_name = "UPCDelete"
  raw_df.createOrReplaceTempView(temp_table_name)
  # display(itemMainTemp)

#   ItemMainDeltaPath = '/mnt/ahold-centralized-price-promo/Item/Outbound/DeltaTables/ItemMain1'
  return raw_df

# COMMAND ----------

def getInactiveUPCs():
  try:
    df = spark.sql('''select SMA_STORE, SMA_GTIN_NUM, SMA_ITEM_STATUS from delta.`{}`as A left anti join UPCDelete B on A.SMA_STORE = B.STORE_NO and A.SMA_GTIN_NUM = B.UPC'''.format(DeltaPath))
    loggerAtt.info("Inactive UPCs Exists. No of Inactive UPCs are " + str(df.count()))
    ABC(getInactiveUPCsCheck = 1)
    ABC(InactiveUPCCount = df.count())
    return df
  except Exception as ex:
      ABC(getInactiveUPCsCheck = 0)
      ABC(InactiveUPCCount='')
      err = ErrorReturn('Error', ex,'getInactiveUPCs')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))


# COMMAND ----------

def transformations(df, JOB_ID):
  df = df.withColumn("SMA_ITEM_STATUS", when(col("SMA_ITEM_STATUS")!='X', 'X').otherwise(col("SMA_ITEM_STATUS")))
  df = df.withColumn("SMA_STATUS_DATE", current_date())
  df = df.withColumn("LAST_UPDATE_TIMESTAMP", current_timestamp())
  df = df.withColumn("LAST_UPDATE_ID",lit(JOB_ID))
  return df
  

# COMMAND ----------

def upsertRecords(df):
  loggerAtt.info("Merge into Delta table initiated")
  temp_table_name = "UPCDeleteItemMainJoin"
  df.createOrReplaceTempView(temp_table_name)
  try:
    spark.sql('''MERGE INTO delta.`{}` as ItemMain
        USING UPCDeleteItemMainJoin
        on ItemMain.SMA_STORE = UPCDeleteItemMainJoin.SMA_STORE and
        ItemMain.SMA_GTIN_NUM = UPCDeleteItemMainJoin.SMA_GTIN_NUM
        WHEN MATCHED Then
                Update Set ItemMain.SMA_ITEM_STATUS = UPCDeleteItemMainJoin.SMA_ITEM_STATUS,
                ItemMain.SMA_STATUS_DATE = UPCDeleteItemMainJoin.SMA_STATUS_DATE,
                ItemMain.LAST_UPDATE_TIMESTAMP = UPCDeleteItemMainJoin.LAST_UPDATE_TIMESTAMP
        '''.format(DeltaPath))
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

def Archival(DeltaPath,Date,Archivalfilelocation):
  archival_df = spark.read.format('delta').load(DeltaPath)
  archival_df = archival_df.filter(col('SMA_ITEM_STATUS') == 'X')
  if archival_df.count() >0:
    
    archival_df.write.mode('Append').format('parquet').save(Archivalfilelocation + "/" +Date+ "/" +"UPC_Archival_Records")
    
#     add count
    loggerAtt.info('========UPC Records Archival successful ========')
  else:
    loggerAtt.info('======== No UPC Records Archival Done ========')

# COMMAND ----------

if __name__ == "__main__":
  PipelineID= str(PipelineID)
  try:
    upc_raw_df = readFile(file_location)
    ABC(ReadDataCheck=1)
    RawDataCount = upc_raw_df.count()
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
      
  if upc_raw_df is not None:
    
    inactiveUPCs = getInactiveUPCs()

    try:
      if inactiveUPCs is not None:
        deletedUPCs = transformations(inactiveUPCs, PipelineID)
        ABC(TransformationCheck=1)
    except Exception as ex:
      ABC(TransformationCheck = 0)
      loggerAtt.error(ex)
      err = ErrorReturn('Error', ex,'Transformation')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)

    if deletedUPCs is not None:
      upsertRecords(deletedUPCs)
      
    try:
      loggerAtt.info("UPC records Archival records initated")
      ABC(archivalCheck = 1)
      Archival(DeltaPath,Date,Archival_filePath)
    except Exception as ex:
      ABC(archivalCheck = 0)
      ABC(archivalInitCount='')
      ABC(archivalAfterCount='')
      err = ErrorReturn('Error', ex,'Archival')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
  else:
    loggerAtt.info("Error in input file reading")
    
     
    
loggerAtt.info('======== UPC Delete processing ended ========')

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