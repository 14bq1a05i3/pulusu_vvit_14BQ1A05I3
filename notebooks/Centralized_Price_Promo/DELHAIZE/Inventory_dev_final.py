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

# MAGIC %md
# MAGIC 
# MAGIC ## Calling Logger

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/Logging

# COMMAND ----------

custom_logfile_Name ='inventory_dailymaintainence_customlog'
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
#dbutils.widgets.removeAll()
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
dbutils.widgets.text("POSemergencyFlag","")

FileName=dbutils.widgets.get("fileName")
Filepath=dbutils.widgets.get("filePath")
Directory=dbutils.widgets.get("directory")
container=dbutils.widgets.get("container")
PipelineID=dbutils.widgets.get("pipelineID")
mount_point=dbutils.widgets.get("MountPoint")
InventoryDeltaPath=dbutils.widgets.get("deltaPath")
Log_FilesPath=dbutils.widgets.get("logFilesPath")
Invalid_RecordsPath=dbutils.widgets.get("invalidRecordsPath") # Make this 
Date = datetime.datetime.now(timezone("America/Halifax")).strftime("%Y-%m-%d")
file_location = '/mnt' + '/' + Directory + '/' + Filepath +'/' + FileName 
source= 'abfss://' + Directory + '@' + container + '.dfs.core.windows.net/'
clientId=dbutils.widgets.get("clientId")
keyVaultName=dbutils.widgets.get("keyVaultName")
itemTempEffDeltaPath=dbutils.widgets.get("itemTempEffDeltaPath")
inventoryEffDeltaPath=dbutils.widgets.get("feedEffDeltaPath")
POSemergencyFlag=dbutils.widgets.get("POSemergencyFlag")

unifiedInventoryFields = ["INVENTORY_CODE",
"INVENTORY_QTY_ON_HAND",
"INVENTORY_SUPER_CATEGORY",
"INVENTORY_MAJOR_CATEGORY",
"INVENTORY_INTMD_CATEGORY",
"STORE_ID",
"UPC",
"INVENTORY_BANNER_ID"]

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
#   err.exit()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code to determine the  type of file maintainence to be processed based on directory path

# COMMAND ----------

processing_file='Delta'

# COMMAND ----------

#Defining the schema for Inventory Table
loggerAtt.info('========Schema definition initiated ========')
inventoryRaw_schema = StructType([
  StructField("INVENTORY_BANNER_ID",StringType(),False),
  StructField("INVENTORY_LOCATION",StringType(),False),
  StructField("INVENTORY_UPC",LongType(),False),
  StructField("INVENTORY_INVENTORY_CODE",StringType(),False),
  StructField("INVENTORY_QUANTITY",FloatType(),False),
  StructField("INVENTORY_SUPER_CATG_ID",IntegerType(),False),
  StructField("INVENTORY_MJR_CATG_ID",IntegerType(),False),
  StructField("INVENTORY_INTMD_CATG_ID",IntegerType(),False)
  ])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Delta table creation

# COMMAND ----------

try:
  ABC(DeltaTableCreateCheck=1)
  spark.sql("""
  CREATE TABLE IF NOT EXISTS Inventory_delta (
  INVENTORY_BANNER_ID STRING,
  LOCATION STRING,
  INVENTORY_STORE_ID STRING,
  INVENTORY_UPC LONG,
  INVENTORY_CODE STRING,
  INVENTORY_QTY_ON_HAND STRING,
  INVENTORY_SUPER_CATEGORY STRING,
  INVENTORY_MAJOR_CATEGORY STRING,
  INVENTORY_INTMD_CATEGORY STRING,
  INSERT_ID STRING,
  INSERT_TIMESTAMP TIMESTAMP,
  LAST_UPDATE_ID STRING,
  LAST_UPDATE_TIMESTAMP TIMESTAMP
  )
  USING delta
  Location '{}'
  PARTITIONED BY (INVENTORY_STORE_ID)
  """.format(InventoryDeltaPath))
except Exception as ex:
  ABC(DeltaTableCreateCheck = 0)
  loggerAtt.error(ex)
  err = ErrorReturn('Error', ex,'inventoryDeltaPath deltaCreator')
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
  PARTITIONED BY (INVENTORY_STORE_ID)
  """.format(inventoryEffDeltaPath))
except Exception as ex:
  ABC(DeltaTableCreateCheck = 0)
  loggerAtt.error(ex)
  err = ErrorReturn('Error', ex,'InventoryTemp deltaCreator')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Inventory Processing

# COMMAND ----------

date_func =  udf (lambda x: datetime.datetime.strptime(str(x), '%Y-%m-%d').date(), DateType())

#Column renaming functions 
def inventoryflat_storetable(s):
    return Inventory_Renaming[s]
def change_col_name(s):
    return s in Inventory_Renaming

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Inventory Transformation

# COMMAND ----------

def inventoryTransformation(processed_df, JOB_ID):
  processed_df=processed_df.withColumn("INSERT_TIMESTAMP",current_timestamp())
  processed_df=processed_df.withColumn("LAST_UPDATE_ID",lit(JOB_ID))
  processed_df=processed_df.withColumn("LAST_UPDATE_TIMESTAMP",current_timestamp())
  processed_df=processed_df.withColumn("INSERT_ID", lit(JOB_ID))
  processed_df = processed_df.withColumn('LOCATION', format_string("%010d", col('LOCATION').cast('int'))) #Length of 10
  # New Derived Column
  processed_df = processed_df.withColumn("INVENTORY_STORE_ID", processed_df['LOCATION'].substr(-4,4)) # New derived column STORE_ID
  processed_df = processed_df.withColumn('INVENTORY_BANNER_ID', regexp_replace(col("INVENTORY_BANNER_ID"), " ", ""))
  return processed_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Transformation for unified item master table

# COMMAND ----------

def itemMasterTransformation(processed_df):

 
  return processed_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Inventory Renaming dictionary

# COMMAND ----------

Inventory_Renaming = {"INVENTORY_LOCATION":"LOCATION",
                      "INVENTORY_INVENTORY_CODE":"INVENTORY_CODE",
                      "INVENTORY_QUANTITY":"INVENTORY_QTY_ON_HAND",
                      "INVENTORY_SUPER_CATG_ID":"INVENTORY_SUPER_CATEGORY",
                      "INVENTORY_MJR_CATG_ID":"INVENTORY_MAJOR_CATEGORY",
                      "INVENTORY_INTMD_CATG_ID":"INVENTORY_INTMD_CATEGORY"
                 }

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Inventory file reading

# COMMAND ----------

def readFile(file_location, infer_schema, first_row_is_header, delimiter,file_type):
  raw_df = spark.read.format(file_type) \
    .option("mode","PERMISSIVE") \
    .option("header", first_row_is_header) \
    .option("dateFormat", "yyyyMMdd") \
    .option("sep", delimiter) \
    .schema(inventoryRaw_schema) \
    .load(file_location)
  return raw_df

# COMMAND ----------

def typeCheck(s):
  try: 
      int(s)
  except ValueError:
      return False

typeCheckUDF = udf(typeCheck)

#Column renaming functions 
def inventoryflat_inventorytable(s):
    return Inventory_Renaming[s]
def change_col_name(s):
    return s in Inventory_Renaming

# COMMAND ----------

def abcFramework(headerFooterRecord, inventory_duplicate_records, inventory_raw_df):
  global fileRecordCount
  loggerAtt.info("ABC Framework function initiated")
  try:
    # Fetching all inventory id records with non integer value
#     headerFooterRecord =inventory_raw_df.filter(typeCheckUDF(col('INVENTORY_BANNER_ID')) == False)
    headerFooterRecord = inventory_raw_df.filter(inventory_raw_df["INVENTORY_BANNER_ID"].rlike("BOF|EOF"))
    # Fetching EOF and BOF records and checking whether it is of length one each
    eofDf = headerFooterRecord.filter(inventory_raw_df["INVENTORY_BANNER_ID"].rlike("^:EOF"))
    bofDf = headerFooterRecord.filter(inventory_raw_df["INVENTORY_BANNER_ID"].rlike("^:BOF"))
    ABC(BOFEOFCheck=1)
    EOFcnt = eofDf.count()
    BOFcnt = bofDf.count()
    ABC(EOFCount=EOFcnt)
    ABC(BOFCount=BOFcnt)
    loggerAtt.info("EOF file record count is " + str(eofDf.count()))
    loggerAtt.info("BOF file record count is " + str(bofDf.count()))
    
    # If there are multiple header/Footer then the file is invalid
    ##if ((eofDf.count() != 1) or (bofDf.count() != 1)):
    ##  raise Exception('Error in EOF or BOF value')
    inventoryDf = inventory_raw_df.filter(inventory_raw_df["INVENTORY_BANNER_ID"].rlike("FDLN|HAN"))
    fileRecordCount = int(re.findall("\d+", eofDf.select('INVENTORY_BANNER_ID').toPandas()['INVENTORY_BANNER_ID'][0])[0])
#     BOFDate = bofDf.select("BOF-DATE").head()[0]
    loggerAtt.info("Counting Records")
    actualRecordCount = int(inventoryDf.count() + headerFooterRecord.count() - 2)
    
    loggerAtt.info("Record Count mentioned in file: " + str(fileRecordCount))
    loggerAtt.info("Actual no of record in file: " + str(actualRecordCount))

  # Checking to see if the count matched the actual record count
    #if actualRecordCount != fileRecordCount:
    ##  raise Exception('Record count mismatch. Actual count ' + str(actualRecordCount) + ', file record count ' + str(fileRecordCount))

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
  

  try:
    # Exception handling of schema records
    inventory_nullRows = inventoryDf.where(reduce(lambda x, y: x | y, (col(x).isNull() for x in inventoryDf.columns)))
    ABC(NullValueCheck=1)
    loggerAtt.info("Dimension of the Null records:("+str(inventory_nullRows.count())+"," +str(len(inventory_nullRows.columns))+")")

    inventory_raw_dfWithNoNull = inventoryDf.na.drop()
    ABC(DropNACheck = 1)
    NullValuCnt = inventory_nullRows.count()
    ABC(NullValuCount = NullValuCnt)
    loggerAtt.info("Dimension of the Not null records:("+str(inventory_raw_dfWithNoNull.count())+"," +str(len(inventory_raw_dfWithNoNull.columns))+")")
    
  except Exception as ex:
    NullValueCheck = 0
    NullValuCnt=''
    ABC(NullValueCheck=0)
    ABC(NullValuCount = NullValuCnt) 
    err = ErrorReturn('Error', ex,'NullRecordHandling')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
#     err = ErrorReturn('Error', ex,'ABC Framework check Error')
#     err.exit()
    
  try:
    # removing duplicate record 
    ABC(DuplicateValueCheck = 1)
    if (inventory_raw_dfWithNoNull.groupBy(inventory_raw_dfWithNoNull.columns).count().filter("count > 1").count()) > 0:
      loggerAtt.info("Duplicate records exists")
      inventory_rawdf_withCount = inventory_raw_dfWithNoNull.groupBy(inventory_raw_dfWithNoNull.columns).count();
      inventory_duplicate_records = inventory_rawdf_withCount.filter("count > 1").drop('count')
      inventoryRecords= inventory_rawdf_withCount.drop('count')
      loggerAtt.info("Duplicate record Exists. No of duplicate records are " + str(inventory_duplicate_records.count()))
      DuplicateValueCnt = inventory_duplicate_records.count()
      ABC(DuplicateValueCount=DuplicateValueCnt)
    else:
      loggerAtt.info("No duplicate records")
      inventoryRecords = inventory_raw_dfWithNoNull
      ABC(DuplicateValueCount=0)
    
    loggerAtt.info("ABC Framework function ended")
    return headerFooterRecord, inventory_nullRows, inventory_duplicate_records, inventoryRecords
  except Exception as ex:
    ABC(DuplicateValueCheck = 0)
    ABC(DuplicateValueCount='')
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'inventory_ProblemRecs')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
#     err = ErrorReturn('Error', ex,'ABC Framework check Error')
#     err.exit()

# COMMAND ----------

def itemMasterRecordsModified(itemTempEffDeltaPath, inventoryEffDeltaPath, InventoryDeltaPath, Date, unifiedInventoryFields):
  
  unifiedInventoryFields = ['INVENTORY_CODE', 'INVENTORY_QTY_ON_HAND', 'INVENTORY_SUPER_CATEGORY', 'INVENTORY_MAJOR_CATEGORY', 'INVENTORY_INTMD_CATEGORY', 'BANNER_ID', 'SMA_GTIN_NUM', 'SMA_DEST_STORE']
  
  inventoryDelta = spark.read.format('delta').load(InventoryDeltaPath)
  itemTempEffDelta = spark.sql('''select SMA_GTIN_NUM, BANNER_ID, SMA_DEST_STORE from delta.`{}`'''.format(itemTempEffDeltaPath))
  
  inventoryDelta = itemTempEffDelta.join(inventoryDelta, [itemTempEffDelta.SMA_GTIN_NUM == inventoryDelta.INVENTORY_UPC, itemTempEffDelta.SMA_DEST_STORE == inventoryDelta.INVENTORY_STORE_ID, itemTempEffDelta.BANNER_ID == inventoryDelta.INVENTORY_BANNER_ID], how='left').select([col(xx) for xx in unifiedInventoryFields])
  
  inventoryDelta = inventoryDelta.withColumn('INVENTORY_BANNER_ID', col('BANNER_ID'))
  inventoryDelta = inventoryDelta.withColumn('INVENTORY_UPC', col('SMA_GTIN_NUM'))
  inventoryDelta = inventoryDelta.withColumn('INVENTORY_STORE_ID', col('SMA_DEST_STORE'))
  inventoryDelta = inventoryDelta.drop('SMA_DEST_STORE')
  inventoryDelta = inventoryDelta.drop('SMA_GTIN_NUM')
  inventoryDelta = inventoryDelta.drop('BANNER_ID')
  
  inventoryEffTemp = spark.sql('''DELETE FROM delta.`{}`'''.format(inventoryEffDeltaPath))
  inventoryDelta.write.partitionBy('INVENTORY_STORE_ID').format('delta').mode('append').save(inventoryEffDeltaPath)
  
  ABC(UnifiedRecordInventoryCount = inventoryDelta.count())
  ABC(UnifiedRecordItemCount = itemTempEffDelta.count())
  loggerAtt.info("UnifiedRecordInventoryCount:" +str(inventoryDelta.count()))
  loggerAtt.info("UnifiedRecordItemCount:" +str(itemTempEffDelta.count()))
  loggerAtt.info("itemMasterRecords fetch successful")


# COMMAND ----------

def upsertInventoryRecords(inventory_valid_Records):
  loggerAtt.info("Merge into Delta table initiated")
  temp_table_name = "InventoryRecords"

  inventory_valid_Records.createOrReplaceTempView(temp_table_name)
  try:
    spark.sql('''MERGE INTO delta.`{}` as inventory
    USING InventoryRecords 
    ON inventory.INVENTORY_UPC = InventoryRecords.INVENTORY_UPC
    and inventory.INVENTORY_STORE_ID = InventoryRecords.INVENTORY_STORE_ID
    and inventory.INVENTORY_BANNER_ID = InventoryRecords.INVENTORY_BANNER_ID
    WHEN MATCHED Then 
            Update Set inventory.INVENTORY_BANNER_ID=InventoryRecords.INVENTORY_BANNER_ID,
            inventory.LOCATION=InventoryRecords.LOCATION,
            inventory.INVENTORY_UPC=InventoryRecords.INVENTORY_UPC,
            inventory.INVENTORY_CODE=InventoryRecords.INVENTORY_CODE,
            inventory.INVENTORY_QTY_ON_HAND=InventoryRecords.INVENTORY_QTY_ON_HAND,
            inventory.INVENTORY_SUPER_CATEGORY=InventoryRecords.INVENTORY_SUPER_CATEGORY,
            inventory.INVENTORY_MAJOR_CATEGORY=InventoryRecords.INVENTORY_MAJOR_CATEGORY,
            inventory.INVENTORY_INTMD_CATEGORY=InventoryRecords.INVENTORY_INTMD_CATEGORY,
            inventory.INSERT_ID=InventoryRecords.INSERT_ID,
            inventory.INSERT_TIMESTAMP=InventoryRecords.INSERT_TIMESTAMP,
            inventory.LAST_UPDATE_ID=InventoryRecords.LAST_UPDATE_ID,
            inventory.LAST_UPDATE_TIMESTAMP=InventoryRecords.LAST_UPDATE_TIMESTAMP,
            inventory.INVENTORY_STORE_ID=InventoryRecords.INVENTORY_STORE_ID

             WHEN NOT MATCHED THEN INSERT * '''.format(InventoryDeltaPath))
    loggerAtt.info("Merge into Delta table successful")

  except Exception as ex:
    loggerAtt.info("Merge into Delta table failed and throwed error")
    loggerAtt.error(str(ex))
    err = ErrorReturn('Error', ex,'MergeDeltaTable')
    err.exit()

# COMMAND ----------

# MAGIC  %md 
# MAGIC  
# MAGIC  ##write processed inventory files to ADLS 

# COMMAND ----------

def writeInvalidRecords(inventory_invalidRecords, Invalid_RecordsPath):
  try:
    if inventory_invalidRecords.count() > 0:
      inventory_invalidRecords.write.mode('Append').format('parquet').save(Invalid_RecordsPath + "/" +Date+ "/" + "Inventory_Invalid_Data")
      loggerAtt.info('======== Invalid Inventory Records write operation finished ========')

  except Exception as ex:
    ABC(InvalidRecordSaveCheck = 0)
    loggerAtt.error(str(ex))
    loggerAtt.info('======== Invalid record file write to ADLS location failed ========')
    err = ErrorReturn('Error', ex,'Writeoperationfailed for Invalid Record')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

if __name__ == "__main__":
  
  ## File reading parameters
  loggerAtt.info('======== Input inventory file processing initiated ========')
  file_location = str(file_location)
  file_type = "csv"
  infer_schema = "false"
  first_row_is_header = "false"
  delimiter = "|"
  inventory_raw_df = None
  inventory_duplicate_records = None
  headerFooterRecord = None
  inventoryRecords = None
  inventory_valid_Records = None
  PipelineID= str(PipelineID)
  folderDate = Date
  if POSemergencyFlag == 'Y':
    loggerAtt.info('Feed processing happening for POS Emergency feed')
    try:
      ABC(itemMasterRecordsCheck=1)  
      itemMasterRecordsModified(itemTempEffDeltaPath, inventoryEffDeltaPath, InventoryDeltaPath, Date, unifiedInventoryFields)
    except Exception as ex:
      ABC(itemMasterCount='')
      ABC(itemMasterRecordsCheck=0)
      err = ErrorReturn('Error', ex,'itemMasterRecordsModified')
      loggerAtt.error(ex)
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
  else:
    loggerAtt.info('Feed processing happening for POS Daile/Weekly feed')
    ## Step1: Reading input file
    try:
      inventory_raw_df = readFile(file_location, infer_schema, first_row_is_header, delimiter, file_type)
      ABC(ReadDataCheck=1)
      RawDataCount = inventory_raw_df.count()
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

    if inventory_raw_df is not None:

      ## Step2: Performing ABC framework check

      headerFooterRecord, inventory_nullRows, inventory_duplicate_records, inventoryRecords = abcFramework(headerFooterRecord, inventory_duplicate_records, inventory_raw_df)


       ## renaming the vendor column names
      inventoryRecords = quinn.with_some_columns_renamed(inventoryflat_inventorytable, change_col_name)(inventoryRecords) ##

      ## Step3: Performing Transformation
      try:  
        if inventoryRecords is not None: 
          inventory_valid_Records = inventoryTransformation(inventoryRecords, PipelineID)
          ABC(TransformationCheck=1)
      except Exception as ex:
        ABC(TransformationCheck = 0)
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'inventoryTransformation')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
        err = ErrorReturn('Error', ex,'inventoryTransformation')
        err.exit()

      ## Step4: Combining all duplicate records
      try:
        ## Combining Duplicate, null record
        if inventory_duplicate_records is not None:
          invalidRecordsList = [headerFooterRecord, inventory_duplicate_records, inventory_nullRows]
          inventory_invalidRecords = reduce(DataFrame.unionAll, invalidRecordsList)
          ABC(InvalidRecordSaveCheck = 1)
          ABC(InvalidRecordCount = inventory_invalidRecords.count())
        else:
          invalidRecordsList = [headerFooterRecord, inventory_nullRows]
          inventory_invalidRecords = reduce(DataFrame.unionAll, invalidRecordsList)
      except Exception as ex:
        ABC(InvalidRecordSaveCheck = 0)
        ABC(InvalidRecordCount='')
        err = ErrorReturn('Error', ex,'Grouping Invalid record function')
        err = ErrorReturn('Error', ex,'InvalidRecordsSave')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
  #       err.exit()    

      loggerAtt.info("No of valid records: " + str(inventory_valid_Records.count()))
      loggerAtt.info("No of invalid records: " + str(inventory_invalidRecords.count()))

      ## Step5: Merging records into delta table
      if inventory_valid_Records is not None:
          upsertInventoryRecords(inventory_valid_Records)

      ## Step6: Writing Invalid records to ADLS location
      if inventory_invalidRecords is not None:
        writeInvalidRecords(inventory_invalidRecords, Invalid_RecordsPath)

      try:
        ABC(itemMasterRecordsCheck=1)  
        itemMasterRecordsModified(itemTempEffDeltaPath, inventoryEffDeltaPath, InventoryDeltaPath, Date, unifiedInventoryFields)
      except Exception as ex:
        ABC(itemMasterCount='')
        ABC(itemMasterRecordsCheck=0)
        err = ErrorReturn('Error', ex,'itemMasterRecordsModified')
        loggerAtt.error(ex)
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))  

    else:
      loggerAtt.info("Error in input file reading")
    
loggerAtt.info('======== Input inventory file processing ended ========')

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
# err.exit()