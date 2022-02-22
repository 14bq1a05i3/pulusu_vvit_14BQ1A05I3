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
    self.time = datetime.datetime.now(timezone("America/Chicago")).isoformat()
  def exit(self):
    dbutils.notebook.exit(json.dumps(self.__dict__))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ##Widgets for getting dynamic paramters from ADF 

# COMMAND ----------

loggerAtt.info("========Widgets call initiated==========")
dbutils.widgets.text("FileName","")
dbutils.widgets.text("FilePath","")
dbutils.widgets.text("Directory","")
dbutils.widgets.text("container","")
dbutils.widgets.text("FileExtension","")
dbutils.widgets.text("PipelineID","")
dbutils.widgets.text("MountPoint","")
dbutils.widgets.text("StoreDeltaPath","")
dbutils.widgets.text("Archival_FilePath","")
dbutils.widgets.text("Store_OutboundPath","")
dbutils.widgets.text("Log_FilesPath","")
dbutils.widgets.text("Invalid_RecordsPath","")



FileName=dbutils.widgets.get("FileName")
Filepath=dbutils.widgets.get("FilePath")
Directory=dbutils.widgets.get("Directory")
container=dbutils.widgets.get("container")
PipelineID=dbutils.widgets.get("PipelineID")
mount_point=dbutils.widgets.get("MountPoint")
StoreDeltaPath=dbutils.widgets.get("StoreDeltaPath")
Archival_filePath=dbutils.widgets.get("Archival_FilePath")
Store_OutboundPath=dbutils.widgets.get("Store_OutboundPath")
Log_FilesPath=dbutils.widgets.get("Log_FilesPath")
Invalid_RecordsPath=dbutils.widgets.get("Invalid_RecordsPath")
Date = datetime.datetime.now(timezone("America/Chicago")).strftime("%Y-%m-%d")
file_location = '/mnt' + '/' + Directory + '/' + Filepath +'/' + FileName 
source= 'abfss://' + Directory + '@' + container + '.dfs.core.windows.net/'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Mounting ADLS location

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/Mount_Point_Creation

# COMMAND ----------

try:
  mounting(mount_point, source)
except Exception as ex:
  # send error message to ADF and send email notification
  loggerAtt.error(str(ex))
  err = ErrorReturn('Error', ex,'Mounting')
  err.exit()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code to determine the  type of file maintainence to be processed based on directory path

# COMMAND ----------

#Defining the schema for Store Table
loggerAtt.info('========Schema definition initiated ========')
storeRaw_schema = StructType([
                           StructField("DEST_STORE",IntegerType(),False),
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

spark.sql("""
CREATE TABLE IF NOT EXISTS Store_delta (
DEST_STORE INTEGER,
STORE_NUMBER INTEGER,
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
  processed_df=processed_df.withColumn("WIC_ELIGIBLE_IND",when(col("WIC_ELIGIBLE_IND")==lit(None), 'Y').otherwise('N'))
  processed_df=processed_df.withColumn("AREA",col("AREA").cast(IntegerType()))
  processed_df=processed_df.withColumn("STORE_OPEN_DATE",when(col("STORE_OPEN_DATE")=='0000-00-00', lit(None)).otherwise(date_format(date_func(col("STORE_OPEN_DATE")), 'yyyy-MM-dd')).cast(DateType()))
  processed_df=processed_df.withColumn('DEST_STORE', lpad(col('DEST_STORE'),4,'0')) 
  processed_df=processed_df.withColumn('STORE_NUMBER', lpad(col('STORE_NUMBER'),4,'0'))                                   
  processed_df=processed_df.withColumn("STORE_CLOSE_DATE",when(col("STORE_CLOSE_DATE")!='0000-00-00', date_format(date_func(col("STORE_OPEN_DATE")), 'yyyy-MM-dd')).otherwise(lit(None)).cast(DateType()))
     
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
    loggerAtt.info('========Store Records Archival successful ========')
  else:
    loggerAtt.info('======== No Store Records Archival Done ========')
  return storearchival_df

# COMMAND ----------

if __name__ == "__main__":
  
  ## File reading parameters
  loggerAtt.info('======== Input Store file processing initiated ========')
  file_location = str(file_location)
  file_type = "csv"
  infer_schema = "false"
  first_row_is_header = "true"
  delimiter = "|"
  Store_transformed_df = None
  raw_df = None
  store_duplicate_records = None
  invalid_transformed_df = None
  PipelineID= str(PipelineID)
  
  p_filename = "store_dailymaintainence_custom_log"
  #folderDate = Date

  ## Reading input file
  try:
    store_raw_df = readFile(file_location, infer_schema, first_row_is_header, delimiter, file_type)
    
    loggerAtt.info("record count check initiated")
    store_raw_df=store_raw_df.withColumn("BATCH_SERIAL",col("BATCH_SERIAL").cast(IntegerType()))
  except Exception as ex:
    if 'java.io.FileNotFoundException' in str(ex):
      loggerAtt.error('File does not exists')
    else:
      loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'readFile')
    err.exit()
  
  if store_raw_df is not None:
    ## renaming the store column names
    store_renamed_df = quinn.with_some_columns_renamed(storeflat_storetable, change_col_name)(store_raw_df) ##
    
    # Exception handling of schema records
    store_nullRows = store_renamed_df.where(reduce(lambda x, y: x | y, (col(x).isNull() for x in store_renamed_df.columns)))

    loggerAtt.info("Dimension of the Null records:("+str(store_nullRows.count())+"," +str(len(store_nullRows.columns))+")")

    store_raw_dfWithNoNull = store_renamed_df.na.drop()

    loggerAtt.info("Dimension of the Not null records:("+str(store_raw_dfWithNoNull.count())+"," +str(len(store_raw_dfWithNoNull.columns))+")")

    # removing duplicate record 
    if (store_raw_dfWithNoNull.groupBy(store_raw_dfWithNoNull.columns).count().filter("count > 1").count()) > 0:
      loggerAtt.info("Duplicate records exists")
      store_rawdf_withCount = store_raw_dfWithNoNull.groupBy(store_raw_dfWithNoNull.columns).count();
      store_duplicate_records = store_rawdf_withCount.filter("count > 1").drop('count')
      store_raw_df= store_rawdf_withCount.drop('count')
    else:
      store_raw_df = store_raw_dfWithNoNull

  

    ## Applying Store transformation
    try:
      loggerAtt.info("Calling transformation function")
      store_transformed_df = storeTransformation(store_raw_df,PipelineID) 
    except Exception as ex:
      loggerAtt.error(ex)
      err = ErrorReturn('Error', ex,'storeTransformation')
      err.exit()
    
    # Read delta table for store delete
    if store_transformed_df is not None:
        store_valid_Records = store_transformed_df
      
        loggerAtt.info('======== Input Store file write operation initiated ========')        
    else:
      loggerAtt.info("Error in transformation function")
    
    ## Combining Duplicate, null record and invalid store records
    if store_duplicate_records is not None:
      invalidRecordsList = [store_duplicate_records, store_nullRows]
      store_invalidRecords = reduce(DataFrame.unionAll, invalidRecordsList)
    else:
#       invalidRecordsList = [store_nullRows]
      store_invalidRecords = store_nullRows
    
  else:
    loggerAtt.info("Error in input file reading")
    
loggerAtt.info('======== Input Store file processing ended ========')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Merge into Delta table 

# COMMAND ----------

loggerAtt.info("Merge into Delta table initiated")
temp_table_name = "storeRecords"

store_valid_Records.createOrReplaceTempView(temp_table_name)
try:
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
  loggerAtt.info("Merge into Delta table successful")
  
except Exception as ex:
  loggerAtt.info("Merge into Delta table failed and throwed error")
  loggerAtt.error(str(ex))
  err = ErrorReturn('Error', ex,'MergeDeltaTable')
  err.exit()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Archiving Store records

# COMMAND ----------

try:
  loggerAtt.info("Store records Archival records initated")
  StoreArchival(StoreDeltaPath,Date,Archival_filePath)
except Exception as ex:
  loggerAtt.info("store archival in store feed script throwed error")
  loggerAtt.error(str(ex))
  err = ErrorReturn('Error', ex,'Store Archival')

# COMMAND ----------

# MAGIC  %md 
# MAGIC  
# MAGIC  ##write processed store files to ADLS 

# COMMAND ----------

store_valid_Records=store_valid_Records.withColumn("STORE_CLOSE_DATE",col('STORE_CLOSE_DATE').cast(StringType()))
store_valid_Records=store_valid_Records.withColumn("STORE_OPEN_DATE",col('STORE_OPEN_DATE').cast(StringType()))
store_valid_Records=store_valid_Records.withColumn("LAST_UPDATE_DATE",col('LAST_UPDATE_DATE').cast(StringType()))
store_invalidRecords=store_invalidRecords.withColumn("STORE_CLOSE_DATE",col('STORE_CLOSE_DATE').cast(StringType()))
store_invalidRecords=store_invalidRecords.withColumn("STORE_OPEN_DATE",col('STORE_OPEN_DATE').cast(StringType()))
store_invalidRecords=store_invalidRecords.withColumn("LAST_UPDATE_DATE",col('LAST_UPDATE_DATE').cast(StringType()))

     

# COMMAND ----------

try:
  store_valid_Records.write.mode('Append').format('parquet').save(Store_OutboundPath + "/" +Date+ "/" + "Store_Maintainence_Data")
  loggerAtt.info('======== Valid Store Records write operation finished ========')
  if store_invalidRecords.count() > 0:
    store_invalidRecords.write.mode('Append').format('parquet').save(Invalid_RecordsPath + "/" +Date+ "/" + "Store_Invalid_Data")
    loggerAtt.info('======== Invalid Store Records write operation finished ========')
  
except Exception as ex:
  loggerAtt.error(str(ex))
  err = ErrorReturn('Error', ex,'StorefileWriteoperationfailed')
  loggerAtt.info('======== store file write to ADLS location failed ========')
  err.exit()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## writing log file to ADLS location

# COMMAND ----------

dbutils.fs.mv("file:"+p_logfile, Log_FilesPath+"/"+ custom_logfile_Name + file_date + '.log')
loggerAtt.info('======== Log file is updated at ADLS Location ========')
logging.shutdown()
err = ErrorReturn('Success', '','')
err.exit()
