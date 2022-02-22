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
from pyspark.sql.functions import regexp_extract
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

custom_logfile_Name ='ProductToStore_daily_customlog'
loggerAtt, p_logfile, file_date = logger(custom_logfile_Name, '/tmp/')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Error Class Defination 

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
# MAGIC ## Widgets from ADF

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("filePath","")
dbutils.widgets.text("directory","")
dbutils.widgets.text("fileName","")
dbutils.widgets.text("deltaPath","")
dbutils.widgets.text("logFilesPath","")
dbutils.widgets.text("invalidRecordsPath","")
dbutils.widgets.text("MountPoint","")
dbutils.widgets.text("Container","")
dbutils.widgets.text("clientId", "")
dbutils.widgets.text("keyVaultName", "")
dbutils.widgets.text("pipelineID", "")
dbutils.widgets.text("feedEffDeltaPath","")
dbutils.widgets.text("itemTempEffDeltaPath","")
dbutils.widgets.text("POSemergencyFlag","")


# Get & Assign the parameters from ADF



Filepath=dbutils.widgets.get("filePath")
Directory=dbutils.widgets.get("directory")
FileName=dbutils.widgets.get("fileName")
DeltaPath = dbutils.widgets.get("deltaPath")
Log_FilesPath = dbutils.widgets.get("logFilesPath") 
Invalid_RecordsPath = dbutils.widgets.get("invalidRecordsPath")
MountPoint = dbutils.widgets.get("MountPoint")
Container = dbutils.widgets.get("Container")
clientId = dbutils.widgets.get("clientId")
keyVaultName = dbutils.widgets.get("keyVaultName")
PipelineID = dbutils.widgets.get("pipelineID")
itemTempEffDeltaPath=dbutils.widgets.get("itemTempEffDeltaPath")
prodtoStoreEffDeltaPath=dbutils.widgets.get("feedEffDeltaPath")
POSemergencyFlag=dbutils.widgets.get("POSemergencyFlag")

# Preparing variables using above parameters
Date = datetime.datetime.now(timezone("America/Halifax")).strftime("%Y-%m-%d")
file_location = '/mnt' + '/' + Directory + '/' + Filepath +'/' + FileName 
source= 'abfss://' + Directory + '@' + Container + '.dfs.core.windows.net/'



# COMMAND ----------

loggerAtt.info(f"Date : {Date}")
loggerAtt.info(f"File Location on Mount Point : {file_location}")
loggerAtt.info(f"Source or File Location on Container : {source}")

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
# MAGIC ## Mount Script

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/Mount_Point_Creation

# COMMAND ----------

try:
  mounting(MountPoint, source, clientId, keyVaultName)
  ABC(MountCheck=1)
except Exception as ex:
  # send error message to ADF and send email notification
  ABC(MountCheck=0)
  loggerAtt.error(str(ex))
  err = ErrorReturn('Error', ex,'Mounting')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))
  #err.exit()

# COMMAND ----------

def readFile(file_location, infer_schema, first_row_is_header, delimiter,file_type,schema):
  global raw_df
  raw_df = spark.read.format(file_type) \
    .option("mode","PERMISSIVE") \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .schema(schema) \
    .load(file_location)
  return raw_df


# COMMAND ----------

date_func =  udf (lambda x: datetime.datetime.strptime(str(x), '%Y-%m-%d').date(), DateType())

#Column renaming functions 
def Producttostoreflat_storetable(s):
    return ProducttoStore_Renaming[s]
def change_col_name(s):
    return s in ProducttoStore_Renaming

# COMMAND ----------

#Defining the Raw schema for ProductToStore Table
ProductToStoreRaw_schema = StructType([
                           StructField("PRD2STORE_BANNER_ID",StringType(),False),
                           StructField("PRD2STORE_UPC",StringType(),False),
                           StructField("PRD2STORE_STORE_ID",StringType(),False),
                           StructField("PRD2STORE_PROMO_CODE",StringType(),False),
                           StructField("PRD2STORE_SPLN_AD_CD",StringType(),False),
                           StructField("PRD2STORE_TAG_TYP_CD",StringType(),False),
                           StructField("PRD2STORE_WINE_VALUE_FLAG",StringType(),False),
                           StructField("PRD2STORE_BUY_QTY",StringType(),False),
                           StructField("PRD2STORE_LMT_QTY",StringType(),False),
                           StructField("PRD2STORE_SALE_STRT_DT",StringType(),True),
                           StructField("PRD2STORE_SALE_END_DT",StringType(),False),
                           StructField("PRD2STORE_AGE_FLAG",StringType(),False),
                           StructField("PRD2STORE_AGE_CODE",StringType(),False),
                           StructField("PRD2STORE_SWAP_SAVE_UPC",StringType(),True)
  ])
          

# COMMAND ----------

# MAGIC %md 
# MAGIC ## DATA TRNAFORMATIONS WITH CAST 

# COMMAND ----------

'''file_location = str(file_location)
file_type = "csv"
infer_schema = "false"
first_row_is_header = "false"
delimiter = "|"
header = "False"
folderDate = Date
rawdf=readFile(file_location, infer_schema, header, delimiter, file_type, schema = ProductToStoreRaw_schema)
readFile(file_location = "/mnt/delhaize-centralized-price-promo/Producttostore/Inbound/RDS/2021/03/18/SinkADF/prism_product_to_store.dat",infer_schema=True, first_row_is_header=False, delimiter="|", file_type="com.databricks.spark.csv")
rawdf.show()'''

# COMMAND ----------

def BOFEOF(df):
  global BOF_df
  global EOF_df
  global BOFDate
  global EOFCount
  global raw_df
  
  BOF_df = df.where(col('PRD2STORE_BANNER_ID').like("%BOF%")).cache()
  EOF_df = df.where(col('PRD2STORE_BANNER_ID').like("%EOF%")).cache()
  BOF_df = BOF_df.select("PRD2STORE_BANNER_ID").withColumn("BOF-DATE", regexp_extract("PRD2STORE_BANNER_ID","\\d+", 0)).cache()
  BOFDate = BOF_df.select("BOF-DATE").head()[0]
  EOF_df = EOF_df.select("PRD2STORE_BANNER_ID").withColumn("EOF-COUNT", regexp_extract("PRD2STORE_BANNER_ID","\\d+", 0)).cache()
  display(EOF_df)
  display(BOF_df)
  EOFCount = EOF_df.select("EOF-COUNT").head()[0]
  EOFCount = EOF_df.count()

  if ((EOF_df.count() != 1) or (BOF_df.count() != 1)):
    raise Exception('Error in EOF or BOF value')
  if ((int(EOFCount)) != (raw_df.count()-2)):
    #raise Exception('Error or Mismatch in count of records')
     raw_df = df.filter(col('PRD2STORE_UPC').isNotNull())
     

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Delta table creation

# COMMAND ----------

def deltaCreator(deltapath):
    spark.sql("""
    CREATE TABLE IF NOT EXISTS ProductToStoreDelta (
      PRD2STORE_BANNER_ID STRING,
      PRD2STORE_UPC LONG,
      PRD2STORE_STORE_ID STRING,
      PRD2STORE_PROMO_CODE STRING,
      PRD2STORE_SPLN_AD_CD STRING,
      PRD2STORE_TAG_TYP_CD STRING,
      PRD2STORE_WINE_VALUE_FLAG STRING,
      PRD2STORE_BUY_QTY STRING,
      PRD2STORE_LMT_QTY STRING,
      PRD2STORE_SALE_STRT_DT STRING,
      PRD2STORE_SALE_END_DT STRING,
      PRD2STORE_AGE_FLAG STRING,
      PRD2STORE_AGE_CODE STRING,
      PRD2STORE_SWAP_SAVE_UPC STRING,
      INSERT_ID STRING,
      INSERT_TIMESTAMP TIMESTAMP,
      LAST_UPDATE_ID STRING,
      LAST_UPDATE_TIMESTAMP TIMESTAMP
    )
    USING delta
    Location '{}'
    PARTITIONED BY (PRD2STORE_STORE_ID)
    """.format(deltapath))

# COMMAND ----------

try:
    ABC(DeltaTableCreateCheck = 1)
    loggerAtt.info("Item Main Temp creation")
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
    err = ErrorReturn('Error', ex,'itemTempEffDeltaPath deltaCreator')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

try:
    ABC(DeltaTableCreateCheck = 1)
    loggerAtt.info("prodtoStoreTemp creation")
    spark.sql(""" CREATE TABLE IF NOT EXISTS prodToStoreTemp(
                PRD2STORE_BANNER_ID STRING,
                PRD2STORE_UPC Long,
                PRD2STORE_STORE_ID STRING,
                PRD2STORE_PROMO_CODE STRING,
                PRD2STORE_SPLN_AD_CD STRING,
                PRD2STORE_TAG_TYP_CD STRING,
                PRD2STORE_WINE_VALUE_FLAG STRING,
                PRD2STORE_BUY_QTY STRING,
                PRD2STORE_LMT_QTY STRING,
                PRD2STORE_SALE_STRT_DT STRING,
                PRD2STORE_SALE_END_DT STRING,
                PRD2STORE_AGE_FLAG STRING,
                PRD2STORE_AGE_CODE STRING,
                PRD2STORE_SWAP_SAVE_UPC STRING)
              USING delta 
              PARTITIONED BY (PRD2STORE_STORE_ID)
              LOCATION '{}' """.format(prodtoStoreEffDeltaPath))
except Exception as ex:
    ABC(DeltaTableCreateCheck = 0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'prodtoStoreEffDeltaPath deltaCreator')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

def producttostoreTransformation(processed_df, pipelineid):
  global raw_df
  #processed_df=processed_df.withColumn('PRD2STORE_STORE_ID', rpad(col('PRD2STORE_STORE_ID'),4,'0'))
  #processed_df = processed_df.withColumn('PRICE-Location', format_string("%010d", col('PRICE-Location').cast('int')))
  processed_df = processed_df.withColumn('PRD2STORE_STORE_ID', format_string("%04d", col('PRD2STORE_STORE_ID').cast('int')))
  #processed_df = processed_df.withcolumn('PRD2STORE_UPC',col('PRD2STORE_UPC').cast('Long'))
#df.withColumn("salary",col("salary").cast("Integer"))
  processed_df=processed_df.withColumn("INSERT_ID",lit(pipelineid))
  processed_df=processed_df.withColumn("INSERT_TIMESTAMP",current_timestamp())
  processed_df=processed_df.withColumn("LAST_UPDATE_ID",lit(pipelineid))
  processed_df=processed_df.withColumn("LAST_UPDATE_TIMESTAMP",current_timestamp())
  raw_df = processed_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Unified Item File Transformations

# COMMAND ----------

def itemMasterTransformation(processed_df, pipelineid):
    global raw_df
    #processed_df=processed_df.withColumn('PRD2STORE_STORE_ID', rpad(col('PRD2STORE_STORE_ID'),4,'0'))
    #processed_df = processed_df.withColumn('PRICE-Location', format_string("%010d", col('PRICE-Location').cast('int')))
    processed_df = processed_df.withColumn('PRD2STORE_STORE_ID', format_string("%04d", col('PRD2STORE_STORE_ID').cast('int')))
    processed_df = processed_df.withColumn('PRD2STORE_BANNER_ID', regexp_replace(col("PRD2STORE_BANNER_ID"), " ", ""))
    processed_df=processed_df.withcolumn("PRD2STORE_UPC",col("PRD2STORE_UPC").cast("Long"))
    processed_df=processed_df.withColumn("INSERT_ID",lit(pipelineid))
    processed_df=processed_df.withColumn("INSERT_TIMESTAMP",current_timestamp())
    processed_df=processed_df.withColumn("LAST_UPDATE_ID",lit(pipelineid))
    processed_df=processed_df.withColumn("LAST_UPDATE_TIMESTAMP",current_timestamp())
    raw_df = processed_df

# COMMAND ----------

def mergeData(df, DeltaFilepath):
  global initial_recs
  global appended_recs
  loggerAtt.info("Merge into Delta table initiated")
  #if producttostore_valid_Records is not None:

  if df is not None:
    temp_table_name = "producttostoreRecords"
    df.createOrReplaceTempView(temp_table_name)
    try:
      #spark.sql("USE {}".format(databaseName))
      initial_recs = spark.sql("""SELECT count(*) as count from ProductToStoreDelta;""")
      print(f"Initial count of records in Delta Table: {initial_recs.head(1)}")
      initial_recs = initial_recs.head(1)

      spark.sql('''MERGE INTO delta.`{}` as producttostore
      USING producttostoreRecords 
      ON producttostore.PRD2STORE_STORE_ID = producttostoreRecords.PRD2STORE_STORE_ID
      AND producttostore.PRD2STORE_UPC=producttostoreRecords.PRD2STORE_UPC
      WHEN MATCHED Then 
              Update Set producttostore.PRD2STORE_BANNER_ID=producttostoreRecords.PRD2STORE_BANNER_ID,            
              producttostore.PRD2STORE_PROMO_CODE=producttostoreRecords.PRD2STORE_PROMO_CODE,
              producttostore.PRD2STORE_SPLN_AD_CD=producttostoreRecords.PRD2STORE_SPLN_AD_CD,
              producttostore.PRD2STORE_TAG_TYP_CD=producttostoreRecords.PRD2STORE_TAG_TYP_CD,
              producttostore.PRD2STORE_WINE_VALUE_FLAG=producttostoreRecords.PRD2STORE_WINE_VALUE_FLAG,
              producttostore.PRD2STORE_BUY_QTY=producttostoreRecords.PRD2STORE_BUY_QTY,
              producttostore.PRD2STORE_LMT_QTY=producttostoreRecords.PRD2STORE_LMT_QTY,
              producttostore.PRD2STORE_SALE_STRT_DT=producttostoreRecords.PRD2STORE_SALE_STRT_DT,
              producttostore.PRD2STORE_SALE_END_DT=producttostoreRecords.PRD2STORE_SALE_END_DT,
              producttostore.PRD2STORE_AGE_FLAG=producttostoreRecords.PRD2STORE_AGE_FLAG,
              producttostore.PRD2STORE_AGE_CODE=producttostoreRecords.PRD2STORE_AGE_CODE,
              producttostore.PRD2STORE_SWAP_SAVE_UPC=producttostoreRecords.PRD2STORE_SWAP_SAVE_UPC,
              producttostore.LAST_UPDATE_ID=producttostoreRecords.LAST_UPDATE_ID,
              producttostore.LAST_UPDATE_TIMESTAMP=producttostoreRecords.LAST_UPDATE_TIMESTAMP
              WHEN NOT MATCHED THEN INSERT * '''.format(DeltaFilepath))

      appended_recs = spark.sql("""SELECT count(*) as count from ProductToStoreDelta""")
      print(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
      appended_recs = appended_recs.head(1)
 
      
    except Exception as ex:
      loggerAtt.info("Merge into Delta table failed and throwed error")
      loggerAtt.error(str(ex))
      err = ErrorReturn('Error', ex,'MergeDeltaTable')
      err.exit()
  loggerAtt.info("Merge into Delta table successful") 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Fetching Unified Item records

# COMMAND ----------

def itemMasterRecordsModified(itemTempEffDeltaPath, prodtoStoreEffDeltaPath, Date, deltapath):
    loggerAtt.info("itemMasterRecords fetch initiated")
    
    unifiedProdToStoreFields = ['SMA_GTIN_NUM', 'PRD2STORE_BANNER_ID', 'SMA_STORE', 'PRD2STORE_PROMO_CODE', 'PRD2STORE_SPLN_AD_CD', 'PRD2STORE_TAG_TYP_CD', 'PRD2STORE_WINE_VALUE_FLAG', 'PRD2STORE_BUY_QTY', 'PRD2STORE_LMT_QTY', 'PRD2STORE_SALE_STRT_DT', 'PRD2STORE_SALE_END_DT', 'PRD2STORE_AGE_FLAG', 'PRD2STORE_AGE_CODE', 'PRD2STORE_SWAP_SAVE_UPC']
    
    ProductToStoreDelta= spark.read.format('delta').load(deltapath)
    
    itemTempEffDelta = spark.sql('''select SMA_STORE,BANNER_ID,SMA_GTIN_NUM from delta.`{}`'''.format(itemTempEffDeltaPath))
    
    
    
    ProductToStoreDelta = itemTempEffDelta.join(ProductToStoreDelta, [itemTempEffDelta.SMA_GTIN_NUM == ProductToStoreDelta.PRD2STORE_UPC, itemTempEffDelta.SMA_STORE == ProductToStoreDelta.PRD2STORE_STORE_ID], how='left').select([col(xx) for xx in unifiedProdToStoreFields])
    
    ProductToStoreDelta = ProductToStoreDelta.withColumnRenamed('SMA_GTIN_NUM', 'PRD2STORE_UPC')
    ProductToStoreDelta = ProductToStoreDelta.withColumnRenamed('SMA_STORE', 'PRD2STORE_STORE_ID')
    
    prodtoStoreTemp = spark.sql('''DELETE FROM delta.`{}`'''.format(prodtoStoreEffDeltaPath))
    ProductToStoreDelta.write.partitionBy('PRD2STORE_STORE_ID').format('delta').mode('append').save(prodtoStoreEffDeltaPath)
    
    ABC(UnifiedRecordFeedCount = ProductToStoreDelta.count())
    ABC(UnifiedRecordItemCount = itemTempEffDelta.count())
    loggerAtt.info("UnifiedRecordFeedCount:" +str(ProductToStoreDelta.count()))
    loggerAtt.info("UnifiedRecordItemCount:" +str(itemTempEffDelta.count()))
    loggerAtt.info("itemMasterRecords fetch successful")

# COMMAND ----------

if __name__ == "__main__":
  
  ## File reading parameters
    loggerAtt.info('======== Input Product file processing initiated ========')
    file_location = str(file_location)
    file_type = "csv"
    infer_schema = "false"
    first_row_is_header = "false"
    delimiter = "|"
    header = "False"
    folderDate = Date

    if POSemergencyFlag == 'Y':
        try:
            ABC(itemMasterRecordsModifiedCheck = 1)
            itemMasterRecordsModified(itemTempEffDeltaPath, prodtoStoreEffDeltaPath, Date, DeltaPath)
        except Exception as ex:
            ABC(itemMasterRecordsModifiedCheck=0)
            ABC(UnifiedRecordFeedCount='')
            ABC(UnifiedRecordItemCount='')
            loggerAtt.error(ex)
            err = ErrorReturn('Error', ex,'itemMasterRecordsModified')
            errJson = jsonpickle.encode(err)
            errJson = json.loads(errJson)
            dbutils.notebook.exit(Merge(ABCChecks,errJson))
    else:
        try:
            readFile(file_location, infer_schema, header, delimiter, file_type, schema = ProductToStoreRaw_schema)
            ABC(ReadDataCheck=1)
            RawDataCount=raw_df.count()
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

        try:
            BOFEOF(raw_df)
            ABC(BOFEOFCheck=1)
            EOFcnt = EOFCount
            BOFDt = BOFDate
            ABC(EOFCount=EOFcnt)
            ABC(BOFDate=BOFDt)        
            loggerAtt.info("BOF & EOF Processing completed")
            BOF_df.unpersist()
            loggerAtt.info("BOF Dataframe unpersisting")
            EOF_df.unpersist()
            loggerAtt.info("EOF Dataframe unpersisting")
        except Exception as ex:
            ABC(BOFEOFCheck=0)
            EOFcnt = Null
            BOFDt = Null
            ABC(EOFCount=EOFcnt)
            ABC(BOFDate=BOFDt) 
            loggerAtt.error(ex)
            err = ErrorReturn('Error', ex,'BOFEOFCheck')
            errJson = jsonpickle.encode(err)
            errJson = json.loads(errJson)
            dbutils.notebook.exit(Merge(ABCChecks,errJson))



        try:
            producttostore_nullRows = raw_df.where(reduce(lambda x, y: x | y, (col(x).isNull() for x in raw_df.columns)))
            ABC(NullValueCheck=1)
            raw_df = raw_df.na.drop()
            ABC(DropNACheck = 1)
            NullValuCnt = producttostore_nullRows.count()
            ABC(NullValuCount = NullValuCnt)
        except Exception as ex:
            NullValueCheck = 0
            NullValuCnt=''
            ABC(NullValueCheck=0)
            ABC(NullValuCount = NullValuCnt)  
            loggerAtt.error(ex)
            err = ErrorReturn('Error', ex,'NullRecordHandling')
            errJson = jsonpickle.encode(err)
            errJson = json.loads(errJson)
            dbutils.notebook.exit(Merge(ABCChecks,errJson))

        try:
            producttostoreTransformation(raw_df, pipelineid=str(PipelineID))
            ABC(TransformationCheck=1)
        except Exception as ex:
            ABC(TransformationCheck = 0)
            loggerAtt.error(ex)
            err = ErrorReturn('Error', ex,'priceTransformation')
            errJson = jsonpickle.encode(err)
            errJson = json.loads(errJson)
            dbutils.notebook.exit(Merge(ABCChecks,errJson))
            #err.exit()

        try:
            # removing duplicate record 
            ABC(DuplicateValueCheck = 1)
            if (raw_df.groupBy(raw_df.columns).count().filter("count > 1").count()) > 0:
                loggerAtt.info("Duplicate records exists")
                producttostore_rawdf_withCount = raw_df.groupBy(raw_df.columns).count();
                producttostore_duplicate_records = producttostore_rawdf_withCount.filter("count > 1").drop('count')
                DuplicateValueCnt = producttostore_duplicate_records.count()
                ABC(DuplicateValueCount=DuplicateValueCnt)
                producttostoreRecords= producttostore_rawdf_withCount.drop('count')
            else:
                producttostore_duplicate_records = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema=ProductToStoreRaw_schema)
                ABC(DuplicateValueCount=0)
                loggerAtt.info(f"No Duplicate RECORDS")
                producttostoreRecords = raw_df
        except Exception as ex:
            pass

        try:

            deltaCreator(str(DeltaPath))
            ABC(DeltaTableCreateCheck=1)
            mergeData(raw_df, str(DeltaPath)) 
            ABC(DeltaTableInitCount=initial_recs[0][0])
            ABC(DeltaTableFinalCount=appended_recs[0][0])
        except Exception as ex:
            ABC(DeltaTableCreateCheck = 0)
            ABC(DeltaTableInitCount='')
            ABC(DeltaTableFinalCount='')
            loggerAtt.error(ex)
            err = ErrorReturn('Error', ex,'DELTAcreationMerge')
            errJson = jsonpickle.encode(err)
            errJson = json.loads(errJson)
            dbutils.notebook.exit(Merge(ABCChecks,errJson))


        try:
            producttostore_invalidRecords = producttostore_duplicate_records.union(producttostore_nullRows)
            producttostore_invalidRecords.repartition(1).write.mode('overwrite').parquet(Invalid_RecordsPath)
            ABC(InvalidRecordSaveCheck = 1)
            ABC(InvalidRecordCount = producttostore_invalidRecords.count())
            loggerAtt.info(f"Invalid Records saved at path: {Invalid_RecordsPath}")
        except Exception as ex:
            ABC(InvalidRecordSaveCheck = 0)
            ABC(InvalidRecordCount='')
            loggerAtt.error(ex)
            err = ErrorReturn('Error', ex,'InvalidRecordsSave')
            errJson = jsonpickle.encode(err)
            errJson = json.loads(errJson)
            dbutils.notebook.exit(Merge(ABCChecks,errJson))
        try:
            ABC(itemMasterRecordsModifiedCheck = 1)
            itemMasterRecordsModified(itemTempEffDeltaPath, prodtoStoreEffDeltaPath, Date, DeltaPath)
        except Exception as ex:
            ABC(itemMasterRecordsModifiedCheck=0)
            ABC(UnifiedRecordFeedCount='')
            ABC(UnifiedRecordItemCount='')
            loggerAtt.error(ex)
            err = ErrorReturn('Error', ex,'itemMasterRecordsModified')
            errJson = jsonpickle.encode(err)
            errJson = json.loads(errJson)
            dbutils.notebook.exit(Merge(ABCChecks,errJson))
loggerAtt.info('======== Input product file processing ended ========')

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

updateDeltaVersioning('Delhaize', 'producttostore', PipelineID, Filepath, FileName)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Writing log file to ADLS location

# COMMAND ----------

dbutils.fs.mv("file:"+p_logfile, Log_FilesPath+"/"+ custom_logfile_Name + file_date + '.log')
loggerAtt.info('======== Log file is updated at ADLS Location ========')
logging.shutdown()
err = ErrorReturn('Success', '','')
errJson = jsonpickle.encode(err)
errJson = json.loads(errJson)
Merge(ABCChecks,errJson)
dbutils.notebook.exit(Merge(ABCChecks,errJson))
#err.exit()