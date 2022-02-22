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

custom_logfile_Name ='alternateUPC_dailymaintainence_customlog'
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
dbutils.widgets.text("POSemergencyFlag","")


FileName=dbutils.widgets.get("fileName")
Filepath=dbutils.widgets.get("filePath")
Directory=dbutils.widgets.get("directory")
container=dbutils.widgets.get("container")
PipelineID=dbutils.widgets.get("pipelineID")
mount_point=dbutils.widgets.get("MountPoint")
AlternateUPCDeltaPath=dbutils.widgets.get("deltaPath")
Log_FilesPath=dbutils.widgets.get("logFilesPath")
Invalid_RecordsPath=dbutils.widgets.get("invalidRecordsPath")
Date = datetime.datetime.now(timezone("America/Halifax")).strftime("%Y-%m-%d")
file_location = '/mnt' + '/' + Directory + '/' + Filepath +'/' + FileName 
source= 'abfss://' + Directory + '@' + container + '.dfs.core.windows.net/'
clientId=dbutils.widgets.get("clientId")
keyVaultName=dbutils.widgets.get("keyVaultName")
itemTempEffDeltaPath=dbutils.widgets.get("itemTempEffDeltaPath")
POSemergencyFlag=dbutils.widgets.get("POSemergencyFlag")

loggerAtt.info(f"Date : {Date}")
loggerAtt.info(f"File Location on Mount Point : {file_location}")
loggerAtt.info(f"Source or File Location on Container : {source}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Declarations

# COMMAND ----------

file_location = str(file_location)
file_type = "csv"
infer_schema = "false"
first_row_is_header = "false"
delimiter = "|"
alternateUPC_raw_df = None
alternateUPC_duplicate_records = None
headerFooterRecord = None
alternateUPCRecords = None
alternateUPC_valid_Records = None
PipelineID= str(PipelineID)
folderDate = Date

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definition

# COMMAND ----------

#Defining the schema for alternate UPC Table
loggerAtt.info('========Schema definition initiated ========')
alternateUPCRaw_schema = StructType([
  StructField("UPC",StringType(),False),
  StructField("ALT_UPC",LongType(),False)
  ])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Delta table creation

# COMMAND ----------

try:
    ABC(DeltaTableCreateCheck = 1)
    loggerAtt.info("AlternateUPC_delta table creation")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS AlternateUPC_delta (
    UPC LONG,
    ALT_UPC LONG,
    INSERT_ID STRING,
    INSERT_TIMESTAMP TIMESTAMP,
    LAST_UPDATE_ID STRING,
    LAST_UPDATE_TIMESTAMP TIMESTAMP
    )
    USING delta
    Location '{}'""".format(AlternateUPCDeltaPath))
except Exception as ex:
    ABC(DeltaTableCreateCheck = 0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'AlternateUPCDeltaPath deltaCreator')
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

# MAGIC %md
# MAGIC 
# MAGIC ## User Defined Function, Functions and Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Transformation

# COMMAND ----------

def alternateUPCTransformation(processed_df, JOB_ID):
    processed_df=processed_df.withColumn("UPC",col("UPC").cast(LongType()))
    processed_df=processed_df.withColumn("INSERT_TIMESTAMP",current_timestamp())
    processed_df=processed_df.withColumn("LAST_UPDATE_ID",lit(JOB_ID))
    processed_df=processed_df.withColumn("LAST_UPDATE_TIMESTAMP",current_timestamp())
    processed_df=processed_df.withColumn("INSERT_ID", lit(JOB_ID))
    return processed_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### File reading

# COMMAND ----------

def readFile(file_location, infer_schema, first_row_is_header, delimiter,file_type):
    raw_df = spark.read.format(file_type) \
    .option("mode","PERMISSIVE") \
    .option("header", first_row_is_header) \
    .option("dateFormat", "yyyyMMdd") \
    .option("sep", delimiter) \
    .schema(alternateUPCRaw_schema) \
    .load(file_location)

    ABC(ReadDataCheck=1)
    RawDataCount = raw_df.count()
    ABC(RawDataCount=RawDataCount)
    loggerAtt.info("Raw count check initiated")
    loggerAtt.info(f"Count of Records in the File: {RawDataCount}")
    return raw_df


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### UDF

# COMMAND ----------

def typeCheck(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False

typeCheckUDF = udf(typeCheck)

#Column renaming functions 
def alternateUPCflat_alternateUPCtable(s):
    return alternateUPC_Renaming[s]
def change_col_name(s):
    return s in alternateUPC_Renaming

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### ABC framework Function

# COMMAND ----------

def abcFramework(headerFooterRecord, alternateUPC_duplicate_records, alternateUPC_raw_df):
    loggerAtt.info("ABC Framework function initiated")
    try:
        ABC(BOFEOFCheck=1)
        # Fetching all alternateUPC id records with non integer value
        headerFooterRecord = alternateUPC_raw_df.filter(typeCheckUDF(col('UPC')) == False)
        display(headerFooterRecord)
        # Fetching EOF and BOF records and checking whether it is of length one each
        eofDf = headerFooterRecord.filter(alternateUPC_raw_df["UPC"].rlike("^:EOF"))
        bofDf = headerFooterRecord.filter(alternateUPC_raw_df["UPC"].rlike("^:BOF"))

        loggerAtt.info("EOF file record count is " + str(eofDf.count()))
        loggerAtt.info("BOF file record count is " + str(bofDf.count()))
        EOFcnt=eofDf.count()
        ABC(EOFCount=EOFcnt)
        # If there are multiple header/Footer then the file is invalid
        if ((eofDf.count() != 1) or (bofDf.count() != 1)):
            raise Exception('Error in EOF or BOF value')

        alternateUPCDf = alternateUPC_raw_df.filter(typeCheckUDF(col('UPC')) == True)

        fileRecordCount = int(eofDf.select('UPC').toPandas()['UPC'][0].split()[1])

        actualRecordCount = int(alternateUPCDf.count() + headerFooterRecord.count() - 2)

        loggerAtt.info("Record Count mentioned in file: " + str(fileRecordCount))
        loggerAtt.info("Actual no of record in file: " + str(actualRecordCount))

        # Checking to see if the count matched the actual record count
        if actualRecordCount != fileRecordCount:
            raise Exception('Record count mismatch. Actual count ' + str(actualRecordCount) + ', file record count ' + str(fileRecordCount))

    except Exception as ex:
        ABC(BOFEOFCheck=0)
        EOFcnt = Null
        ABC(EOFCount=0)
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'ABC Framework check Error')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
    try:
        # Exception handling of schema records
        alternateUPC_nullRows = alternateUPCDf.where(reduce(lambda x, y: x | y, (col(x).isNull() for x in alternateUPCDf.columns)))
        ABC(NullValueCheck=1)
        ABC(DropNACheck = 1)
        loggerAtt.info("Dimension of the Null records:("+str(alternateUPC_nullRows.count())+"," +str(len(alternateUPC_nullRows.columns))+")")
        ABC(NullValuCount = alternateUPC_nullRows.count())
        alternateUPC_raw_dfWithNoNull = alternateUPCDf.na.drop()

        loggerAtt.info("Dimension of the Not null records:("+str(alternateUPC_raw_dfWithNoNull.count())+"," +str(len(alternateUPC_raw_dfWithNoNull.columns))+")")

        # removing duplicate record 
        ABC(DuplicateValueCheck = 1)
        if (alternateUPC_raw_dfWithNoNull.groupBy('ALT_UPC').count().filter("count > 1").count()) > 0:
            loggerAtt.info("Duplicate records exists")
            alternateUPC_rawdf_withCount =alternateUPC_raw_dfWithNoNull.groupBy('ALT_UPC').count()
            alternateUPC_duplicate_records = alternateUPC_rawdf_withCount.filter("count > 1").drop('count')
            DuplicateValueCnt = alternateUPC_duplicate_records.count()
            ABC(DuplicateValueCount=DuplicateValueCnt)
            alternateUPCRecords= alternateUPC_rawdf_withCount.filter("count == 1").drop('count')
            loggerAtt.info("Duplicate record Exists. No of duplicate records are " + str(alternateUPC_duplicate_records.count()))
        else:
            loggerAtt.info("No duplicate records")
            ABC(DuplicateValueCount=0)
            alternateUPCRecords = alternateUPC_raw_dfWithNoNull

        loggerAtt.info("ABC Framework function ended")
        return headerFooterRecord, alternateUPC_nullRows, alternateUPC_duplicate_records, alternateUPCRecords
    except Exception as ex:
        ABC(NullValueCheck=0)
        ABC(DropNACheck = 0)
        ABC(NullValuCount = "")
        ABC(DuplicateValueCheck = 0)
        ABC(DuplicateValueCount='')
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'ABC Framework check Error')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SQL Table Functions

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Upsert Alternate UPC records

# COMMAND ----------

def upsertAlternateUPCRecords(alternateUPC_valid_Records):
    loggerAtt.info("Merge into Delta table initiated")
    try:
        temp_table_name = "AlternateUPCRecords"

        alternateUPC_valid_Records.createOrReplaceTempView(temp_table_name)
        initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(AlternateUPCDeltaPath))
        loggerAtt.info(f"Initial count of records in Delta Table: {initial_recs.head(1)}")
        initial_recs = initial_recs.head(1)

        ABC(DeltaTableInitCount=initial_recs[0][0])
        spark.sql('''MERGE INTO delta.`{}` as alternateUPC
        USING AlternateUPCRecords 
        ON alternateUPC.ALT_UPC=AlternateUPCRecords.ALT_UPC
        WHEN MATCHED Then 
                Update Set 
                alternateUPC.UPC=AlternateUPCRecords.UPC,
                alternateUPC.LAST_UPDATE_ID=AlternateUPCRecords.LAST_UPDATE_ID,
                alternateUPC.LAST_UPDATE_TIMESTAMP=AlternateUPCRecords.LAST_UPDATE_TIMESTAMP

        WHEN NOT MATCHED THEN INSERT * '''.format(AlternateUPCDeltaPath))
        appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(AlternateUPCDeltaPath))
        loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
        appended_recs = appended_recs.head(1)
        ABC(DeltaTableFinalCount=appended_recs[0][0])
        loggerAtt.info("Merge into Delta table successful")
        spark.catalog.dropTempView(temp_table_name)
    except Exception as ex:
        loggerAtt.info("Merge into Delta table failed and throwed error")
        loggerAtt.error(str(ex))
        ABC(DeltaTableInitCount='')
        ABC(DeltaTableFinalCount='')
        err = ErrorReturn('Error', ex,'MergeDeltaTable')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
    loggerAtt.info("Merge into Delta table successful")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write Invalid Records

# COMMAND ----------

def writeInvalidRecords(alternateUPC_invalidRecords, Invalid_RecordsPath):
    try:
        if alternateUPC_invalidRecords.count() > 0:
            alternateUPC_invalidRecords.write.mode('Append').format('parquet').save(Invalid_RecordsPath + "/" +Date+ "/" + "AlternateUPC_Invalid_Data")
            loggerAtt.info('======== Invalid alternateUPC Records write operation finished ========')

    except Exception as ex:
        ABC(InvalidRecordSaveCheck = 0)
        loggerAtt.error(str(ex))
        loggerAtt.info('======== Invalid record file write to ADLS location failed ========')
        err = ErrorReturn('Error', ex,'Writeoperationfailed for Invalid Record')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Fetching Unified Item records

# COMMAND ----------

def mergeAltUPCAndItemMainTemp(itemTempEffDeltaPath, AlternateUPCDeltaPath):
    loggerAtt.info("Alt UPC merging to Item Main Temp  initiated")
    AlternateUPCDelta = spark.read.format('delta').load(AlternateUPCDeltaPath)
    AlternateUPCDelta = AlternateUPCDelta.select([c for c in AlternateUPCDelta.columns if c not in {'INSERT_ID','INSERT_TIMESTAMP','LAST_UPDATE_ID','LAST_UPDATE_TIMESTAMP'}])
    itemTempEffDelta = spark.sql('''select * from delta.`{}`'''.format(itemTempEffDeltaPath))
    ABC(UnifiedRecordItemCount = itemTempEffDelta.count())
    itemTempEffDelta = itemTempEffDelta.join(AlternateUPCDelta, [AlternateUPCDelta.ALT_UPC == itemTempEffDelta.SMA_GTIN_NUM], how='left').select([col(xx) for xx in itemTempEffDelta.columns] + ['UPC'])
    itemTempEffDelta = itemTempEffDelta.withColumn("ALTERNATE_UPC", col("UPC"))
    itemTempEffDelta = itemTempEffDelta.withColumn("ALTERNATE_UPC", col("ALTERNATE_UPC").cast(LongType()))
    itemTempEffDelta = itemTempEffDelta.withColumn("ALT_UPC_FETCH", when((col("ALTERNATE_UPC").isNull()), col("SMA_GTIN_NUM")).otherwise(col("ALTERNATE_UPC")))
    itemTempEffDelta = itemTempEffDelta.drop("UPC")

    itemTempEffDelta.write.partitionBy('SMA_DEST_STORE').format('delta').mode('overwrite').save(itemTempEffDeltaPath)
    
    ABC(UnifiedRecordFeedCount = itemTempEffDelta.count())
    
    loggerAtt.info("UnifiedRecordFeedCount:" +str(itemTempEffDelta.count()))
    loggerAtt.info("UnifiedRecordItemCount:" +str(itemTempEffDelta.count()))
    loggerAtt.info("Alt UPC merging to Item Main Temp successful")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Main Function

# COMMAND ----------

if __name__ == "__main__":
    loggerAtt.info('======== Input alternateUPC file processing initiated ========')
    
    if POSemergencyFlag == 'Y':
        loggerAtt.info('Feed processing happening for POS Emergency feed')
        ## Step 7: Fetching records for Unified Item 
        try:
            ABC(UnifiedRecordFetch = 1)
            mergeAltUPCAndItemMainTemp(itemTempEffDeltaPath, AlternateUPCDeltaPath)
        except Exception as ex:
            ABC(UnifiedRecordFetch = 0)
            ABC(UnifiedRecordFeedCount = "")
            ABC(UnifiedRecordItemCount = "")
            loggerAtt.error(ex)
            err = ErrorReturn('Error', ex,'mergeAltUPCAndItemMainTemp')
            errJson = jsonpickle.encode(err)
            errJson = json.loads(errJson)
            dbutils.notebook.exit(Merge(ABCChecks,errJson))  
    else:
        loggerAtt.info('Feed processing happening for POS Daile/Weekly feed')
    
        ## Step 1: Reading input file
        try:
            alternateUPC_raw_df = readFile(file_location, infer_schema, first_row_is_header, delimiter, file_type)
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

        if alternateUPC_raw_df is not None:
            ## Step 2: Performing ABC framework check
            headerFooterRecord, alternateUPC_nullRows, alternateUPC_duplicate_records, alternateUPCRecords = abcFramework(headerFooterRecord, alternateUPC_duplicate_records, alternateUPC_raw_df)

            ## Step 3: Performing Transformation
            try:  
                ABC(RenamingCheck=1)
                if alternateUPCRecords is not None: 
                    ABC(TransformationCheck = 1)
                    alternateUPC_valid_Records = alternateUPCTransformation(alternateUPCRecords, PipelineID)
            except Exception as ex:
                err = ErrorReturn('Error', ex,'alternateUPCTransformation')
                ABC(TransformationCheck = 0)
                loggerAtt.error(ex)
                errJson = jsonpickle.encode(err)
                errJson = json.loads(errJson)
                dbutils.notebook.exit(Merge(ABCChecks,errJson))

            ## Step 4: Combining all duplicate records
            try:
              ## Combining Duplicate, null record
                ABC(InvalidRecordSaveCheck = 1)
                if alternateUPC_duplicate_records is not None:
                    invalidRecordsList = [headerFooterRecord, alternateUPC_duplicate_records, alternateUPC_nullRows]
                    alternateUPC_invalidRecords = reduce(DataFrame.unionAll, invalidRecordsList)
                else:
                    invalidRecordsList = [headerFooterRecord, alternateUPC_nullRows]
                    alternateUPC_invalidRecords = reduce(DataFrame.unionAll, invalidRecordsList)
                ABC(InvalidRecordCount = alternateUPC_invalidRecords.count())
            except Exception as ex:
                err = ErrorReturn('Error', ex,'Grouping Invalid record function')
                ABC(InvalidRecordSaveCheck = 0)
                ABC(InvalidRecordCount = "")
                loggerAtt.error(ex)
                errJson = jsonpickle.encode(err)
                errJson = json.loads(errJson)
                dbutils.notebook.exit(Merge(ABCChecks,errJson))   

            loggerAtt.info("No of valid records: " + str(alternateUPC_valid_Records.count()))
            loggerAtt.info("No of invalid records: " + str(alternateUPC_invalidRecords.count()))

            ## Step 5: Merging records into delta table
            if alternateUPC_valid_Records is not None:
                upsertAlternateUPCRecords(alternateUPC_valid_Records)

            ## Step 6: Writing Invalid records to ADLS location
            if alternateUPC_invalidRecords is not None:
                writeInvalidRecords(alternateUPC_invalidRecords, Invalid_RecordsPath)

            ## Step 7: Fetching records for Unified Item 
            try:
                ABC(UnifiedRecordFetch = 1)
                mergeAltUPCAndItemMainTemp(itemTempEffDeltaPath, AlternateUPCDeltaPath)
            except Exception as ex:
                ABC(UnifiedRecordFetch = 0)
                ABC(UnifiedRecordFeedCount = "")
                ABC(UnifiedRecordItemCount = "")
                loggerAtt.error(ex)
                err = ErrorReturn('Error', ex,'mergeAltUPCAndItemMainTemp')
                errJson = jsonpickle.encode(err)
                errJson = json.loads(errJson)
                dbutils.notebook.exit(Merge(ABCChecks,errJson))  
        else:
            loggerAtt.info("Error in input file reading")
    
loggerAtt.info('======== Input alternateUPC file processing ended ========')

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