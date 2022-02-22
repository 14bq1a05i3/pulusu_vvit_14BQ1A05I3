# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # DELHAIZE PRICE FEED

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### LOADING LIBRARIES

# COMMAND ----------

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
# MAGIC #### CALLING LOGGER

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/Logging

# COMMAND ----------

custom_logfile_Name ='productPrice_daily_customlog'
loggerAtt, p_logfile, file_date = logger(custom_logfile_Name, '/tmp/')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### ERROR CLASS DEFINTION

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
# MAGIC #### ABC FRAMEWORK

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
# MAGIC #### WIDGETS

# COMMAND ----------

loggerAtt.info("========Widgets call initiated==========")
#dbutils.widgets.removeAll()

# Used for collecting or Metadata at Pipeline "delhaizeProductPipeline" (inside Get Metadata)
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
dbutils.widgets.text("itemTempEffDeltaPath","")
#dbutils.widgets.text("priceEffDeltaPath","") /mnt/delhaize-centralized-price-promo/Productprice/Outbound/SDM/PriceDeltaTemp
dbutils.widgets.text("feedEffDeltaPath","")
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
priceEffDeltaPath=dbutils.widgets.get("feedEffDeltaPath")
POSemergencyFlag=dbutils.widgets.get("POSemergencyFlag")

# Preparing variables using above parameters
Date = datetime.datetime.now(timezone("America/Halifax")).strftime("%Y-%m-%d")
file_location = '/mnt' + '/' + Directory + '/' + Filepath +'/' + FileName 
source= 'abfss://' + Directory + '@' + Container + '.dfs.core.windows.net/'

loggerAtt.info(f"Date : {Date}")
loggerAtt.info(f"File Location on Mount Point : {file_location}")
loggerAtt.info(f"Source or File Location on Container : {source}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### MOUNTING ADLS LOCATION

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/Mount_Point_Creation

# COMMAND ----------

try: 
  mounting(MountPoint, source, clientId, keyVaultName) #abfss://delhaize-centralized-price-promo@rs08ue2qmasadata01.dfs.core.windows.net/, #0e103665-60d5-420a-b1e1-28f049e86c0c, #Merchandising-Access-Storage-Account-data02-key1
  ABC(MountCheck=1)
except Exception as ex:
  # send error message to ADF and send email notification
  ABC(MountCheck=0)
  loggerAtt.error(str(ex))
  err = ErrorReturn('Error', ex,'Mounting')
  errJson = jsonpickle.encode(err)
  errJson = json.loads(errJson)
  dbutils.notebook.exit(Merge(ABCChecks,errJson))
  #err.exit

# COMMAND ----------

# MAGIC %md
# MAGIC # PRICE WORK

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 1.SCHEMA DEFINING

# COMMAND ----------

#Defining the schema for Delhaize Product Price Table
loggerAtt.info('========Schema definition initiated ========')
priceRaw_schema = StructType([
                           StructField("PRICE-BANNER-ID",StringType(),False),                    # MISSING IN UNIFIED ITEM FILE
                           StructField("PRICE-Location",StringType(),False),                     # MISSING IN UNIFIED ITEM FILE
                           StructField("PRICE-UPC",LongType(),False),                          # MISSING IN UNIFIED ITEM FILE
                           StructField("PRICE-RTL-PRC",StringType(),False),                      # MISSING IN UNIFIED ITEM FILE
                           StructField("PRICE-UNIT-PRICE",StringType(),True),            #Unified Item Field Name: SAME Feed: VARCHAR, length = 10
                           StructField("PRICE-UOM-CD",StringType(),True),                #Unified Item Field Name: SAME Feed: VARCHAR, length = 25
                           StructField("PRICE-MLT-Quantity",StringType(),True),          #Unified Item Field Name: SAME Feed: VARCHAR, length = 10
                           StructField("PRICE-PROMO-RTL-PRC",StringType(),False),         #Unified Item Field Name: SAME Feed: VARCHAR, length = 10
                           StructField("PRICE-PROMO-UNIT-PRICE",StringType(),True),      #Unified Item Field Name: SAME Feed: VARCHAR, length = 10
                           StructField("PRICE-Promotional-Quantity",StringType(),False),   #Unified Item Field Name: SAME Feed: VARCHAR, length = 10
                           StructField("PRICE-ACTIVE-FLG",StringType(),False),                   # MISSING IN UNIFIED ITEM FILE
                           StructField("PRICE-START-DATE",StringType(),False),            #Unified Item Field Name: SAME Feed: VARCHAR, length = 20
                           StructField("PRICE-END-DATE",StringType(),False),              #Unified Item Field Name: SAME Feed: VARCHAR, length = 20
                           StructField("TPRX001-STORE-SID-NBR",StringType(),False),       #Unified Item Field Name: SAME Feed: VARCHAR, length = 4
                           StructField("TPRX001-ITEM-NBR",StringType(),False),           #Unified Item Field Name: SAME Feed: VARCHAR, length = 14
                           StructField("TPRX001-DC-ID",StringType(),True),                       # MISSING IN UNIFIED ITEM FILE
                           StructField("TPRX001-ITEM-SRC-CD",StringType(),False),         #Unified Item Field Name: SAME Feed: VARCHAR, length = 1
                           StructField("TPRX001-CPN-SRC-CD",StringType(),True),          #Unified Item Field Name: SAME Feed: VARCHAR, length = 1
                           StructField("TPRX001-ECPN-NBR",StringType(),False),                   # MISSING IN UNIFIED ITEM FILE
                           StructField("TPRX001-RTL-PRC-EFF-DT",StringType(),False),      #Unified Item Field Name: SAME Feed: VARCHAR, length = 10
                           StructField("TPRX001-ITEM-PROMO-FLG",StringType(),True),      #Unified Item Field Name: SAME Feed: VARCHAR, length = 1
                           StructField("TPRX001-PROMO-TYP-CD",StringType(),True),         #Unified Item Field Name: SAME Feed: VARCHAR, length = 2
                           StructField("TPRX001-AD-TYP-CD",StringType(),True),           #Unified Item Field Name: SAME Feed: VARCHAR, length = 2
                           StructField("TPRX001-PROMO-DSC",StringType(),True),                  # MISSING IN UNIFIED ITEM FILE
                           StructField("TPRX001-MIX-MTCH-FLG",StringType(),True),        #Unified Item Field Name: SAME Feed: VARCHAR, length = 20
                           StructField("TPRX001-PRC-STRAT-CD",StringType(),True),        #Unified Item Field Name: SAME Feed: VARCHAR, length = 1
                           StructField("TPRX001-LOYAL-CRD-FLG",StringType(),True),       #Unified Item Field Name: SAME Feed: VARCHAR, length = 1
                           StructField("TPRX001-SCAN-AUTH-FLG",StringType(),False),       #Unified Item Field Name: SAME Feed: VARCHAR, length = 1
                           StructField("TPRX001-MDSE-AUTH-FLG",StringType(),False),       #Unified Item Field Name: SAME Feed: VARCHAR, length = 1
                           StructField("TPRX001-SBT-FLG",StringType(),False),             #Unified Item Field Name: SAME Feed: VARCHAR, length = 1
                           StructField("TPRX001-SBT-VEND-ID",StringType(),True),          #Unified Item Field Name: SAME Feed: VARCHAR, length = 9
                           StructField("TPRX001-SBT-VEND-NET-CST",StringType(),False),            # MISSING IN UNIFIED ITEM FILE
                           StructField("TPRX001-SCAN-DAUTH-DT",StringType(),False),       #Unified Item Field Name: SAME Feed: VARCHAR, length = 10
                           StructField("TPRX001-SCAN-PRVWK-RTL-MLT",StringType(),False),  #Unified Item Field Name: SAME Feed: VARCHAR, length = 4
                           StructField("TPRX001-SCAN-PRVWK-RTL-PRC",StringType(),False),  #Unified Item Field Name: SAME Feed: VARCHAR, length = 10
                           StructField("TPRX001-SCANPRVDAY-RTL-MLT",StringType(),False),  #Unified Item Field Name: SAME Feed: VARCHAR, length = 4
                           StructField("TPRX001-SCANPRVDAY-RTL-PRC",StringType(),False),  #Unified Item Field Name: SAME Feed: VARCHAR, length = 10
                           StructField("TPRX001-TAG-PRV-WK-RTL-MLT",StringType(),False),  #Unified Item Field Name: SAME Feed: VARCHAR, length = 4
                           StructField("TPRX001-TAG-PRV-WK-RTL-PRC",StringType(),False)   #Unified Item Field Name: SAME Feed: VARCHAR, length = 10
])



# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ##### 2.READ FOLDER

# COMMAND ----------

def readFile(file_location, infer_schema, header, delimiter,file_type, schema):
  global raw_df
  raw_df = spark.read.format(file_type) \
  .option("mode","PERMISSIVE") \
  .option("header", header) \
  .option("sep", delimiter) \
  .schema(schema) \
  .load(file_location)
#  print(f"Columns in the DataFrame: {raw_df.columns}")
#  print(f"Count of records Read : {raw_df.count()}")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select TPRX001_ITEM_NBR from dapricedelta where PRICE_UPC = 180

# COMMAND ----------

def BOFEOF(df):
  global BOF_df
  global EOF_df
  global BOFDate
  global EOFCount
  global raw_df
  BOF_df = df.where(col('PRICE-BANNER-ID').like("%BOF%"))
  EOF_df = df.where(col('PRICE-BANNER-ID').like("%EOF%"))
  BOF_df = BOF_df.select("PRICE-BANNER-ID").withColumn("BOF-DATE", regexp_extract("PRICE-BANNER-ID","\\d+", 0)).cache()
  BOFDate = BOF_df.select("BOF-DATE").head()[0]
  EOF_df = EOF_df.select("PRICE-BANNER-ID").withColumn("EOF-COUNT", regexp_extract("PRICE-BANNER-ID","\\d+", 0)).cache()
  EOFCount = EOF_df.select("EOF-COUNT").head()[0]  
  if ((EOF_df.count() != 1) or (BOF_df.count() != 1)):
    raise Exception('Error in EOF or BOF value')
  if ((int(EOFCount)) != (raw_df.count()-2)):
    raise Exception('Error or Mismatch in count of records')


# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ##### 3.PRODUCT PRICE TRANSFORMATIONS

# COMMAND ----------

def priceTransformation(df, pipelineid):
  global raw_df
  global invalidRecords
  global TransformationCheck
  
  # NULL CHECKING
  
  #1. Setting DF with empth schema
  invalidRecords = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema=priceRaw_schema)
  
  # PADDING
  
  df = df.withColumn('PRICE-Location', lpad(df['PRICE-Location'],10, ' '))
  
  # New Derived Column
  df = df.withColumn("PRICE-STORE-ID", df['PRICE-Location'].substr(-4,4)) # New derived column PRICE-STORE-ID
  df=df.withColumn('PRICE-BANNER-ID', regexp_replace(col("PRICE-BANNER-ID"), " ", ""))
  
  #CASTING
  #processed_df = processed_df.withColumn('PRICE-UPC', processed_df['PRICE-UPC'].cast('int')))
  df = df.withColumn('PRICE-RTL-PRC', df['PRICE-RTL-PRC'].cast(FloatType()))
  df = df.withColumn('PRICE-UNIT-PRICE', df['PRICE-UNIT-PRICE'].cast(FloatType()))
  df = df.withColumn('PRICE-PROMO-RTL-PRC', df['PRICE-PROMO-RTL-PRC'].cast(FloatType())) 
  df = df.withColumn('PRICE-PROMO-UNIT-PRICE', df['PRICE-PROMO-UNIT-PRICE'].cast(FloatType())) 
  
  df = df.withColumn('PRICE-Promotional-Quantity', df['PRICE-Promotional-Quantity'].cast(IntegerType()))
  df = df.withColumn('PRICE-Promotional-Quantity', lpad(col('PRICE-Promotional-Quantity'),10,'0')) ##
  
  df = df.withColumn('PRICE-MLT-Quantity', df['PRICE-MLT-Quantity'].cast(IntegerType()))
  df = df.withColumn('PRICE-MLT-Quantity', lpad(col('PRICE-MLT-Quantity'),10,'0')) ##
  
  df = df.withColumn('TPRX001-SBT-VEND-NET-CST', df['TPRX001-SBT-VEND-NET-CST'].cast(DecimalType(8,3)))
  
  df = df.withColumn('TPRX001-SCAN-PRVWK-RTL-MLT', df['TPRX001-SCAN-PRVWK-RTL-MLT'].cast(IntegerType()))
  df = df.withColumn('TPRX001-SCAN-PRVWK-RTL-MLT', lpad(col('TPRX001-SCAN-PRVWK-RTL-MLT'),4,'0'))
  
  df = df.withColumn('TPRX001-SCANPRVDAY-RTL-MLT', df['TPRX001-SCANPRVDAY-RTL-MLT'].cast(IntegerType()))
  df = df.withColumn('TPRX001-SCANPRVDAY-RTL-MLT', lpad(col('TPRX001-SCANPRVDAY-RTL-MLT'),4,'0'))
  
  df = df.withColumn('TPRX001-TAG-PRV-WK-RTL-MLT', df['TPRX001-TAG-PRV-WK-RTL-MLT'].cast(IntegerType()))
  df = df.withColumn('TPRX001-TAG-PRV-WK-RTL-MLT', lpad(col('TPRX001-TAG-PRV-WK-RTL-MLT'),4,'0'))
  
  df = df.withColumn('TPRX001-ECPN-NBR', df['TPRX001-ECPN-NBR'].cast(IntegerType()))
  df = df.withColumn('TPRX001-ECPN-NBR', lpad(col('TPRX001-ECPN-NBR'),14,'0'))
  
  df = df.withColumn('TPRX001-SCAN-PRVWK-RTL-PRC', df['TPRX001-SCAN-PRVWK-RTL-PRC'].cast(DecimalType(7,2)))
  df = df.withColumn('TPRX001-SCANPRVDAY-RTL-PRC', df['TPRX001-SCANPRVDAY-RTL-PRC'].cast(DecimalType(7,2))) 
  df = df.withColumn('TPRX001-TAG-PRV-WK-RTL-PRC', df['TPRX001-TAG-PRV-WK-RTL-PRC'].cast(DecimalType(7,2))) 
  
  # APPENDING PIPELINE PARAMS
  df=df.withColumn("INSERT_ID",lit(pipelineid))
  df=df.withColumn("INSERT_TIMESTAMP",current_timestamp())
  df=df.withColumn("LAST_UPDATE_ID",lit(pipelineid))
  df=df.withColumn("LAST_UPDATE_TIMESTAMP",current_timestamp())
  raw_df = df



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 4.RENAMING COLUMNS

# COMMAND ----------

price_renaming = {"PRICE-BANNER-ID":"PRICE_BANNER_ID",
                  "PRICE-Location":"PRICE_Location",
                  "PRICE-STORE-ID":"PRICE_STORE_ID",
                  "PRICE-UPC":"PRICE_UPC",
                  "PRICE-RTL-PRC":"PRICE_RTL_PRC",
                  "PRICE-UNIT-PRICE":"PRICE_UNIT_PRICE",
                  "PRICE-UOM-CD":"PRICE_UOM_CD",
                  "PRICE-MLT-Quantity":"PRICE_MLT_Quantity",
                  "PRICE-PROMO-RTL-PRC":"PRICE_PROMO_RTL_PRC",
                  "PRICE-PROMO-UNIT-PRICE":"PRICE_PROMO_UNIT_PRICE",
                  "PRICE-Promotional-Quantity":"PRICE_Promotional_Quantity",
                  "PRICE-ACTIVE-FLG":"PRICE_ACTIVE_FLG",
                  "PRICE-START-DATE":"PRICE_START_DATE",
                  "PRICE-END-DATE":"PRICE_END_DATE",
                  "TPRX001-STORE-SID-NBR":"TPRX001_STORE_SID_NBR",
                  "TPRX001-ITEM-NBR":"TPRX001_ITEM_NBR",
                  "TPRX001-DC-ID":"TPRX001_DC_ID",
                  "TPRX001-ITEM-SRC-CD":"TPRX001_ITEM_SRC_CD",
                  "TPRX001-CPN-SRC-CD":"TPRX001_CPN_SRC_CD",
                  "TPRX001-ECPN-NBR":"TPRX001_ECPN_NBR",
                  "TPRX001-RTL-PRC-EFF-DT":"TPRX001_RTL_PRC_EFF_DT",
                  "TPRX001-ITEM-PROMO-FLG":"TPRX001_ITEM_PROMO_FLG",
                  "TPRX001-PROMO-TYP-CD":"TPRX001_PROMO_TYP_CD",
                  "TPRX001-AD-TYP-CD":"TPRX001_AD_TYP_CD",
                  "TPRX001-PROMO-DSC":"TPRX001_PROMO_DSC",
                  "TPRX001-MIX-MTCH-FLG":"TPRX001_MIX_MTCH_FLG",
                  "TPRX001-PRC-STRAT-CD":"TPRX001_PRC_STRAT_CD",
                  "TPRX001-LOYAL-CRD-FLG":"TPRX001_LOYAL_CRD_FLG",
                  "TPRX001-SCAN-AUTH-FLG":"TPRX001_SCAN_AUTH_FLG",
                  "TPRX001-MDSE-AUTH-FLG":"TPRX001_MDSE_AUTH_FLG",
                  "TPRX001-SBT-FLG":"TPRX001_SBT_FLG",
                  "TPRX001-SBT-VEND-ID":"TPRX001_SBT_VEND_ID",
                  "TPRX001-SBT-VEND-NET-CST":"TPRX001_SBT_VEND_NET_CST",
                  "TPRX001-SCAN-DAUTH-DT":"TPRX001_SCAN_DAUTH_DT",
                  "TPRX001-SCAN-PRVWK-RTL-MLT":"TPRX001_SCAN_PRVWK_RTL_MLT",
                  "TPRX001-SCAN-PRVWK-RTL-PRC":"TPRX001_SCAN_PRVWK_RTL_PRC",
                  "TPRX001-SCANPRVDAY-RTL-MLT":"TPRX001_SCANPRVDAY_RTL_MLT",
                  "TPRX001-SCANPRVDAY-RTL-PRC":"TPRX001_SCANPRVDAY_RTL_PRC",
                  "TPRX001-TAG-PRV-WK-RTL-MLT":"TPRX001_TAG_PRV_WK_RTL_MLT",
                  "TPRX001-TAG-PRV-WK-RTL-PRC":"TPRX001_TAG_PRV_WK_RTL_PRC"}

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 5.Renaming UDFs

# COMMAND ----------

#Column renaming functions 
def priceflat_pricetable(s):
    return price_renaming[s]
def change_col_name(s):
    return s in price_renaming

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 6.DELTA TABLE CREATION

# COMMAND ----------

def dropDelta():
    spark.sql("""DROP TABLE IF EXISTS DAPriceDelta;""")

# COMMAND ----------

def removeDeltaFiles(deltaPath):
  dbutils.fs.rm(deltaPath,recurse=True)

# COMMAND ----------

def deltaCreator(deltapath, df):
  global DeltaTableCreateCheck
  spark.sql("""
  CREATE TABLE IF NOT EXISTS DAPriceDelta (
  PRICE_BANNER_ID STRING,
  PRICE_Location STRING,
  PRICE_STORE_ID STRING,
  PRICE_UPC LONG,
  PRICE_RTL_PRC FLOAT,
  PRICE_UNIT_PRICE FLOAT,
  PRICE_UOM_CD STRING,
  PRICE_MLT_Quantity STRING,
  PRICE_PROMO_RTL_PRC FLOAT,
  PRICE_PROMO_UNIT_PRICE FLOAT,
  PRICE_Promotional_Quantity STRING,
  PRICE_ACTIVE_FLG STRING,
  PRICE_START_DATE STRING,
  PRICE_END_DATE STRING,
  TPRX001_STORE_SID_NBR STRING,
  TPRX001_ITEM_NBR STRING,
  TPRX001_DC_ID STRING,
  TPRX001_ITEM_SRC_CD STRING,
  TPRX001_CPN_SRC_CD STRING,
  TPRX001_ECPN_NBR INTEGER,
  TPRX001_RTL_PRC_EFF_DT STRING,
  TPRX001_ITEM_PROMO_FLG STRING,
  TPRX001_PROMO_TYP_CD STRING,
  TPRX001_AD_TYP_CD STRING,
  TPRX001_PROMO_DSC STRING,
  TPRX001_MIX_MTCH_FLG STRING,
  TPRX001_PRC_STRAT_CD STRING,
  TPRX001_LOYAL_CRD_FLG STRING,
  TPRX001_SCAN_AUTH_FLG STRING,
  TPRX001_MDSE_AUTH_FLG STRING,
  TPRX001_SBT_FLG STRING,
  TPRX001_SBT_VEND_ID STRING,
  TPRX001_SBT_VEND_NET_CST DECIMAL(8,3),
  TPRX001_SCAN_DAUTH_DT STRING,
  TPRX001_SCAN_PRVWK_RTL_MLT STRING,
  TPRX001_SCAN_PRVWK_RTL_PRC DECIMAL(7,2),
  TPRX001_SCANPRVDAY_RTL_MLT STRING,
  TPRX001_SCANPRVDAY_RTL_PRC DECIMAL(7,2),
  TPRX001_TAG_PRV_WK_RTL_MLT STRING,
  TPRX001_TAG_PRV_WK_RTL_PRC DECIMAL(7,2),
  INSERT_ID STRING,
  INSERT_TIMESTAMP TIMESTAMP,
  LAST_UPDATE_ID STRING,
  LAST_UPDATE_TIMESTAMP TIMESTAMP
  )
  USING delta
  Location '{}'
  PARTITIONED BY (PRICE_STORE_ID)
  """.format(deltapath))
  
  spark.sql("OPTIMIZE DAPriceDelta")  

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ##### 7.MERGE/ UPSERT DATA TO DELTA TABLE

# COMMAND ----------

def mergeData(df):
  global initial_recs
  global appended_recs
  loggerAtt.info("Merge into Delta table initiated")
  #spark.sql('''CACHE SELECT * FROM DAPriceDelta;''')
  if df is not None:
    temp_table_name = "df_data_delta_to_upsert"
    df.createOrReplaceTempView(temp_table_name)
    try:
      #spark.sql("USE {}".format(databaseName))
      initial_recs = spark.sql("""SELECT count(*) as count from DAPriceDelta;""")
      print(f"Initial count of records in Delta Table: {initial_recs.head(1)}")
      initial_recs = initial_recs.head(1)
#      df_data_delta_to_upsert = df
      
      #Merge into DeltaTableName = DAPriceDelta
      spark.sql('''
      MERGE INTO DAPriceDelta
      USING df_data_delta_to_upsert
      ON DAPriceDelta.PRICE_STORE_ID = df_data_delta_to_upsert.PRICE_STORE_ID
      AND DAPriceDelta.PRICE_UPC = df_data_delta_to_upsert.PRICE_UPC
      WHEN MATCHED THEN
        UPDATE SET
          DAPriceDelta.PRICE_BANNER_ID = df_data_delta_to_upsert.PRICE_BANNER_ID,
          DAPriceDelta.PRICE_RTL_PRC = df_data_delta_to_upsert.PRICE_RTL_PRC,
          DAPriceDelta.PRICE_UNIT_PRICE = df_data_delta_to_upsert.PRICE_RTL_PRC,
          DAPriceDelta.PRICE_UOM_CD = df_data_delta_to_upsert.PRICE_UOM_CD,
          DAPriceDelta.PRICE_MLT_Quantity = df_data_delta_to_upsert.PRICE_MLT_Quantity,
          DAPriceDelta.PRICE_PROMO_RTL_PRC = df_data_delta_to_upsert.PRICE_PROMO_RTL_PRC,
          DAPriceDelta.PRICE_PROMO_UNIT_PRICE = df_data_delta_to_upsert.PRICE_PROMO_UNIT_PRICE,
          DAPriceDelta.PRICE_Promotional_Quantity = df_data_delta_to_upsert.PRICE_Promotional_Quantity,
          DAPriceDelta.PRICE_ACTIVE_FLG = df_data_delta_to_upsert.PRICE_ACTIVE_FLG,
          DAPriceDelta.PRICE_START_DATE = df_data_delta_to_upsert.PRICE_START_DATE,
          DAPriceDelta.PRICE_END_DATE = df_data_delta_to_upsert.PRICE_END_DATE,
          DAPriceDelta.TPRX001_STORE_SID_NBR = df_data_delta_to_upsert.TPRX001_STORE_SID_NBR,
          DAPriceDelta.TPRX001_ITEM_NBR = df_data_delta_to_upsert.TPRX001_ITEM_NBR,
          DAPriceDelta.TPRX001_DC_ID = df_data_delta_to_upsert.TPRX001_DC_ID,
          DAPriceDelta.TPRX001_ITEM_SRC_CD = df_data_delta_to_upsert.TPRX001_ITEM_SRC_CD,
          DAPriceDelta.TPRX001_CPN_SRC_CD = df_data_delta_to_upsert.TPRX001_CPN_SRC_CD,
          DAPriceDelta.TPRX001_ECPN_NBR = df_data_delta_to_upsert.TPRX001_ECPN_NBR,
          DAPriceDelta.TPRX001_RTL_PRC_EFF_DT = df_data_delta_to_upsert.TPRX001_RTL_PRC_EFF_DT,
          DAPriceDelta.TPRX001_ITEM_PROMO_FLG = df_data_delta_to_upsert.TPRX001_ITEM_PROMO_FLG,
          DAPriceDelta.TPRX001_PROMO_TYP_CD = df_data_delta_to_upsert.TPRX001_PROMO_TYP_CD,
          DAPriceDelta.TPRX001_AD_TYP_CD = df_data_delta_to_upsert.TPRX001_AD_TYP_CD,
          DAPriceDelta.TPRX001_PROMO_DSC = df_data_delta_to_upsert.TPRX001_PROMO_DSC,
          DAPriceDelta.TPRX001_MIX_MTCH_FLG = df_data_delta_to_upsert.TPRX001_MIX_MTCH_FLG,
          DAPriceDelta.TPRX001_PRC_STRAT_CD = df_data_delta_to_upsert.TPRX001_PRC_STRAT_CD,
          DAPriceDelta.TPRX001_LOYAL_CRD_FLG = df_data_delta_to_upsert.TPRX001_LOYAL_CRD_FLG,
          DAPriceDelta.TPRX001_SCAN_AUTH_FLG = df_data_delta_to_upsert.TPRX001_SCAN_AUTH_FLG,
          DAPriceDelta.TPRX001_MDSE_AUTH_FLG = df_data_delta_to_upsert.TPRX001_MDSE_AUTH_FLG,
          DAPriceDelta.TPRX001_SBT_FLG = df_data_delta_to_upsert.TPRX001_SBT_FLG,
          DAPriceDelta.TPRX001_SBT_VEND_ID = df_data_delta_to_upsert.TPRX001_SBT_VEND_ID,
          DAPriceDelta.TPRX001_SBT_VEND_NET_CST = df_data_delta_to_upsert.TPRX001_SBT_VEND_NET_CST,
          DAPriceDelta.TPRX001_SCAN_DAUTH_DT = df_data_delta_to_upsert.TPRX001_SCAN_DAUTH_DT,
          DAPriceDelta.TPRX001_SCAN_PRVWK_RTL_MLT = df_data_delta_to_upsert.TPRX001_SCAN_PRVWK_RTL_MLT,
          DAPriceDelta.TPRX001_SCAN_PRVWK_RTL_PRC = df_data_delta_to_upsert.TPRX001_SCAN_PRVWK_RTL_PRC,
          DAPriceDelta.TPRX001_SCANPRVDAY_RTL_MLT = df_data_delta_to_upsert.TPRX001_SCANPRVDAY_RTL_MLT,
          DAPriceDelta.TPRX001_SCANPRVDAY_RTL_PRC = df_data_delta_to_upsert.TPRX001_SCANPRVDAY_RTL_PRC,
          DAPriceDelta.TPRX001_TAG_PRV_WK_RTL_MLT = df_data_delta_to_upsert.TPRX001_TAG_PRV_WK_RTL_MLT,
          DAPriceDelta.TPRX001_TAG_PRV_WK_RTL_PRC = df_data_delta_to_upsert.TPRX001_TAG_PRV_WK_RTL_PRC,
          DAPriceDelta.LAST_UPDATE_ID = df_data_delta_to_upsert.LAST_UPDATE_ID,
          DAPriceDelta.LAST_UPDATE_TIMESTAMP = df_data_delta_to_upsert.LAST_UPDATE_TIMESTAMP
      WHEN NOT MATCHED
        THEN INSERT * ''')
      
      spark.sql("OPTIMIZE DAPriceDelta")

#      spark.sql(sqlCmd)
      appended_recs = spark.sql("""SELECT count(*) as count from DAPriceDelta""")
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
# MAGIC # UNIFIED ITEM WORK

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 1.UNIFIED ITEM PRICE FIELDS

# COMMAND ----------

unifiedPriceFields = ["BANNER_ID", "SMA_DEST_STORE", "SMA_GTIN_NUM", "PRICE_UNIT_PRICE", "PRICE_UOM_CD", "PRICE_MLT_Quantity", "PRICE_PROMO_RTL_PRC", "PRICE_PROMO_UNIT_PRICE", "PRICE_Promotional_Quantity", "PRICE_START_DATE", "PRICE_END_DATE", "TPRX001_STORE_SID_NBR", "TPRX001_ITEM_NBR","TPRX001_ITEM_SRC_CD", "TPRX001_CPN_SRC_CD", "TPRX001_ITEM_PROMO_FLG" , "TPRX001_PROMO_TYP_CD", "TPRX001_AD_TYP_CD", "TPRX001_PROMO_DSC", "TPRX001_MIX_MTCH_FLG", "TPRX001_PRC_STRAT_CD", "TPRX001_LOYAL_CRD_FLG", "TPRX001_SCAN_AUTH_FLG", "TPRX001_MDSE_AUTH_FLG","TPRX001_SBT_FLG", "TPRX001_SBT_VEND_ID","TPRX001_SBT_VEND_NET_CST", "TPRX001_SCAN_DAUTH_DT", "TPRX001_SCAN_PRVWK_RTL_MLT", "TPRX001_SCAN_PRVWK_RTL_PRC", "TPRX001_SCANPRVDAY_RTL_MLT", "TPRX001_SCANPRVDAY_RTL_PRC", "TPRX001_TAG_PRV_WK_RTL_MLT", "TPRX001_TAG_PRV_WK_RTL_PRC", "SMA_SOURCE_WHSE", "SMA_SMR_EFF_DATE", "TPRX001_RTL_PRC_EFF_DT", "ALT_UPC_FETCH" ]


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 2.UNIFIED ITEM RENAMING

# COMMAND ----------

price_TempDeltaRenaming = { "TPRX001_DC_ID":"SMA_SOURCE_WHSE"
                            }

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ##### 3.RENAMING UDFs

# COMMAND ----------

def typeCheck(s):
  try: 
      int(s)
      return True
  except ValueError:
      return False

typeCheckUDF = udf(typeCheck)

#Column renaming functions 
def itemEff_promotable(s):
    return price_TempDeltaRenaming[s]
def itemEff_change_col_name(s):
    return s in price_TempDeltaRenaming

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 4.DELTA TABLE CREATION - ITEMMAINTEMP & PRICETEMP

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

def deltaTempCreator(priceEffDeltaPath):
  try:
    spark.sql("""
    CREATE TABLE IF NOT EXISTS DApriceTemp (
    PRICE_BANNER_ID STRING,
    PRICE_STORE_ID STRING,
    PRICE_UPC LONG,
    PRICE_UNIT_PRICE FLOAT,
    PRICE_UOM_CD STRING,
    PRICE_MLT_Quantity STRING,
    PRICE_PROMO_RTL_PRC FLOAT,
    PRICE_PROMO_UNIT_PRICE FLOAT,
    PRICE_Promotional_Quantity STRING,
    PRICE_START_DATE STRING,
    PRICE_END_DATE STRING,
    TPRX001_STORE_SID_NBR STRING,
    TPRX001_ITEM_NBR STRING,
    SMA_LEGACY_ITEM STRING,
    SMA_SOURCE_WHSE STRING,
    TPRX001_ITEM_SRC_CD STRING,
    TPRX001_CPN_SRC_CD STRING,
    SMA_SMR_EFF_DATE STRING,
    TPRX001_RTL_PRC_EFF_DT STRING,
    TPRX001_ITEM_PROMO_FLG STRING,
    TPRX001_PROMO_TYP_CD STRING,
    TPRX001_AD_TYP_CD STRING,
    TPRX001_PROMO_DSC STRING,
    TPRX001_MIX_MTCH_FLG STRING,
    TPRX001_PRC_STRAT_CD STRING,
    TPRX001_LOYAL_CRD_FLG STRING,
    TPRX001_SCAN_AUTH_FLG STRING,
    TPRX001_MDSE_AUTH_FLG STRING,
    TPRX001_SBT_FLG STRING,
    TPRX001_SBT_VEND_ID STRING,
    TPRX001_SBT_VEND_NET_CST DECIMAL(8,3),
    TPRX001_SCAN_DAUTH_DT STRING,
    TPRX001_SCAN_PRVWK_RTL_MLT STRING,
    TPRX001_SCAN_PRVWK_RTL_PRC DECIMAL(7,2),
    TPRX001_SCANPRVDAY_RTL_MLT STRING,
    TPRX001_SCANPRVDAY_RTL_PRC DECIMAL(7,2),
    TPRX001_TAG_PRV_WK_RTL_MLT STRING,
    TPRX001_TAG_PRV_WK_RTL_PRC DECIMAL(7,2)
    )
    USING delta
    Location '{}'
    PARTITIONED BY (PRICE_STORE_ID)
    """.format(priceEffDeltaPath))

    spark.sql("OPTIMIZE DApriceTemp")
    ABC(DeltaFeedTempTableCreateCheck = 1)
  except Exception as ex:
      ABC(DeltaFeedTempTableCreateCheck = 0)
      loggerAtt.error(ex)
      err = ErrorReturn('Error', ex,'DeltaFeedTempTableCreateCheck')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))
  

  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 5.TRANSFORMATIONS RELATED TO UNIFIED

# COMMAND ----------

def itemMasterTransformation(df):
  #df = df.withColumn("SMA_SMR_EFF_DATE", col("TPRX001_RTL_PRC_EFF_DT"))
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 6.FETCHING UNIFIED ITEM RECORDS

# COMMAND ----------

def itemMasterRecordsModified(itemTempEffDeltaPath, priceEffDeltaPath, PriceDeltaPath, Date, unifiedPriceFields):
  loggerAtt.info("itemMasterRecords fetch initiated")
  priceEffTemp = spark.sql('''DELETE FROM delta.`{}`'''.format(priceEffDeltaPath))
  priceDelta = spark.read.format('delta').load(PriceDeltaPath)
  #print(priceDelta.count())
  priceDelta=priceDelta.withColumn('PRICE_BANNER_ID', regexp_replace(col("PRICE_BANNER_ID"), " ", ""))
  itemTempEffDelta = spark.read.format('delta').load(itemTempEffDeltaPath)
  itemTempEffDelta = spark.sql('''select BANNER_ID,SMA_DEST_STORE,SMA_GTIN_NUM, ALT_UPC_FETCH from delta.`{}`'''.format(itemTempEffDeltaPath))
  itemTempEffDelta = itemTempEffDelta.withColumn('BANNER_ID', regexp_replace(col("BANNER_ID"), " ", ""))
  priceDelta=priceDelta.withColumnRenamed('TPRX001_DC_ID', 'SMA_SOURCE_WHSE')
  priceDelta = priceDelta.withColumn("SMA_SMR_EFF_DATE", col("TPRX001_RTL_PRC_EFF_DT"))
  
  
  priceEffTemp = itemTempEffDelta.join(priceDelta, [itemTempEffDelta.SMA_GTIN_NUM == priceDelta.PRICE_UPC, itemTempEffDelta.SMA_DEST_STORE == priceDelta.PRICE_STORE_ID, itemTempEffDelta.BANNER_ID == priceDelta.PRICE_BANNER_ID], how='left').select([col(xx) for xx in unifiedPriceFields]) 
  
  priceEffTemp = priceEffTemp.withColumnRenamed('BANNER_ID', 'PRICE_BANNER_ID')
  priceEffTemp = priceEffTemp.withColumnRenamed('SMA_GTIN_NUM', 'PRICE_UPC')
  priceEffTemp = priceEffTemp.withColumnRenamed('SMA_DEST_STORE', 'PRICE_STORE_ID')
  priceEffTemp = priceEffTemp.withColumnRenamed('ALT_UPC_FETCH', 'SMA_LEGACY_ITEM')
#   priceEffTemp = priceEffTemp.withColumn('SMA_LEGACY_ITEM', col('SMA_LEGACY_ITEM').cast(IntegerType()))
  priceEffTemp = priceEffTemp.withColumn('SMA_LEGACY_ITEM', lpad(col('SMA_LEGACY_ITEM'),12,'0'))
  
  
  priceEffTemp = quinn.with_some_columns_renamed(itemEff_promotable, itemEff_change_col_name)(priceEffTemp)
  priceEffTemp = itemMasterTransformation(priceEffTemp) 
  priceEffTemp.write.partitionBy('PRICE_STORE_ID').format('delta').mode('append').save(priceEffDeltaPath)
  UnifiedRecordFeedCnt = priceEffTemp.count()
  ABC(UnifiedRecordFeedCount = UnifiedRecordFeedCnt)
  ABC(UnifiedRecordItemCount = itemTempEffDelta.count())
  loggerAtt.info("UnifiedRecordPriceCount:" +str(UnifiedRecordFeedCnt))
  loggerAtt.info("UnifiedRecordItemCount:" +str(itemTempEffDelta.count()))
  loggerAtt.info("itemMasterRecords fetch successful")
#   display(priceEffTemp.filter(col('PRICE-Location')= )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # MAIN FUNCTION

# COMMAND ----------

if __name__ == "__main__":
    loggerAtt.info('======== Input Delhaize Product Price Feed processing initiated ========')
    price_filename = "price_dailymaintainence_custom_log"
    #dbutils.widgets.removeAll()
    # Step 1: Reading File
    ##### Step 1.a: Reading Params
    file_location = str(file_location)   #file_location = "/mnt/delhaizeFeed-productPrice/Productprice/Inbound/ProductPrice.txt"
    file_type = "com.databricks.spark.csv"
    infer_schema = "True"
    header = "False"
    delimiter = "|"
    schema = priceRaw_schema
    
    if POSemergencyFlag == 'Y':
      loggerAtt.info('Feed processing happening for POS Emergency feed')
      try:
          ABC(UnifiedRecordFetch=1)		
          deltaTempCreator(str(priceEffDeltaPath))
          itemMasterRecordsModified(itemTempEffDeltaPath,priceEffDeltaPath,str(DeltaPath),Date,unifiedPriceFields)      
      except Exception as ex:
          ABC(UnifiedRecordFetch=0)
          ABC(UnifiedRecordFeedCount=0)
          ABC(UnifiedRecordItemCount=0)
          loggerAtt.error(ex)
          err = ErrorReturn('Error', ex,'UnifiedItemCreation')
          errJson = jsonpickle.encode(err)
          errJson = json.loads(errJson)
          dbutils.notebook.exit(Merge(ABCChecks,errJson))
    else:
      loggerAtt.info('Feed processing happening for POS Daile/Weekly feed')
    
      ##### Step 1.b: Executing Read Function 
      try:
          readFile(file_location, infer_schema, header, delimiter, file_type, schema = priceRaw_schema)
          ABC(ReadDataCheck=1)
          RawDataCount = raw_df.count()
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
            #err.exit()

      ##### Step 1.c: Removing BOF & EOF records from df 
      try:
          BOFEOF(raw_df)
          ABC(BOFEOFCheck=1)
          EOFcnt = EOFCount
          BOFDt = BOFDate
          ABC(EOFCount=EOFcnt)
          ABC(BOFDate=BOFDt)        
          loggerAtt.info("BOF & EOF Processing completed")
          loggerAtt.info(f"BOF value: {BOFDt}")
          loggerAtt.info(f"EOF value: {EOFCount}")
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
          #err.exit() 



      # Step 2: Transformation
      ##### Step 2.a: Removing Duplicate Records
      try:
          productPrice_nullRows = raw_df.where(reduce(lambda x, y: x | y, (col(x).isNull() for x in raw_df.columns)))
          ABC(NullValueCheck=1)
          raw_df = raw_df.na.drop()
          ABC(DropNACheck = 1)
          NullValuCnt = productPrice_nullRows.count()
          loggerAtt.info(f"Null Value Count: {NullValuCnt}")        
          ABC(NullValuCount = NullValuCnt)
      except:
          NullValueCheck = 0
          NullValuCnt=''
          ABC(NullValueCheck=0)
          ABC(NullValuCount = NullValuCnt)  
          loggerAtt.error(ex)
          err = ErrorReturn('Error', ex,'NullRecordHandling')
          errJson = jsonpickle.encode(err)
          errJson = json.loads(errJson)
          dbutils.notebook.exit(Merge(ABCChecks,errJson))
          #err.exit() 


      try:
          ABC(DuplicateValueCheck = 1)
          if (raw_df.groupBy('PRICE-BANNER-ID','PRICE-Location', 'PRICE-UPC').count().filter("count > 1").count()) > 0:
              price_ProblemRecs = raw_df.groupBy('PRICE-BANNER-ID','PRICE-Location', 'PRICE-UPC').count().filter(col('count') > 1)
              price_ProblemRecs=price_ProblemRecs.drop(price_ProblemRecs['count'])
              price_ProblemRecs = (raw_df.join(price_ProblemRecs,["PRICE-UPC"], "leftsemi"))
              DuplicateValueCnt = price_ProblemRecs.count()
              loggerAtt.info(f"Duplicate Record Count: {DuplicateValueCnt}")
              ABC(DuplicateValueCount=DuplicateValueCnt)
              raw_df = (raw_df.join(price_ProblemRecs,["PRICE-UPC"], "leftanti"))
          else:
              price_ProblemRecs = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema=priceRaw_schema)
              ABC(DuplicateValueCount=0)
              loggerAtt.info(f"No PROBLEM RECORDS")
      except Exception as ex:
          ABC(DuplicateValueCheck = 0)
          ABC(DuplicateValueCount='')
          loggerAtt.error(ex)
          err = ErrorReturn('Error', ex,'price_ProblemRecs')
          errJson = jsonpickle.encode(err)
          errJson = json.loads(errJson)
          dbutils.notebook.exit(Merge(ABCChecks,errJson))
          #err.exit()


      try:
          priceTransformation(raw_df, pipelineid=str(PipelineID))
          ABC(TransformationCheck=1)
          loggerAtt.info(f"Price Transformation Completed")
      except Exception as ex:
          ABC(TransformationCheck = 0)
          loggerAtt.error(ex)
          err = ErrorReturn('Error', ex,'priceTransformation')
          errJson = jsonpickle.encode(err)
          errJson = json.loads(errJson)
          dbutils.notebook.exit(Merge(ABCChecks,errJson))
          #err.exit()


      try:
          loggerAtt.info("Product Price Feed Renaming Started")
          ABC(RenamingCheck=1)
          if raw_df is not None:
              raw_df = quinn.with_some_columns_renamed(priceflat_pricetable, change_col_name)(raw_df)
              loggerAtt.info("Product Price Feed Renaming completed")
          loggerAtt.info(f"Price Feed Renaming Completed")
      except Exception as ex:
          ABC(RenamingCheck = 0)
          loggerAtt.error(ex)
          err = ErrorReturn('Error', ex,'RenamingColumns')
          errJson = jsonpickle.encode(err)
          errJson = json.loads(errJson)
          dbutils.notebook.exit(Merge(ABCChecks,errJson))
          #err.exit()


      try:
          dropDelta()
          removeDeltaFiles(deltaPath = str(DeltaPath))
          deltaCreator(deltapath = str(DeltaPath), df = raw_df)
          ABC(DeltaTableCreateCheck=1)
          #spark.sql('''ALTER TABLE DAPriceDelta SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)''')
          #spark.sql('''VACUUM DAPriceDelta RETAIN 36 HOURS''')
          #OPTIMIZE DAPriceDelta
          mergeData(df=raw_df) 
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
          #err.exit()


      try:
          price_ProblemRecs = price_ProblemRecs.union(productPrice_nullRows)
          #price_ProblemRecs.withColumnRenamed("PRICE-Promotional Quantity", "PRICE-Promotional-Quantity")
          price_ProblemRecs.repartition(1).write.mode('overwrite').parquet(Invalid_RecordsPath + Date +'/')
          #price_ProblemRecs.write.format('com.databricks.spark.csv').mode('overwrite').option("header", "true").save(Invalid_RecordsPath)
          ABC(InvalidRecordSaveCheck = 1)
          invalidCount = price_ProblemRecs.count()
          loggerAtt.info(f"Count of Invalid record count = Duplicate problem records + Null value records : {invalidCount}")        
          ABC(InvalidRecordCount = invalidCount)
          loggerAtt.info(f"Invalid Records saved at path: {Invalid_RecordsPath + Date +'/'}")
      except Exception as ex:
          ABC(InvalidRecordSaveCheck = 0)
          ABC(InvalidRecordCount='')
          loggerAtt.error(ex)
          err = ErrorReturn('Error', ex,'InvalidRecordsSave')
          errJson = jsonpickle.encode(err)
          errJson = json.loads(errJson)
          dbutils.notebook.exit(Merge(ABCChecks,errJson))
          #err.exit()

      try:
          ABC(UnifiedRecordFetch=1)		
          deltaTempCreator(str(priceEffDeltaPath))
          itemMasterRecordsModified(itemTempEffDeltaPath,priceEffDeltaPath,str(DeltaPath),Date,unifiedPriceFields)      
      except Exception as ex:
          ABC(UnifiedRecordFetch=0)
          ABC(UnifiedRecordFeedCount=0)
          ABC(UnifiedRecordItemCount=0)
          loggerAtt.error(ex)
          err = ErrorReturn('Error', ex,'UnifiedItemCreation')
          errJson = jsonpickle.encode(err)
          errJson = json.loads(errJson)
          dbutils.notebook.exit(Merge(ABCChecks,errJson))
loggerAtt.info('======== Delhaize Product Price Feed processing ended ========')

# COMMAND ----------

# MAGIC %md 
# MAGIC # EXIT PROGRAM: WRITING LOG FILE TO ADLS LOCATION

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

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DAPriceDelta

# COMMAND ----------

