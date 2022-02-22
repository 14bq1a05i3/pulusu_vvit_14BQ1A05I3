# Databricks notebook source
def insertPriceChange(priceChange_transformed_df):
  loggerAtt.info("Append/Insert into Delta table initiated")
  
  initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(PriceChangeDeltaPath))
  loggerAtt.info(f"Initial count of records in Price Change Delta Table: {initial_recs.head(1)}")
  initial_recs = initial_recs.head(1)
  ABC(priceChangeInitCount=initial_recs[0][0])

  temp_table_name = "priceChangeRecords"
  priceChange_transformed_df.createOrReplaceTempView(temp_table_name)
  
  priceChange_transformed_df.write\
        .format("delta")\
        .mode("Append")\
        .option("mergeSchema",True)\
        .save(PriceChangeDeltaPath) 
  
  appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(PriceChangeDeltaPath))
  loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
  appended_recs = appended_recs.head(1)
  ABC(priceChangeFinalCount=appended_recs[0][0])


  spark.catalog.dropTempView(temp_table_name)
  loggerAtt.info("Merge into PriceChange Delta table successful")

# COMMAND ----------

def priceChangeFunc(item_raw_df):
  try:
    ABC(priceChangeSetUp = 1)
    #Fetching increase/Decrease chg type records
    item_raw_df = item_raw_df.filter(((col('SMA_RETL_CHG_TYPE') == 'I') | (col('SMA_RETL_CHG_TYPE') == 'D')))
    
    
    item_raw_df = item_raw_df.select([c for c in item_raw_df.columns if c in PriceChange_List])
    item_raw_df= item_raw_df.selectExpr(  "SMA_PRICE_CHG_ID",
                                          "SMA_GTIN_NUM",
                                          "SMA_STORE ",
                                          "LOCATION_TYPE",
                                          "SMA_ITM_EFF_DATE",
                                          "SELL_UNIT_CHG_IND",
                                          "SMA_SELL_RETL",
                                          "SMA_RETL_UOM",
                                          "SELL_RETAIL_CURR",
                                          "MLT_UNIT_CHG_IND",
                                          "SMA_RETL_MULT_UNIT",
                                          "SMA_MULT_UNIT_RETL",
                                          "SMA_RETL_MULT_UOM",
                                          "MLT_UNIT_CUR",
                                          "SMA_BATCH_SERIAL_NBR",
                                          "PRICECHANGE_STATUS as STATUS",
                                          "SMA_RETL_PRC_ENTRY_CHG",
                                          "PRICECHANGE_CUSTOM_FIELD_2 as CUSTOM_FIELD_2",
                                          "PRICECHANGE_CUSTOM_FIELD_3 as CUSTOM_FIELD_3",
                                          "INSERT_ID",
                                          "INSERT_TIMESTAMP")
  except Exception as ex:
    ABC(priceChangeSetUp = 0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'Initial Processing Price Change')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
    
  try: 
    ABC(DeltaTableCreateCheck = 0)
    if item_raw_df is not None:
      if item_raw_df.count()>0:
        insertPriceChange(item_raw_df)
    else:
      loggerAtt.info("Error in input file reading")
      
  except Exception as ex:
    ABC(DeltaTableCreateCheck = 0)
    ABC(priceChangeFinalCount = '')
    ABC(priceChangeInitCount = '')
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'insertPriceChange')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))   
  loggerAtt.info('======== Input Price Change file processing ended ========')