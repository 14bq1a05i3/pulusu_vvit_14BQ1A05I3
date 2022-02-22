# Databricks notebook source
def promoLinkingMain(item_raw_df,Date_serial):
  #Selecting required columns of Promotion main from raw/source file
  try:
    ABC(promoMainSetUp = 1)
    item_raw_df=item_raw_df.select([c for c in item_raw_df.columns if c in PromotionLinkMain_List])

    #Start Date and end date  getting '00000000' values therefore renaming it  
    item_raw_df=item_raw_df.filter((col("SMA_LINK_APPLY_DATE") != '00000000') & (col("SMA_LINK_END_DATE") != '00000000')  & (col("SMA_LINK_CHG_TYPE") != 0))

    item_raw_df=item_raw_df.filter(col("SMA_LINK_HDR_COUPON").isNotNull())

    item_raw_df=item_raw_df.withColumn('SMA_LINK_APPLY_DATE', to_date(date_format(date_func(col('SMA_LINK_APPLY_DATE')), 'yyyy-MM-dd')))
    item_raw_df=item_raw_df.withColumn('SMA_LINK_END_DATE',to_date(date_format(date_func(col('SMA_LINK_END_DATE')), 'yyyy-MM-dd')))
    
    loggerAtt.info(f"No of promotion main records for insert: {item_raw_df.count()}")
    
  except Exception as ex:
    ABC(promoMainSetUp = 0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'Initial Processing PromolinkingMain')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
    
  try:
    ABC(DeltaTableCreateCheck = 1)
    if item_raw_df is not None:
      if item_raw_df.count()>0:
          temp_table_name = "Promotion_renamed"
          item_raw_df.createOrReplaceTempView(temp_table_name)

          initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(PromotionMainDeltaPath))
          loggerAtt.info(f"Initial count of records in Promotion Main Delta Table: {initial_recs.head(1)}")
          initial_recs = initial_recs.head(1)
          ABC(promoLinkingMainInitCount=initial_recs[0][0])

          spark.sql('''INSERT INTO delta.`{}` SELECT  SMA_PROMO_LINK_UPC,
                                                      SMA_ITM_EFF_DATE,
                                                      SMA_LINK_HDR_COUPON,
                                                      SMA_LINK_HDR_LOCATION,
                                                      SMA_LINK_APPLY_DATE,
                                                      SMA_LINK_APPLY_TIME,
                                                      SMA_LINK_END_DATE,
                                                      SMA_LINK_CHG_TYPE,
                                                      SMA_CHG_TYPE,
                                                      SMA_STORE,
                                                      SMA_BATCH_SERIAL_NBR,
                                                      INSERT_ID,
                                                      INSERT_TIMESTAMP
                                                      FROM Promotion_renamed'''.format(PromotionMainDeltaPath))

          appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(PromotionMainDeltaPath))
          loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
          appended_recs = appended_recs.head(1)
          ABC(promoLinkingMainFinalCount=appended_recs[0][0])


          spark.catalog.dropTempView(temp_table_name)
          loggerAtt.info("Merge into PromotionMain Delta table successful")
  except Exception as ex:
    ABC(DeltaTableCreateCheck = 0)
    ABC(promoLinkingMainFinalCount = '')
    ABC(promoLinkingMainInitCount = '')
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'PromolinkingMain')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson)) 