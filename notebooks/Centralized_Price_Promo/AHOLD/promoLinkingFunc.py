# Databricks notebook source
# def activePromoUpdate(active_promos):
#   loggerAtt.info("active promos exists")

# #   active_promos=active_promos.filter(col('rank')==1)
  
#   if active_promos.count()>0:
#     temp_table_name = "active_promo_id"
#     active_promos.createOrReplaceTempView(temp_table_name)
#     loggerAtt.info(f"No of active promo records for update: {active_promos.count()}")
#     display()
#     loggerAtt.info("UPDATING THE PROMOTION LINK FOR ACTIVE PROMO ID FOR A GIVEN ITEM COUPON LOACTION FOR CHANGE TYPE 2,3,4,5,6,7")
#     spark.sql('''MERGE INTO delta.`{}` as Promotion_linking USING active_promo_id 
#                   ON Promotion_linking.SMA_PROMO_LINK_UPC = active_promo_id.SMA_PROMO_LINK_UPC 
#                   AND Promotion_linking.SMA_STORE= active_promo_id.SMA_STORE 
#                   AND Promotion_linking.SMA_LINK_HDR_COUPON= active_promo_id.SMA_LINK_HDR_COUPON  
#                   AND Promotion_linking.INSERT_TIMESTAMP= active_promo_id.INSERT_TIMESTAMP  
#                   WHEN MATCHED AND active_promo_id.SMA_CHG_TYPE IN (2,3,4,5) Then 
#                     Update Set Promotion_linking.SMA_LINK_END_DATE = active_promo_id.SMA_LINK_END_DATE , 
#                                Promotion_linking.SMA_LINK_HDR_MAINT_TYPE =active_promo_id.SMA_LINK_HDR_MAINT_TYPE,
#                                Promotion_linking.SMA_LINK_CHG_TYPE =active_promo_id.SMA_LINK_CHG_TYPE,
#                                Promotion_linking.SMA_LINK_FAMCD_PROMOCD = active_promo_id.SMA_LINK_FAMCD_PROMOCD,
#                                Promotion_linking.SMA_BATCH_SERIAL_NBR = active_promo_id.SMA_BATCH_SERIAL_NBR,
#                                Promotion_linking.SMA_LINK_TYPE = active_promo_id.SMA_LINK_TYPE,
#                                Promotion_linking.LAST_UPDATE_ID=active_promo_id.LAST_UPDATE_ID,
#                                Promotion_linking.LAST_UPDATE_TIMESTAMP=active_promo_id.LAST_UPDATE_TIMESTAMP
#                   WHEN MATCHED AND active_promo_id.SMA_CHG_TYPE IN (6,7) Then
#                    Update Set  Promotion_linking.SMA_LINK_END_DATE = date_sub(active_promo_id.SMA_ITM_EFF_DATE,1),
#                                Promotion_linking.LAST_UPDATE_ID=active_promo_id.LAST_UPDATE_ID,
#                                Promotion_linking.LAST_UPDATE_TIMESTAMP=active_promo_id.LAST_UPDATE_TIMESTAMP'''.format(PromotionLinkDeltaPath))
    
#     loggerAtt.info("UPDATING THE PROMOTION LINK FOR CURRENT ITEM WITH DIFFERENT COUPON NUMBER")
#     ## Promotion_temp already created when fetching active promo
#     spark.sql('''MERGE INTO delta.`{}` as Promotion_linking USING active_promo_id  
#                    ON Promotion_linking.SMA_PROMO_LINK_UPC = active_promo_id.SMA_PROMO_LINK_UPC 
#                    AND Promotion_linking.SMA_STORE= active_promo_id.SMA_STORE 
#                    AND Promotion_linking.SMA_LINK_HDR_COUPON != active_promo_id.SMA_LINK_HDR_COUPON  
#                    AND (active_promo_id.SMA_ITM_EFF_DATE BETWEEN Promotion_linking.SMA_LINK_START_DATE AND Promotion_linking.SMA_LINK_END_DATE)
#                    AND active_promo_id.SMA_CHG_TYPE IN (2,3,4,5)
#                    WHEN MATCHED Then 
#                      Update Set Promotion_linking.SMA_LINK_END_DATE = date_sub(active_promo_id.SMA_ITM_EFF_DATE,1) , 
#                                 Promotion_linking.SMA_LINK_HDR_MAINT_TYPE = 'D',
#                                 promotion_linking.LAST_UPDATE_ID=active_promo_id.LAST_UPDATE_ID,
#                                 Promotion_linking.LAST_UPDATE_TIMESTAMP=active_promo_id.LAST_UPDATE_TIMESTAMP'''.format(PromotionLinkDeltaPath))
#     spark.catalog.dropTempView(temp_table_name)
#     loggerAtt.info("Successful update of active promo")

# COMMAND ----------

def activePromoUpdateSameCoupon(active_promos):
  loggerAtt.info("active promos exists")

#   active_promos=active_promos.filter(col('rank')==1)
  
  if active_promos.count()>0:
    temp_table_name = "active_promo_id"
    active_promos.createOrReplaceTempView(temp_table_name)
    loggerAtt.info(f"No of active promo records for update: {active_promos.count()}")
    display()
    loggerAtt.info("UPDATING THE PROMOTION LINK FOR ACTIVE PROMO ID FOR A GIVEN ITEM COUPON LOACTION FOR CHANGE TYPE 2,3,4,5,6,7")
    spark.sql('''MERGE INTO delta.`{}` as Promotion_linking USING active_promo_id 
                  ON Promotion_linking.SMA_PROMO_LINK_UPC = active_promo_id.SMA_PROMO_LINK_UPC 
                  AND Promotion_linking.SMA_STORE= active_promo_id.SMA_STORE 
                  AND Promotion_linking.SMA_LINK_HDR_COUPON= active_promo_id.SMA_LINK_HDR_COUPON  
                  AND Promotion_linking.INSERT_TIMESTAMP= active_promo_id.INSERT_TIMESTAMP  
                  WHEN MATCHED AND active_promo_id.SMA_CHG_TYPE IN (2,3,4,5) Then 
                    Update Set Promotion_linking.SMA_LINK_END_DATE = active_promo_id.SMA_LINK_END_DATE , 
                               Promotion_linking.SMA_LINK_HDR_MAINT_TYPE =active_promo_id.SMA_LINK_HDR_MAINT_TYPE,
                               Promotion_linking.SMA_LINK_CHG_TYPE =active_promo_id.SMA_LINK_CHG_TYPE,
                               Promotion_linking.SMA_LINK_FAMCD_PROMOCD = active_promo_id.SMA_LINK_FAMCD_PROMOCD,
                               Promotion_linking.SMA_BATCH_SERIAL_NBR = active_promo_id.SMA_BATCH_SERIAL_NBR,
                               Promotion_linking.SMA_LINK_TYPE = active_promo_id.SMA_LINK_TYPE,
                               Promotion_linking.LAST_UPDATE_ID=active_promo_id.LAST_UPDATE_ID,
                               Promotion_linking.LAST_UPDATE_TIMESTAMP=active_promo_id.LAST_UPDATE_TIMESTAMP
                  WHEN MATCHED AND active_promo_id.SMA_CHG_TYPE IN (6,7) Then
                   Update Set  Promotion_linking.SMA_LINK_END_DATE = date_sub(active_promo_id.SMA_ITM_EFF_DATE,1),
                               Promotion_linking.LAST_UPDATE_ID=active_promo_id.LAST_UPDATE_ID,
                               Promotion_linking.LAST_UPDATE_TIMESTAMP=active_promo_id.LAST_UPDATE_TIMESTAMP'''.format(PromotionLinkDeltaPath))

# COMMAND ----------

def activePromoUpdateDiffCoupon(active_promos):
  loggerAtt.info("active promos exists")
  if active_promos.count()>0:
    loggerAtt.info("UPDATING THE PROMOTION LINK FOR CURRENT ITEM WITH DIFFERENT COUPON NUMBER")
    ## Promotion_temp already created when fetching active promo
    spark.sql('''MERGE INTO delta.`{}` as Promotion_linking USING Promotion_temp  
                   ON Promotion_linking.SMA_PROMO_LINK_UPC = Promotion_temp.SMA_PROMO_LINK_UPC 
                   AND Promotion_linking.SMA_STORE= Promotion_temp.SMA_STORE 
                   AND Promotion_linking.SMA_LINK_HDR_COUPON != Promotion_temp.SMA_LINK_HDR_COUPON  
                   AND (Promotion_temp.SMA_ITM_EFF_DATE BETWEEN Promotion_linking.SMA_LINK_START_DATE AND Promotion_linking.SMA_LINK_END_DATE)
                   AND Promotion_temp.SMA_CHG_TYPE IN (2,3,4,5)
                   WHEN MATCHED Then 
                     Update Set Promotion_linking.SMA_LINK_END_DATE = date_sub(Promotion_temp.SMA_ITM_EFF_DATE,1) , 
                                Promotion_linking.SMA_LINK_HDR_MAINT_TYPE = 'D',
                                promotion_linking.LAST_UPDATE_ID=Promotion_temp.LAST_UPDATE_ID,
                                Promotion_linking.LAST_UPDATE_TIMESTAMP=Promotion_temp.LAST_UPDATE_TIMESTAMP'''.format(PromotionLinkDeltaPath))
    temp_table_name = "active_promo_id"
    spark.catalog.dropTempView(temp_table_name)
    loggerAtt.info("Successful update of active promo")

# COMMAND ----------

def updateNonActivePromo(PromotionLinkDeltaPath):
  spark.sql('''MERGE INTO delta.`{}` as Promotion_linking USING Promotion_temp  
               ON Promotion_linking.SMA_PROMO_LINK_UPC = Promotion_temp.SMA_PROMO_LINK_UPC 
               AND Promotion_linking.SMA_STORE= Promotion_temp.SMA_STORE 
               AND (Promotion_LINKING.SMA_LINK_START_DATE <=Promotion_temp.SMA_ITM_EFF_DATE     
                     AND Promotion_LINKING.SMA_LINK_END_DATE >= Promotion_temp.SMA_ITM_EFF_DATE)
               AND Promotion_temp.SMA_CHG_TYPE IN (2,3,4,5)
               WHEN MATCHED  Then 
                 Update Set Promotion_linking.SMA_LINK_END_DATE = date_sub(Promotion_temp.SMA_ITM_EFF_DATE,1),
                            Promotion_linking.SMA_LINK_HDR_MAINT_TYPE = 'D',
                            promotion_linking.LAST_UPDATE_ID=Promotion_temp.LAST_UPDATE_ID,
                            Promotion_linking.LAST_UPDATE_TIMESTAMP=Promotion_temp.LAST_UPDATE_TIMESTAMP'''.format(PromotionLinkDeltaPath))

# COMMAND ----------

def fetchNonActivePromoRecords(active_promos, promo_DF):
  active_promos = active_promos.filter((col('SMA_CHG_TYPE') == 2) | (col('SMA_CHG_TYPE') == 3)|(col('SMA_CHG_TYPE') == 4) | (col('SMA_CHG_TYPE') == 5)|(col('SMA_CHG_TYPE') == 6) | (col('SMA_CHG_TYPE') == 7))

#   active_promos = active_promos.drop(active_promos.rank)
  promo_DF =promo_DF.filter((col('SMA_CHG_TYPE') == 2) | (col('SMA_CHG_TYPE') == 3)|(col('SMA_CHG_TYPE') == 4) | (col('SMA_CHG_TYPE') == 5)|(col('SMA_CHG_TYPE') == 6) | (col('SMA_CHG_TYPE') == 7))
  
  loggerAtt.info(f"No of active promo records to remove from promo record: {active_promos.count()}")
  loggerAtt.info(f"No of promo records before union: {promo_DF.count()}")
  
#   promo_DF = promo_DF.select("SMA_PROMO_LINK_UPC", "SMA_STORE", "SMA_LINK_HDR_COUPON", "SMA_LINK_HDR_LOCATION", "SMA_CHG_TYPE", "SMA_LINK_START_DATE", "SMA_LINK_RCRD_TYPE", "SMA_LINK_END_DATE", "SMA_ITM_EFF_DATE", "SMA_LINK_CHG_TYPE", "LOCATION_TYPE", "MERCH_TYPE", "SMA_LINK_HDR_MAINT_TYPE", "SMA_LINK_ITEM_NBR", "SMA_LINK_OOPS_ADWK", "SMA_LINK_OOPS_FILE_ID", "SMA_LINK_TYPE", "SMA_LINK_SYS_DIGIT", "SMA_LINK_FAMCD_PROMOCD", "SMA_BATCH_SERIAL_NBR", "INSERT_ID", "INSERT_TIMESTAMP", "LAST_UPDATE_ID", "LAST_UPDATE_TIMESTAMP")
  
  promo_DF = promo_DF.select("SMA_LINK_HDR_COUPON", "SMA_LINK_HDR_LOCATION", "SMA_STORE", "SMA_CHG_TYPE", "SMA_LINK_START_DATE", "SMA_LINK_RCRD_TYPE", "SMA_LINK_END_DATE", "SMA_ITM_EFF_DATE", "SMA_LINK_CHG_TYPE", "SMA_PROMO_LINK_UPC", "LOCATION_TYPE", "MERCH_TYPE", "SMA_LINK_HDR_MAINT_TYPE", "SMA_LINK_ITEM_NBR", "SMA_LINK_OOPS_ADWK", "SMA_LINK_OOPS_FILE_ID", "SMA_LINK_TYPE", "SMA_LINK_SYS_DIGIT", "SMA_LINK_FAMCD_PROMOCD", "SMA_BATCH_SERIAL_NBR", "INSERT_ID", "INSERT_TIMESTAMP", "LAST_UPDATE_ID", "LAST_UPDATE_TIMESTAMP")
  
  active_promos = active_promos.drop("INSERT_ID", "INSERT_TIMESTAMP", "LAST_UPDATE_ID", "LAST_UPDATE_TIMESTAMP")
  promo_DF = promo_DF.drop("INSERT_ID", "INSERT_TIMESTAMP", "LAST_UPDATE_ID", "LAST_UPDATE_TIMESTAMP")
  
#   print(promo_DF.printSchema())
#   print(active_promos.printSchema())
  promo_DF = promo_DF.union(active_promos)
  promo_DF = promo_DF.groupBy(promo_DF.columns).count()
  promo_DF = promo_DF.filter("count == 1").drop('count')
  
  loggerAtt.info(f"No of promo records after union for insert: {promo_DF.count()}")
  return promo_DF

# COMMAND ----------

# def fetchInsertPromoRecords(promo_DF, promoInvalidRecordsCount):
#   promo_DF=promo_DF.withColumn("INSERT_ID",lit(PipelineID))
#   promo_DF=promo_DF.withColumn("INSERT_TIMESTAMP",current_timestamp())
#   promo_DF=promo_DF.withColumn("LAST_UPDATE_ID",lit(PipelineID))
#   promo_DF=promo_DF.withColumn("LAST_UPDATE_TIMESTAMP",current_timestamp())
#   promoInvalidRecords= promo_DF.filter((col('SMA_CHG_TYPE') == 6) | (col('SMA_CHG_TYPE') == 7))
#   promo_DF= promo_DF.filter((col('SMA_CHG_TYPE') != 6) & (col('SMA_CHG_TYPE') != 7))
  
  
#   if promoInvalidRecords != None:
#     if promoInvalidRecords.count() > 0:
#       ABC(inactivePromoUpdate = '')
#       promoInvalidRecordsCount = promoInvalidRecordsCount + promoInvalidRecords.count()
#       ABC(promoInvalidRecordsCount = promoInvalidRecordsCount)
#       loggerAtt.info(f"Added Promo Invalid Record count due to chg type 6, 7: {promoInvalidRecordsCount}")        
#       promoInvalidRecords=promoInvalidRecords.withColumn('ERROR_MESSAGE',lit('Received Off record for a promo link that does not exist'))
#       promoInvalidRecords.write.format("parquet").mode("Append").save(promoErrorPath)
  
#   return promo_DF
  

# COMMAND ----------

def insertPromoRecords(promo_DF, promoInvalidRecordsCount):
  promo_DF=promo_DF.withColumn("INSERT_ID",lit(PipelineID))
  promo_DF=promo_DF.withColumn("INSERT_TIMESTAMP",current_timestamp())
  promo_DF=promo_DF.withColumn("LAST_UPDATE_ID",lit(PipelineID))
  promo_DF=promo_DF.withColumn("LAST_UPDATE_TIMESTAMP",current_timestamp())
  promoInvalidRecords= promo_DF.filter((col('SMA_CHG_TYPE') == 6) | (col('SMA_CHG_TYPE') == 7))
  promo_DF= promo_DF.filter((col('SMA_CHG_TYPE') != 6) & (col('SMA_CHG_TYPE') != 7))
  
  
  if promoInvalidRecords != None:
    if promoInvalidRecords.count() > 0:
      ABC(inactivePromoUpdate = '')
      promoInvalidRecordsCount = promoInvalidRecordsCount + promoInvalidRecords.count()
      ABC(promoInvalidRecordsCount = promoInvalidRecordsCount)
      loggerAtt.info(f"Added Promo Invalid Record count due to chg type 6, 7: {promoInvalidRecordsCount}")        
      promoInvalidRecords=promoInvalidRecords.withColumn('ERROR_MESSAGE',lit('Received Off record for a promo link that does not exist'))
      promoInvalidRecords.write.format("parquet").mode("Append").save(promoErrorPath)
  
  
  
  loggerAtt.info(f"new records inserted into promotion link delta table by removing active promoid records from source file: {promo_DF.count()}")
  
  initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(PromotionLinkDeltaPath))
  loggerAtt.info(f"Initial count of records in Promotion Link Delta Table: {initial_recs.head(1)}")
  initial_recs = initial_recs.head(1)
  ABC(promoLinkingInitCount=initial_recs[0][0])
  
  promo_DF=promo_DF.filter((col('SMA_CHG_TYPE') == 2) | (col('SMA_CHG_TYPE') == 3)|(col('SMA_CHG_TYPE') == 4) | (col('SMA_CHG_TYPE') == 5))
  promo_DF.write.format('delta').mode('Append').save(PromotionLinkDeltaPath)
  
  appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(PromotionLinkDeltaPath))
  loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
  appended_recs = appended_recs.head(1)
  ABC(promoLinkingFinalCount=appended_recs[0][0])

# COMMAND ----------

def promoProcessing(promo_DF, promoInvalidRecordsCount):
  try:
    if promo_DF.count() > 0:
      active_promos=spark.sql('''SELECT pt.SMA_LINK_HDR_COUPON, 
                                        pt.SMA_LINK_HDR_LOCATION, 
                                        pt.SMA_STORE, 
                                        pt.SMA_CHG_TYPE, 
                                        pt.SMA_LINK_START_DATE, 
                                        pt.SMA_LINK_RCRD_TYPE,
                                        pt.SMA_LINK_END_DATE,
                                        pt.SMA_ITM_EFF_DATE,
                                        pt.SMA_LINK_CHG_TYPE,
                                        pt.SMA_PROMO_LINK_UPC,
                                        pt.LOCATION_TYPE,
                                        pt.MERCH_TYPE,
                                        pt.SMA_LINK_HDR_MAINT_TYPE,
                                        pt.SMA_LINK_ITEM_NBR,
                                        pt.SMA_LINK_OOPS_ADWK,
                                        pt.SMA_LINK_OOPS_FILE_ID,
                                        pt.SMA_LINK_TYPE,
                                        pt.SMA_LINK_SYS_DIGIT,
                                        pt.SMA_LINK_FAMCD_PROMOCD,
                                        pt.SMA_BATCH_SERIAL_NBR,
                                        pl.INSERT_ID,
                                        pl.INSERT_TIMESTAMP,
                                        pt.LAST_UPDATE_ID,
                                        pt.LAST_UPDATE_TIMESTAMP
                                    ,DENSE_RANK() OVER   
                                    (PARTITION BY pt.SMA_STORE, pt.SMA_PROMO_LINK_UPC, pt.SMA_LINK_HDR_COUPON ORDER BY pl.SMA_LINK_START_DATE desc, pl.INSERT_TIMESTAMP desc) AS Rank  
                                FROM (select * from delta.`{}`) pl  
                                INNER JOIN Promotion_temp pt
                                  ON pl.SMA_PROMO_LINK_UPC = pt.SMA_PROMO_LINK_UPC 
                                  AND pl.SMA_STORE= pt.SMA_STORE 
                                  AND pl.SMA_LINK_HDR_COUPON=pt.SMA_LINK_HDR_COUPON
                                  AND (pt.SMA_ITM_EFF_DATE between pl.SMA_LINK_START_DATE AND pl.SMA_LINK_END_DATE)'''.format(PromotionLinkDeltaPath))
    active_promos = active_promos.filter((col('Rank') == 1))
    active_promos = active_promos.drop(col('Rank'))
    loggerAtt.info(f"Active Promo before filter Count: {active_promos.count()}")
    loggerAtt.info(f"Total Input Promo Count: {promo_DF.count()}")
    loggerAtt.info(f"Active Promo Count: {active_promos.count()}")
    ABC(activePromoCount = active_promos.count())
  except Exception as ex:
    ABC(activePromoCount = '')
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'PromotionLink active promo fetch')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  ## Step 4: Promo processing 
  if active_promos is not None:
    if active_promos.count()>0:
#       try:
#         ABC(fetchNonActivePromoRecordsCheck = 1)
#         nonActivepromo_DF = fetchNonActivePromoRecords(active_promos, promo_DF).cache()
#       except Exception as ex:
#         loggerAtt.error(ex)
#         err = ErrorReturn('Error', ex,'fetchNonActivePromoRecords')
#         ABC(fetchNonActivePromoRecordsCheck = '')
#         errJson = jsonpickle.encode(err)
#         errJson = json.loads(errJson)
#         dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
      try:
        ABC(activePromoUpdateCheck = 1)
        activePromoUpdateSameCoupon(active_promos)
      except Exception as ex:
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'activePromoUpdate same coupon')
        ABC(activePromoUpdateCheck = '')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))

      try:
        ABC(activePromoUpdateCheck = 1)
        activePromoUpdateDiffCoupon(active_promos)
      except Exception as ex:
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'activePromoUpdate different coupon')
        ABC(activePromoUpdateCheck = '')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))                

    else:
        loggerAtt.info("UPDATING FOR B2B and NO EXISTING PROMOTION LINK FOR ITEM-CPN COMBINATION")
        try:
          ABC(inactivePromoUpdate = 1)
          updateNonActivePromo(PromotionLinkDeltaPath)
        except Exception as ex:
          loggerAtt.error(ex)
          err = ErrorReturn('Error', ex,'updateNonActivePromo')
          ABC(inactivePromoUpdate = '')
          errJson = jsonpickle.encode(err)
          errJson = json.loads(errJson)
          dbutils.notebook.exit(Merge(ABCChecks,errJson))
  else:
    loggerAtt.info("no active promos")
    try:
      ABC(inactivePromoUpdate = 1)
      updateNonActivePromo(PromotionLinkDeltaPath)
    except Exception as ex:
      loggerAtt.error(ex)
      err = ErrorReturn('Error', ex,'updateNonActivePromo')
      ABC(inactivePromoUpdate = '')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))

  if active_promos is not None:
    if active_promos.count() > 0:
      try:
        ABC(fetchNonActivePromoRecordsCheck = 1)
        nonActivepromo_DF = fetchNonActivePromoRecords(active_promos, promo_DF)
      except Exception as ex:
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'fetchNonActivePromoRecords')
        ABC(fetchNonActivePromoRecordsCheck = '')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))

      try:
        ABC(insertPromoRecordsCheck = 1)
        insertPromoRecords(nonActivepromo_DF, promoInvalidRecordsCount)
      except Exception as ex:
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'insertPromoRecords')
        ABC(insertPromoRecordsCheck = 0)
        ABC(promoLinkingFinalCount='')
        ABC(promoLinkingInitCount='')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
    else:
      try:
        ABC(insertPromoRecordsCheck = 1)
        insertPromoRecords(promo_DF, promoInvalidRecordsCount)
      except Exception as ex:
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'insertPromoRecords')
        ABC(insertPromoRecordsCheck = 0)
        ABC(promoLinkingFinalCount='')
        ABC(promoLinkingInitCount='')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))
  else:
    try:
      ABC(insertPromoRecordsCheck = 1)
      insertPromoRecords(promo_DF, promoInvalidRecordsCount)
    except Exception as ex:
      loggerAtt.error(ex)
      err = ErrorReturn('Error', ex,'insertPromoRecords')
      ABC(insertPromoRecordsCheck = 0)
      ABC(promoLinkingFinalCount='')
      ABC(promoLinkingInitCount='')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

def processPromoRec(duplicatesPromo, promoInvalidRecordsCount, duplicateFlag):
  try:
    temp_table_name = "duplicatesPromo"
    duplicatesPromo.createOrReplaceTempView(temp_table_name)
    duplicatesPromoBeforeCnt = duplicatesPromo.count()
    ABC(duplicatesPromoBeforeCnt=duplicatesPromoBeforeCnt)
    loggerAtt.info(f"Duplicates Promo Count before group by: {duplicatesPromoBeforeCnt}")

    duplicatesPromo = spark.sql('''select SMA_STORE,
                                      first_value(SMA_BATCH_SERIAL_NBR) as SMA_BATCH_SERIAL_NBR,
                                      first_value(SMA_CHG_TYPE) as SMA_CHG_TYPE,
                                      SMA_ITM_EFF_DATE,
                                      first_value(SMA_LINK_HDR_LOCATION) as SMA_LINK_HDR_LOCATION,
                                      first_value(SMA_LINK_RCRD_TYPE) as SMA_LINK_RCRD_TYPE,
                                      first_value(SMA_LINK_HDR_MAINT_TYPE) as SMA_LINK_HDR_MAINT_TYPE,
                                      SMA_LINK_HDR_COUPON,
                                      first_value(SMA_LINK_START_DATE) as SMA_LINK_START_DATE,
                                      first_value(SMA_LINK_END_DATE) as SMA_LINK_END_DATE,
                                      first_value(SMA_LINK_ITEM_NBR) as SMA_LINK_ITEM_NBR,
                                      SMA_PROMO_LINK_UPC,
                                      first_value(SMA_LINK_TYPE) as SMA_LINK_TYPE,
                                      first_value(SMA_LINK_FAMCD_PROMOCD) as SMA_LINK_FAMCD_PROMOCD,
                                      first_value(SMA_LINK_CHG_TYPE) as SMA_LINK_CHG_TYPE,
                                      first_value(SMA_LINK_OOPS_ADWK) as SMA_LINK_OOPS_ADWK,
                                      first_value(SMA_LINK_OOPS_FILE_ID) as SMA_LINK_OOPS_FILE_ID,
                                      first_value(LOCATION_TYPE) as LOCATION_TYPE,
                                      first_value(MERCH_TYPE) as MERCH_TYPE,
                                      first_value(INSERT_ID) as INSERT_ID,
                                      first_value(INSERT_TIMESTAMP) as INSERT_TIMESTAMP,
                                      first_value(LAST_UPDATE_ID) as LAST_UPDATE_ID,
                                      first_value(LAST_UPDATE_TIMESTAMP) as LAST_UPDATE_TIMESTAMP,
                                      first_value(SMA_LINK_SYS_DIGIT) as SMA_LINK_SYS_DIGIT
                                from duplicatesPromo
                                group by SMA_STORE, SMA_PROMO_LINK_UPC, SMA_LINK_HDR_COUPON, SMA_ITM_EFF_DATE''')

    duplicatesPromoAfterCnt = duplicatesPromo.count()
    ABC(duplicatesPromoAfterCnt=duplicatesPromoAfterCnt)
    loggerAtt.info(f"Duplicates Promo Count after group by: {duplicatesPromoAfterCnt}")
  except Exception as ex:
    ABC(duplicatesPromoAfterCnt='')
    ABC(duplicatesPromoBeforeCnt='')
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'duplicate promo group by')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

  try:
    temp_table_name = "duplicatesPromo"
    ABC(dupPromoRankChk=1)
    duplicatesPromo.createOrReplaceTempView(temp_table_name)

    duplicatesPromo=spark.sql('''SELECT SMA_LINK_HDR_COUPON, 
                                      SMA_LINK_HDR_LOCATION, 
                                      SMA_STORE, 
                                      SMA_CHG_TYPE, 
                                      SMA_LINK_START_DATE, 
                                      SMA_LINK_RCRD_TYPE,
                                      SMA_LINK_END_DATE,
                                      SMA_ITM_EFF_DATE,
                                      SMA_LINK_CHG_TYPE,
                                      SMA_PROMO_LINK_UPC,
                                      LOCATION_TYPE,
                                      MERCH_TYPE,
                                      SMA_LINK_HDR_MAINT_TYPE,
                                      SMA_LINK_ITEM_NBR,
                                      SMA_LINK_OOPS_ADWK,
                                      SMA_LINK_OOPS_FILE_ID,
                                      SMA_LINK_TYPE,
                                      SMA_LINK_SYS_DIGIT,
                                      SMA_LINK_FAMCD_PROMOCD,
                                      SMA_BATCH_SERIAL_NBR,
                                      INSERT_ID,
                                      INSERT_TIMESTAMP,
                                      LAST_UPDATE_ID,
                                      LAST_UPDATE_TIMESTAMP,
                                      DENSE_RANK() OVER   
                                  (PARTITION BY SMA_STORE, SMA_PROMO_LINK_UPC 
                                  ORDER BY SMA_ITM_EFF_DATE asc, SMA_LINK_CHG_TYPE asc, SMA_LINK_HDR_COUPON desc) AS Rank  
                              FROM duplicatesPromo''')
  except Exception as ex:
    ABC(dupPromoRankChk='')
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'duplicate promo ranking')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

  try:
    ABC(dupPromoRankLoop=1)
    duplicatesPromo = duplicatesPromo.withColumn("duplicateFlag", lit(duplicateFlag).cast(BooleanType()));
    loggerAtt.info(f"Promo Count: {duplicatesPromo.count()} for flag {duplicateFlag}")
    duplicatesPromoTemp = spark.sql('''DELETE FROM delta.`{}` where duplicateFlag={}'''.format(promoLinkTemp, duplicateFlag))
    duplicatesPromo.write.partitionBy('SMA_STORE').format('delta').mode('append').save(promoLinkTemp)
  except Exception as ex:
    ABC(dupPromoRankLoop='')
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'duplicate promo load into temp table')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

def processPromoRecProcessing(promoInvalidRecordsCount, duplicateFlag):
  try:
    duplicatesPromo = spark.sql('''SELECT * FROM delta.`{}` where duplicateFlag={}'''.format(promoLinkTemp, duplicateFlag))
    duplicatesPromo = duplicatesPromo.drop("duplicateFlag")
    loggerAtt.info(f"Promo Count : {duplicatesPromo.count()} for duplicateFlag {duplicateFlag}")
    val = list(set(duplicatesPromo.select('Rank').toPandas()['Rank']))
    pandasDF = duplicatesPromo.toPandas()
    val.sort()
    arr = []
    for i in val:
      promoRec = duplicatesPromo.filter((col('Rank') == i))
      promoRec = promoRec.drop(col('Rank'))
      temp_table_name = "Promotion_temp"
      promoRec.createOrReplaceTempView(temp_table_name)
      loggerAtt.info(f"Promo Count for Rank {i} : {promoRec.count()} for duplicateFlag {duplicateFlag}")
      arr.append(promoRec)
      promoProcessing(promoRec, promoInvalidRecordsCount)
  except Exception as ex:
    ABC(dupPromoRankLoop='')
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'duplicate promo loop based on rank')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

def promoLinking(promo_DF, Invalid_RecordsPath, processing_file):   
#   promo_DF=item_raw_df
  active_promos=None
  nonActivepromo_DF=None
  promoInvalidRecords=None
  promoInvalidRecordsCount = 0

  try:
    promo_DF=promo_DF.select([c for c in promo_DF.columns if c in PromotionLink_List])

    promo_DF=promo_DF.filter((col("SMA_LINK_CHG_TYPE")!= 0))

  except Exception as ex:
    ABC(productRecallinvalidRecCount = '')
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'Filtering promo records')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))

  if promo_DF != None:
    if promo_DF.count() > 0:
      ## Step 1: Pre-Process and remove invalid records based on coupon numder
      try:
        promoInvalidRecords=promo_DF.filter(col("SMA_LINK_HDR_COUPON").isNull())

        if promoInvalidRecords != None:
          if promoInvalidRecords.count() > 0:
            promoInvalidRecordsCount = promoInvalidRecords.count()
            loggerAtt.info(f"Promo Invalid Record file count based on coupon no: {promoInvalidRecordsCount}")        
            promoInvalidRecords=promoInvalidRecords.withColumn('ERROR_MESSAGE',lit('Coupon no is null'))
            promoInvalidRecords.write.format("parquet").mode("Append").save(promoErrorPath)

        promo_DF=promo_DF.filter(col("SMA_LINK_HDR_COUPON").isNotNull())
      except Exception as ex:
        ABC(productRecallinvalidRecCount = '')
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'PromotionLink error on coupon check')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))

      ## Step 2: Pre-Process and remove invalid start date and end date records
      try:
        promoInvalidRecords=promo_DF.filter(((col("SMA_LINK_START_DATE")== '00000000') | (col("SMA_LINK_END_DATE")== '00000000')))

        if promoInvalidRecords != None:
          if promoInvalidRecords.count() > 0:
            promoInvalidRecordsCount = promoInvalidRecordsCount + promoInvalidRecords.count()
            loggerAtt.info(f"Promo Invalid Record file count after combining error due to start date and end date: {promoInvalidRecordsCount}")
            ABC(promoInvalidRecordsCount = promoInvalidRecordsCount)
            promoInvalidRecords=promoInvalidRecords.withColumn('ERROR_MESSAGE',lit('start date or end date is 0'))
            promoInvalidRecords.write.format("parquet").mode("Append").save(promoErrorPath)

        promo_DF=promo_DF.filter(((col("SMA_LINK_START_DATE")!= '00000000') & (col("SMA_LINK_END_DATE")!= '00000000')))

        promo_DF=promo_DF.withColumn('SMA_LINK_START_DATE', to_date(date_format(date_func(col('SMA_LINK_START_DATE')), 'yyyy-MM-dd')))
        promo_DF=promo_DF.withColumn('SMA_LINK_END_DATE', to_date(date_format(date_func(col('SMA_LINK_END_DATE')), 'yyyy-MM-dd')))

      except Exception as ex:
        ABC(productRecallinvalidRecCount = '')
        loggerAtt.error(ex)
        err = ErrorReturn('Error', ex,'PromotionLink error on start date/end date check')
        errJson = jsonpickle.encode(err)
        errJson = json.loads(errJson)
        dbutils.notebook.exit(Merge(ABCChecks,errJson))

      if promo_DF != None:
        if promo_DF.count() > 0:
          ## Step 3: Active Promo fetch
          try:
            promo_DF= promo_DF.na.drop()

            promo_DF=promo_DF.withColumn('SMA_LINK_SYS_DIGIT',lit(None).cast(StringType())) 

            loggerAtt.info(f"No of promotion link records for insert/update: {promo_DF.count()}")

            temp_table_name = 'Promotion_temp'
            promo_DF.createOrReplaceTempView(temp_table_name)

            loggerAtt.info(f"Promotion_temp table created from source file with count:{promo_DF.count()}")
          except Exception as ex:
            ABC(activePromoCount = '')
            loggerAtt.error(ex)
            err = ErrorReturn('Error', ex,'PromotionLink active promo fetch setting')
            errJson = jsonpickle.encode(err)
            errJson = json.loads(errJson)
            dbutils.notebook.exit(Merge(ABCChecks,errJson))

          try:
            temp = spark.sql('''select SMA_PROMO_LINK_UPC, SMA_STORE, collect_set(SMA_LINK_HDR_COUPON) as coupon_list, count(*) as count from 
                                      (select Promotion_temp.* from delta.`{}` as Promotion_linking join Promotion_temp  
                                       ON Promotion_linking.SMA_PROMO_LINK_UPC = Promotion_temp.SMA_PROMO_LINK_UPC 
                                       AND Promotion_linking.SMA_STORE= Promotion_temp.SMA_STORE 
                                       AND (Promotion_temp.SMA_ITM_EFF_DATE BETWEEN Promotion_linking.SMA_LINK_START_DATE AND Promotion_linking.SMA_LINK_END_DATE)) 
                                       group by SMA_PROMO_LINK_UPC, SMA_STORE having count > 1'''.format(PromotionLinkDeltaPath))
            temp_table_name = "temp_id"
            temp.createOrReplaceTempView(temp_table_name)
          except Exception as ex:
            ABC(activePromoCount = '')
            loggerAtt.error(ex)
            err = ErrorReturn('Error', ex,'Fetching all problem records')
            errJson = jsonpickle.encode(err)
            errJson = json.loads(errJson)
            dbutils.notebook.exit(Merge(ABCChecks,errJson))

          try:
            ABC(promo_DFBeforeCnt=promo_DF.count())
            promo_DF = spark.sql('''select Promotion_temp.* from Promotion_temp LEFT ANTI JOIN temp_id
                                ON temp_id.SMA_PROMO_LINK_UPC = Promotion_temp.SMA_PROMO_LINK_UPC 
                                AND temp_id.SMA_STORE = Promotion_temp.SMA_STORE 
                                AND Promotion_temp.SMA_LINK_HDR_COUPON in 
                                    (temp_id.coupon_list[0], temp_id.coupon_list[1], temp_id.coupon_list[2], temp_id.coupon_list[3], temp_id.coupon_list[4], temp_id.coupon_list[5], temp_id.coupon_list[6])'''.format(PromotionLinkDeltaPath))
            ABC(promo_DFAfterCnt=promo_DF.count())
  #           temp_table_name = "Promotion_temp"
  #           promo_DF.createOrReplaceTempView(temp_table_name)
          except Exception as ex:
            ABC(promo_DFAfterCnt='')
            ABC(promo_DFBeforeCnt='')
            loggerAtt.error(ex)
            err = ErrorReturn('Error', ex,'Removing error records')
            errJson = jsonpickle.encode(err)
            errJson = json.loads(errJson)
            dbutils.notebook.exit(Merge(ABCChecks,errJson))
          try:
            duplicatesPromo = spark.sql('''select Promotion_temp.* from Promotion_temp LEFT SEMI JOIN temp_id
                                           ON temp_id.SMA_PROMO_LINK_UPC = Promotion_temp.SMA_PROMO_LINK_UPC 
                                           AND temp_id.SMA_STORE = Promotion_temp.SMA_STORE 
                                           AND Promotion_temp.SMA_LINK_HDR_COUPON in 
                                                (temp_id.coupon_list[0], temp_id.coupon_list[1], temp_id.coupon_list[2], 
                                                temp_id.coupon_list[3], temp_id.coupon_list[4], temp_id.coupon_list[5], 
                                                temp_id.coupon_list[6])'''.format(PromotionLinkDeltaPath))

            temp_table_name = "duplicatesPromo"
            duplicatesPromo.createOrReplaceTempView(temp_table_name)
            temp_table_name = 'Promotion_temp'
            promo_DF.createOrReplaceTempView(temp_table_name)
          except Exception as ex:
            loggerAtt.error(ex)
            err = ErrorReturn('Error', ex,'Taking error records separate')
            errJson = jsonpickle.encode(err)
            errJson = json.loads(errJson)
            dbutils.notebook.exit(Merge(ABCChecks,errJson))
          try:
            ABC(tempCnt=temp.count())
            Invalid_RecordsPath = Invalid_RecordsPath + "/" +Date+ "/" + "duplicates_PromoLink"
            writeInvalidRecord(temp, Invalid_RecordsPath, Date)
          except Exception as ex:
            ABC(tempCnt='')
            loggerAtt.error(ex)
            err = ErrorReturn('Error', ex,'Write error records')
            errJson = jsonpickle.encode(err)
            errJson = json.loads(errJson)
            dbutils.notebook.exit(Merge(ABCChecks,errJson))
          
          if processing_file != 'FullItem':
            processPromoRec(duplicatesPromo, promoInvalidRecordsCount, True)
          processPromoRec(promo_DF, promoInvalidRecordsCount, False)
          processPromoRecProcessing(promoInvalidRecordsCount, False)
#           promoProcessing(promo_DF,promoInvalidRecordsCount)
          
          if processing_file != 'FullItem':
            processPromoRecProcessing(promoInvalidRecordsCount, True)


# COMMAND ----------

# def promoLinking(promo_DF, Invalid_RecordsPath): 
#   active_promos=None
#   promoInvalidRecords=None
#   promoInvalidRecordsCount = 0
  
#   try:
#     promo_DF=promo_DF.select([c for c in promo_DF.columns if c in PromotionLink_List])
  
#     promo_DF=promo_DF.filter((col("SMA_LINK_CHG_TYPE")!= 0))
    
#   except Exception as ex:
#     ABC(productRecallinvalidRecCount = '')
#     loggerAtt.error(ex)
#     err = ErrorReturn('Error', ex,'Filtering promo records')
#     errJson = jsonpickle.encode(err)
#     errJson = json.loads(errJson)
#     dbutils.notebook.exit(Merge(ABCChecks,errJson))
  
#   if promo_DF != None:
#     if promo_DF.count() > 0:
#       ## Step 1: Pre-Process and remove invalid records based on coupon numder
#       try:
#         promoInvalidRecords=promo_DF.filter(col("SMA_LINK_HDR_COUPON").isNull())

#         if promoInvalidRecords != None:
#           if promoInvalidRecords.count() > 0:
#             promoInvalidRecordsCount = promoInvalidRecords.count()
#             loggerAtt.info(f"Promo Invalid Record file count based on coupon no: {promoInvalidRecordsCount}")        
#             promoInvalidRecords=promoInvalidRecords.withColumn('ERROR_MESSAGE',lit('Coupon no is null'))
#             promoInvalidRecords.write.format("parquet").mode("Append").save(promoErrorPath)

#         promo_DF=promo_DF.filter(col("SMA_LINK_HDR_COUPON").isNotNull())
#       except Exception as ex:
#         ABC(productRecallinvalidRecCount = '')
#         loggerAtt.error(ex)
#         err = ErrorReturn('Error', ex,'PromotionLink error on coupon check')
#         errJson = jsonpickle.encode(err)
#         errJson = json.loads(errJson)
#         dbutils.notebook.exit(Merge(ABCChecks,errJson))

#       ## Step 2: Pre-Process and remove invalid start date and end date records
#       try:
#         promoInvalidRecords=promo_DF.filter(((col("SMA_LINK_START_DATE")== '00000000') | (col("SMA_LINK_END_DATE")== '00000000')))

#         if promoInvalidRecords != None:
#           if promoInvalidRecords.count() > 0:
#             promoInvalidRecordsCount = promoInvalidRecordsCount + promoInvalidRecords.count()
#             loggerAtt.info(f"Promo Invalid Record file count after combining error due to start date and end date: {promoInvalidRecordsCount}")
#             ABC(promoInvalidRecordsCount = promoInvalidRecordsCount)
#             promoInvalidRecords=promoInvalidRecords.withColumn('ERROR_MESSAGE',lit('start date or end date is 0'))
#             promoInvalidRecords.write.format("parquet").mode("Append").save(promoErrorPath)

#         promo_DF=promo_DF.filter(((col("SMA_LINK_START_DATE")!= '00000000') & (col("SMA_LINK_END_DATE")!= '00000000')))

#         promo_DF=promo_DF.withColumn('SMA_LINK_START_DATE', to_date(date_format(date_func(col('SMA_LINK_START_DATE')), 'yyyy-MM-dd')))
#         promo_DF=promo_DF.withColumn('SMA_LINK_END_DATE', to_date(date_format(date_func(col('SMA_LINK_END_DATE')), 'yyyy-MM-dd')))

#       except Exception as ex:
#         ABC(productRecallinvalidRecCount = '')
#         loggerAtt.error(ex)
#         err = ErrorReturn('Error', ex,'PromotionLink error on start date/end date check')
#         errJson = jsonpickle.encode(err)
#         errJson = json.loads(errJson)
#         dbutils.notebook.exit(Merge(ABCChecks,errJson))
      
#       if promo_DF != None:
#         if promo_DF.count() > 0:
#           ## Step 3: Active Promo fetch
#           try:
#             promo_DF= promo_DF.na.drop()

#             promo_DF=promo_DF.withColumn('SMA_LINK_SYS_DIGIT',lit(None).cast(StringType())) 
            
#             loggerAtt.info(f"No of promotion link records for insert/update: {promo_DF.count()}")

#             temp_table_name = 'Promotion_temp'
#             promo_DF.createOrReplaceTempView(temp_table_name)

#             loggerAtt.info(f"Promotion_temp table created from source file with count:{promo_DF.count()}")
#           except Exception as ex:
#             ABC(activePromoCount = '')
#             loggerAtt.error(ex)
#             err = ErrorReturn('Error', ex,'PromotionLink active promo fetch setting')
#             errJson = jsonpickle.encode(err)
#             errJson = json.loads(errJson)
#             dbutils.notebook.exit(Merge(ABCChecks,errJson))
          
#           try:
#             temp = spark.sql('''select SMA_PROMO_LINK_UPC, SMA_STORE, collect_set(SMA_LINK_HDR_COUPON) as coupon_list, count(*) as count from 
#                                       (select Promotion_temp.* from delta.`{}` as Promotion_linking join Promotion_temp  
#                                        ON Promotion_linking.SMA_PROMO_LINK_UPC = Promotion_temp.SMA_PROMO_LINK_UPC 
#                                        AND Promotion_linking.SMA_STORE= Promotion_temp.SMA_STORE 
#                                        AND (Promotion_temp.SMA_ITM_EFF_DATE BETWEEN Promotion_linking.SMA_LINK_START_DATE AND Promotion_linking.SMA_LINK_END_DATE)) 
#                                        group by SMA_PROMO_LINK_UPC, SMA_STORE having count > 1'''.format(PromotionLinkDeltaPath))
#             temp_table_name = "temp_id"
#             temp.createOrReplaceTempView(temp_table_name)
#           except Exception as ex:
#             ABC(activePromoCount = '')
#             loggerAtt.error(ex)
#             err = ErrorReturn('Error', ex,'Fetching all problem records')
#             errJson = jsonpickle.encode(err)
#             errJson = json.loads(errJson)
#             dbutils.notebook.exit(Merge(ABCChecks,errJson))
            
#           try:
#             ABC(promo_DFBeforeCnt=promo_DF.count())
#             promo_DF = spark.sql('''select Promotion_temp.* from Promotion_temp LEFT ANTI JOIN temp_id
#                                 ON temp_id.SMA_PROMO_LINK_UPC = Promotion_temp.SMA_PROMO_LINK_UPC 
#                                 AND temp_id.SMA_STORE = Promotion_temp.SMA_STORE 
#                                 AND Promotion_temp.SMA_LINK_HDR_COUPON in 
#                                     (temp_id.coupon_list[0], temp_id.coupon_list[1], temp_id.coupon_list[2], temp_id.coupon_list[3], temp_id.coupon_list[4], temp_id.coupon_list[5], temp_id.coupon_list[6])'''.format(PromotionLinkDeltaPath))
#             ABC(promo_DFAfterCnt=promo_DF.count())
#             temp_table_name = "Promotion_temp"
#             promo_DF.createOrReplaceTempView(temp_table_name)
#           except Exception as ex:
#             ABC(promo_DFAfterCnt='')
#             ABC(promo_DFBeforeCnt='')
#             loggerAtt.error(ex)
#             err = ErrorReturn('Error', ex,'Removing error records')
#             errJson = jsonpickle.encode(err)
#             errJson = json.loads(errJson)
#             dbutils.notebook.exit(Merge(ABCChecks,errJson))
            
            
#           try:
#             ABC(tempCnt=temp.count())
#             Invalid_RecordsPath = Invalid_RecordsPath + "/" +Date+ "/" + "duplicates_PromoLink"
#             writeInvalidRecord(temp, Invalid_RecordsPath, Date)
#           except Exception as ex:
#             ABC(tempCnt='')
#             loggerAtt.error(ex)
#             err = ErrorReturn('Error', ex,'Write error records')
#             errJson = jsonpickle.encode(err)
#             errJson = json.loads(errJson)
#             dbutils.notebook.exit(Merge(ABCChecks,errJson))
          
#           try:
#             if promo_DF.count() > 0:
#               active_promos=spark.sql('''SELECT pt.SMA_LINK_HDR_COUPON, 
#                                                 pt.SMA_LINK_HDR_LOCATION, 
#                                                 pt.SMA_STORE, 
#                                                 pt.SMA_CHG_TYPE, 
#                                                 pt.SMA_LINK_START_DATE, 
#                                                 pt.SMA_LINK_RCRD_TYPE,
#                                                 pt.SMA_LINK_END_DATE,
#                                                 pt.SMA_ITM_EFF_DATE,
#                                                 pt.SMA_LINK_CHG_TYPE,
#                                                 pt.SMA_PROMO_LINK_UPC,
#                                                 pt.LOCATION_TYPE,
#                                                 pt.MERCH_TYPE,
#                                                 pt.SMA_LINK_HDR_MAINT_TYPE,
#                                                 pt.SMA_LINK_ITEM_NBR,
#                                                 pt.SMA_LINK_OOPS_ADWK,
#                                                 pt.SMA_LINK_OOPS_FILE_ID,
#                                                 pt.SMA_LINK_TYPE,
#                                                 pt.SMA_LINK_SYS_DIGIT,
#                                                 pt.SMA_LINK_FAMCD_PROMOCD,
#                                                 pt.SMA_BATCH_SERIAL_NBR,
#                                                 pl.INSERT_ID,
#                                                 pl.INSERT_TIMESTAMP,
#                                                 pt.LAST_UPDATE_ID,
#                                                 pt.LAST_UPDATE_TIMESTAMP
#                                             ,DENSE_RANK() OVER   
#                                             (PARTITION BY pt.SMA_STORE, pt.SMA_PROMO_LINK_UPC, pt.SMA_LINK_HDR_COUPON ORDER BY pl.SMA_LINK_START_DATE desc, pl.INSERT_TIMESTAMP desc) AS Rank  
#                                         FROM (select * from delta.`{}`) pl  
#                                         INNER JOIN Promotion_temp pt
#                                           ON pl.SMA_PROMO_LINK_UPC = pt.SMA_PROMO_LINK_UPC 
#                                           AND pl.SMA_STORE= pt.SMA_STORE 
#                                           AND pl.SMA_LINK_HDR_COUPON=pt.SMA_LINK_HDR_COUPON
#                                           AND (pt.SMA_ITM_EFF_DATE between pl.SMA_LINK_START_DATE AND pl.SMA_LINK_END_DATE)'''.format(PromotionLinkDeltaPath))
#             active_promos = active_promos.filter((col('Rank') == 1))
#             active_promos = active_promos.drop(col('Rank'))
#             loggerAtt.info(f"Active Promo before filter Count: {active_promos.count()}")
# #             if processing_file =='Delta':
# #               active_promos = active_promos.filter(((col('SMA_PROMO_LINK_UPC') != 81234902032) & (col('SMA_STORE') != 6662)))
#             loggerAtt.info(f"Total Input Promo Count: {promo_DF.count()}")
#             loggerAtt.info(f"Active Promo Count: {active_promos.count()}")
#             ABC(activePromoCount = active_promos.count())
#           except Exception as ex:
#             ABC(activePromoCount = '')
#             loggerAtt.error(ex)
#             err = ErrorReturn('Error', ex,'PromotionLink active promo fetch')
#             errJson = jsonpickle.encode(err)
#             errJson = json.loads(errJson)
#             dbutils.notebook.exit(Merge(ABCChecks,errJson))
          
# #           if active_promos is not None:
# #             if active_promos.count()>0:
# #               dupActiveValueCnt = 0
# #               try:
# #                 ABC(activePromoBeforeCnt=active_promos.count())
# #                 ABC(dupActiveValueCntCheck = 1)
# #                 dup_active_promos = active_promos.groupBy('SMA_PROMO_LINK_UPC','SMA_STORE', 'SMA_LINK_HDR_COUPON').count().filter(col('count') > 1)
# #                 dup_active_promos = dup_active_promos.drop(dup_active_promos['count'])
# #                 dup_active_promos = (active_promos.join(dup_active_promos,['SMA_PROMO_LINK_UPC','SMA_STORE', 'SMA_LINK_HDR_COUPON'], "leftsemi"))

# #                 dupActiveValueCnt = dup_active_promos.count()
# #                 loggerAtt.info("Duplicate Record Count: ("+str(dupActiveValueCnt)+"," +str(len(dup_active_promos.columns))+")")
# #                 ABC(dupActiveValueCnt=dupActiveValueCnt)
# #                 active_promos = (active_promos.join(dup_active_promos,['SMA_PROMO_LINK_UPC', 'SMA_STORE', 'SMA_LINK_HDR_COUPON'], "leftanti"))
# #                 display(dup_active_promos)
# #                 ABC(activePromoAfterCnt=active_promos.count())
# #               except Exception as ex:
# #                 loggerAtt.error(ex)
# #                 err = ErrorReturn('Error', ex,'active promo duplicate check')
# #                 ABC(dupActiveValueCntCheck = '')
# #                 ABC(dupActiveValueCnt = '')
# #                 errJson = jsonpickle.encode(err)
# #                 errJson = json.loads(errJson)
# #                 dbutils.notebook.exit(Merge(ABCChecks,errJson))
          
#           ## Step 4: Promo processing
#           if active_promos is not None:
#             if active_promos.count()>0:
#               try:
#                 ABC(activePromoUpdateCheck = 1)
#                 activePromoUpdateSameCoupon(active_promos)
#               except Exception as ex:
#                 loggerAtt.error(ex)
#                 err = ErrorReturn('Error', ex,'activePromoUpdate same coupon')
#                 ABC(activePromoUpdateCheck = '')
#                 errJson = jsonpickle.encode(err)
#                 errJson = json.loads(errJson)
#                 dbutils.notebook.exit(Merge(ABCChecks,errJson))
                
#               try:
#                 ABC(activePromoUpdateCheck = 1)
#                 activePromoUpdateDiffCoupon(active_promos)
#               except Exception as ex:
#                 loggerAtt.error(ex)
#                 err = ErrorReturn('Error', ex,'activePromoUpdate different coupon')
#                 ABC(activePromoUpdateCheck = '')
#                 errJson = jsonpickle.encode(err)
#                 errJson = json.loads(errJson)
#                 dbutils.notebook.exit(Merge(ABCChecks,errJson))                
                
#             else:
#                 loggerAtt.info("UPDATING FOR B2B and NO EXISTING PROMOTION LINK FOR ITEM-CPN COMBINATION")
#                 try:
#                   ABC(inactivePromoUpdate = 1)
#                   updateNonActivePromo(PromotionLinkDeltaPath)
#                 except Exception as ex:
#                   loggerAtt.error(ex)
#                   err = ErrorReturn('Error', ex,'updateNonActivePromo')
#                   ABC(inactivePromoUpdate = '')
#                   errJson = jsonpickle.encode(err)
#                   errJson = json.loads(errJson)
#                   dbutils.notebook.exit(Merge(ABCChecks,errJson))
#           else:
#             loggerAtt.info("no active promos")
#             try:
#               ABC(inactivePromoUpdate = 1)
#               updateNonActivePromo(PromotionLinkDeltaPath)
#             except Exception as ex:
#               loggerAtt.error(ex)
#               err = ErrorReturn('Error', ex,'updateNonActivePromo')
#               ABC(inactivePromoUpdate = '')
#               errJson = jsonpickle.encode(err)
#               errJson = json.loads(errJson)
#               dbutils.notebook.exit(Merge(ABCChecks,errJson))

#           if active_promos is not None:
#             if active_promos.count() > 0:
#               try:
#                 ABC(fetchNonActivePromoRecordsCheck = 1)
#                 promo_DF = fetchNonActivePromoRecords(active_promos, promo_DF)
#               except Exception as ex:
#                 loggerAtt.error(ex)
#                 err = ErrorReturn('Error', ex,'fetchNonActivePromoRecords')
#                 ABC(fetchNonActivePromoRecordsCheck = '')
#                 errJson = jsonpickle.encode(err)
#                 errJson = json.loads(errJson)
#                 dbutils.notebook.exit(Merge(ABCChecks,errJson))

#               try:
#                 ABC(insertPromoRecordsCheck = 1)
#                 insertPromoRecords(promo_DF, promoInvalidRecordsCount)
#               except Exception as ex:
#                 loggerAtt.error(ex)
#                 err = ErrorReturn('Error', ex,'insertPromoRecords')
#                 ABC(insertPromoRecordsCheck = 0)
#                 ABC(promoLinkingFinalCount='')
#                 ABC(promoLinkingInitCount='')
#                 errJson = jsonpickle.encode(err)
#                 errJson = json.loads(errJson)
#                 dbutils.notebook.exit(Merge(ABCChecks,errJson))
#             else:
#               try:
#                 ABC(insertPromoRecordsCheck = 1)
#                 insertPromoRecords(promo_DF, promoInvalidRecordsCount)
#               except Exception as ex:
#                 loggerAtt.error(ex)
#                 err = ErrorReturn('Error', ex,'insertPromoRecords')
#                 ABC(insertPromoRecordsCheck = 0)
#                 ABC(promoLinkingFinalCount='')
#                 ABC(promoLinkingInitCount='')
#                 errJson = jsonpickle.encode(err)
#                 errJson = json.loads(errJson)
#                 dbutils.notebook.exit(Merge(ABCChecks,errJson))
#           else:
#             try:
#               ABC(insertPromoRecordsCheck = 1)
#               insertPromoRecords(promo_DF, promoInvalidRecordsCount)
#             except Exception as ex:
#               loggerAtt.error(ex)
#               err = ErrorReturn('Error', ex,'insertPromoRecords')
#               ABC(insertPromoRecordsCheck = 0)
#               ABC(promoLinkingFinalCount='')
#               ABC(promoLinkingInitCount='')
#               errJson = jsonpickle.encode(err)
#               errJson = json.loads(errJson)
#               dbutils.notebook.exit(Merge(ABCChecks,errJson))

# COMMAND ----------

