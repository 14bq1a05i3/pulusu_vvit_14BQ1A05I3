# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Find items in item delta table

# COMMAND ----------

def findItemsInItemTable(item_transformed_df):
  temp_table_name = "validitem_df"
  item_transformed_df.createOrReplaceTempView(temp_table_name)
  
  loggerAtt.info(f"Input file count going for left join : {item_transformed_df.count()}")
  
  item_transformed_df = spark.sql('''SELECT hostItemExist.exist, item_df.*, hostItemExist.SMA_ITEM_STATUS as deltaSTATUS 
                              FROM  validitem_df item_df 
                                LEFT JOIN (SELECT "Y" as exist,hostItem.SMA_GTIN_NUM, hostItem.SMA_STORE, deltaItem.SMA_ITEM_STATUS 
                                  FROM validitem_df hostItem join delta.`{}` deltaItem 
                                    WHERE deltaItem.SMA_GTIN_NUM = hostItem.SMA_GTIN_NUM 
                                    AND deltaItem.SMA_STORE = hostItem.SMA_STORE) AS hostItemExist 
                                ON item_df.SMA_GTIN_NUM = hostItemExist.SMA_GTIN_NUM 
                                  AND item_df.SMA_STORE = hostItemExist.SMA_STORE'''.format(ItemMainDeltaPath))
  itemsNotInItemTable = item_transformed_df.filter((col('exist').isNull()))
  item_transformed_df = item_transformed_df.filter((col('exist').isNotNull()))
  
  loggerAtt.info(f"Items in Delta Table: {item_transformed_df.count()}")
  loggerAtt.info(f"Items not in Delta Table: {itemsNotInItemTable.count()}")
  
  return item_transformed_df, itemsNotInItemTable

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC #### Update Item table

# COMMAND ----------

def updateItemRecord(itemsInItemTable):
  loggerAtt.info("Update into Item Main Delta table initiated")
  temp_table_name = "itemsInItemTableProcessing"

  itemsInItemTable.createOrReplaceTempView(temp_table_name)
  
  initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(ItemMainDeltaPath))
  loggerAtt.info(f"Initial count of records in Item Main Delta Table: {initial_recs.head(1)}")
  initial_recs = initial_recs.head(1)
  ABC(ItemMainInitCount=initial_recs[0][0])
  
  spark.sql('''MERGE INTO delta.`{}` as Item 
      USING itemsInItemTableProcessing 
      ON Item.SMA_GTIN_NUM = itemsInItemTableProcessing.SMA_GTIN_NUM and Item.SMA_STORE= itemsInItemTableProcessing.SMA_STORE
      WHEN MATCHED Then 
              Update Set  Item.SMA_DEST_STORE = itemsInItemTableProcessing.SMA_DEST_STORE,
                          Item.ITEM_PARENT = itemsInItemTableProcessing.ITEM_PARENT,
                          Item.SMA_SUB_DEPT = itemsInItemTableProcessing.SMA_SUB_DEPT,
                          Item.GROUP_NO = itemsInItemTableProcessing.GROUP_NO,
                          Item.SMA_COMM = itemsInItemTableProcessing.SMA_COMM,
                          Item.SMA_SUB_COMM = itemsInItemTableProcessing.SMA_SUB_COMM,
                          Item.SMA_DATA_TYPE = itemsInItemTableProcessing.SMA_DATA_TYPE,
                          Item.SMA_DEMANDTEC_PROMO_ID = itemsInItemTableProcessing.SMA_DEMANDTEC_PROMO_ID,
                          Item.SMA_BOTTLE_DEPOSIT_IND = itemsInItemTableProcessing.SMA_BOTTLE_DEPOSIT_IND,
                          Item.SMA_POS_SYSTEM = itemsInItemTableProcessing.SMA_POS_SYSTEM,
                          Item.SMA_SEGMENT = itemsInItemTableProcessing.SMA_SEGMENT,
                          Item.SMA_ITEM_DESC = itemsInItemTableProcessing.SMA_ITEM_DESC,
                          Item.SMA_ITEM_MVMT = itemsInItemTableProcessing.SMA_ITEM_MVMT,
                          Item.SMA_ITEM_POG_COMM = itemsInItemTableProcessing.SMA_ITEM_POG_COMM,
                          Item.SMA_UPC_DESC = itemsInItemTableProcessing.SMA_UPC_DESC,
                          Item.SMA_RETL_MULT_UOM = itemsInItemTableProcessing.SMA_RETL_MULT_UOM,
                          Item.SMA_SBW_IND = itemsInItemTableProcessing.SMA_SBW_IND,
                          Item.SMA_PRIME_UPC = itemsInItemTableProcessing.SMA_PRIME_UPC,
                          Item.SMA_ITEM_SIZE = itemsInItemTableProcessing.SMA_ITEM_SIZE,
                          Item.SMA_ITEM_UOM = itemsInItemTableProcessing.SMA_ITEM_UOM,
                          Item.SMA_ITEM_BRAND = itemsInItemTableProcessing.SMA_ITEM_BRAND,
                          Item.SMA_ITEM_TYPE = itemsInItemTableProcessing.SMA_ITEM_TYPE,
                          Item.SMA_VEND_COUPON_FAM1 = itemsInItemTableProcessing.SMA_VEND_COUPON_FAM1,
                          Item.SMA_VEND_COUPON_FAM2 = itemsInItemTableProcessing.SMA_VEND_COUPON_FAM2,
                          Item.SMA_FSA_IND = itemsInItemTableProcessing.SMA_FSA_IND,
                          Item.SMA_NAT_ORG_IND = itemsInItemTableProcessing.SMA_NAT_ORG_IND,
                          Item.SMA_NEW_ITMUPC = itemsInItemTableProcessing.SMA_NEW_ITMUPC,
                          Item.ATC = itemsInItemTableProcessing.ATC,
                          Item.SMA_PRIV_LABEL = itemsInItemTableProcessing.SMA_PRIV_LABEL,
                          Item.SMA_TAG_SZ = itemsInItemTableProcessing.SMA_TAG_SZ,
                          Item.SMA_FUEL_PROD_CATEG = itemsInItemTableProcessing.SMA_FUEL_PROD_CATEG,
                          Item.SMA_CONTRIB_QTY = itemsInItemTableProcessing.SMA_CONTRIB_QTY,
                          Item.SMA_CONTRIB_QTY_UOM = itemsInItemTableProcessing.SMA_CONTRIB_QTY_UOM,
                          Item.SMA_GAS_IND = itemsInItemTableProcessing.SMA_GAS_IND,
                          Item.SMA_ITEM_STATUS = itemsInItemTableProcessing.SMA_ITEM_STATUS,
                          Item.SMA_STATUS_DATE = itemsInItemTableProcessing.SMA_STATUS_DATE,
                          Item.SMA_VEND_NUM = itemsInItemTableProcessing.SMA_VEND_NUM,
                          Item.SMA_SOURCE_METHOD = itemsInItemTableProcessing.SMA_SOURCE_METHOD,
                          Item.SMA_SOURCE_WHSE = itemsInItemTableProcessing.SMA_SOURCE_WHSE,
                          Item.SMA_RECV_TYPE = itemsInItemTableProcessing.SMA_RECV_TYPE,
                          Item.SMA_MEAS_OF_EACH = itemsInItemTableProcessing.SMA_MEAS_OF_EACH,
                          Item.SMA_MEAS_OF_EACH_WIP = itemsInItemTableProcessing.SMA_MEAS_OF_EACH_WIP,
                          Item.SMA_MEAS_OF_PRICE = itemsInItemTableProcessing.SMA_MEAS_OF_PRICE,
                          Item.SMA_MKT_AREA = itemsInItemTableProcessing.SMA_MKT_AREA,
                          Item.SMA_UOM_OF_PRICE = itemsInItemTableProcessing.SMA_UOM_OF_PRICE,
                          Item.SMA_VENDOR_PACK = itemsInItemTableProcessing.SMA_VENDOR_PACK,
                          Item.SMA_SHELF_TAG_REQ = itemsInItemTableProcessing.SMA_SHELF_TAG_REQ,
                          Item.SMA_QTY_KEY_OPTIONS = itemsInItemTableProcessing.SMA_QTY_KEY_OPTIONS,
                          Item.SMA_MANUAL_PRICE_ENTRY = itemsInItemTableProcessing.SMA_MANUAL_PRICE_ENTRY,
                          Item.SMA_FOOD_STAMP_IND = itemsInItemTableProcessing.SMA_FOOD_STAMP_IND,
                          Item.SMA_HIP_IND = itemsInItemTableProcessing.SMA_HIP_IND,
                          Item.SMA_WIC_IND = itemsInItemTableProcessing.SMA_WIC_IND,
                          Item.SMA_TARE_PCT = itemsInItemTableProcessing.SMA_TARE_PCT,
                          Item.SMA_FIXED_TARE_WGT = itemsInItemTableProcessing.SMA_FIXED_TARE_WGT,
                          Item.SMA_TARE_UOM = itemsInItemTableProcessing.SMA_TARE_UOM,
                          Item.SMA_POINTS_ELIGIBLE = itemsInItemTableProcessing.SMA_POINTS_ELIGIBLE,
                          Item.SMA_RECALL_FLAG = itemsInItemTableProcessing.SMA_RECALL_FLAG,
                          Item.SMA_TAX_CHG_IND = itemsInItemTableProcessing.SMA_TAX_CHG_IND,
                          Item.SMA_RESTR_CODE = itemsInItemTableProcessing.SMA_RESTR_CODE,
                          Item.SMA_WIC_SHLF_MIN = itemsInItemTableProcessing.SMA_WIC_SHLF_MIN,
                          Item.SMA_WIC_ALT = itemsInItemTableProcessing.SMA_WIC_ALT,
                          Item.SMA_TAX_1 = itemsInItemTableProcessing.SMA_TAX_1,
                          Item.SMA_TAX_2 = itemsInItemTableProcessing.SMA_TAX_2,
                          Item.SMA_TAX_3 = itemsInItemTableProcessing.SMA_TAX_3,
                          Item.SMA_TAX_4 = itemsInItemTableProcessing.SMA_TAX_4,
                          Item.SMA_TAX_5 = itemsInItemTableProcessing.SMA_TAX_5,
                          Item.SMA_TAX_6 = itemsInItemTableProcessing.SMA_TAX_6,
                          Item.SMA_TAX_7 = itemsInItemTableProcessing.SMA_TAX_7,
                          Item.SMA_TAX_8 = itemsInItemTableProcessing.SMA_TAX_8,
                          Item.SMA_DISCOUNTABLE_IND = itemsInItemTableProcessing.SMA_DISCOUNTABLE_IND,
                          Item.SMA_CPN_MLTPY_IND = itemsInItemTableProcessing.SMA_CPN_MLTPY_IND,
                          Item.SMA_MULT_UNIT_RETL = itemsInItemTableProcessing.SMA_MULT_UNIT_RETL,
                          Item.SMA_RETL_MULT_UNIT = itemsInItemTableProcessing.SMA_RETL_MULT_UNIT,
                          Item.SMA_LINK_UPC = itemsInItemTableProcessing.SMA_LINK_UPC,
                          Item.SMA_ACTIVATION_CODE = itemsInItemTableProcessing.SMA_ACTIVATION_CODE,
                          Item.SMA_GLUTEN_FREE = itemsInItemTableProcessing.SMA_GLUTEN_FREE,
                          Item.SMA_NON_PRICED_ITEM = itemsInItemTableProcessing.SMA_NON_PRICED_ITEM,
                          Item.SMA_ORIG_CHG_TYPE = itemsInItemTableProcessing.SMA_ORIG_CHG_TYPE,
                          Item.SMA_ORIG_LIN = itemsInItemTableProcessing.SMA_ORIG_LIN,
                          Item.SMA_ORIG_VENDOR = itemsInItemTableProcessing.SMA_ORIG_VENDOR,
                          Item.SMA_IFPS_CODE = itemsInItemTableProcessing.SMA_IFPS_CODE,
                          Item.SMA_IN_STORE_TAG_PRINT = itemsInItemTableProcessing.SMA_IN_STORE_TAG_PRINT,
                          Item.SMA_BATCH_SERIAL_NBR = itemsInItemTableProcessing.SMA_BATCH_SERIAL_NBR,
                          Item.SMA_BATCH_SUB_DEPT = itemsInItemTableProcessing.SMA_BATCH_SUB_DEPT,
                          Item.SMA_EFF_DOW = itemsInItemTableProcessing.SMA_EFF_DOW,
                          Item.SMA_SMR_EFF_DATE = itemsInItemTableProcessing.SMA_SMR_EFF_DATE,
                          Item.SMA_STORE_DIV = itemsInItemTableProcessing.SMA_STORE_DIV,
                          Item.SMA_STORE_STATE = itemsInItemTableProcessing.SMA_STORE_STATE,
                          Item.SMA_STORE_ZONE = itemsInItemTableProcessing.SMA_STORE_ZONE,
                          Item.SMA_TAX_CATEG = itemsInItemTableProcessing.SMA_TAX_CATEG,
                          Item.SMA_TIBCO_DATE = itemsInItemTableProcessing.SMA_TIBCO_DATE,
                          Item.SMA_TRANSMIT_DATE = itemsInItemTableProcessing.SMA_TRANSMIT_DATE,
                          Item.SMA_UNIT_PRICE_CODE = itemsInItemTableProcessing.SMA_UNIT_PRICE_CODE,
                          Item.SMA_WPG = itemsInItemTableProcessing.SMA_WPG,
                          Item.SMA_EMERGY_UPDATE_IND = itemsInItemTableProcessing.SMA_EMERGY_UPDATE_IND,
                          Item.SMA_CHG_TYPE = itemsInItemTableProcessing.SMA_CHG_TYPE,
                          Item.SMA_RETL_VENDOR = itemsInItemTableProcessing.SMA_RETL_VENDOR,
                          Item.SMA_POS_REQUIRED = itemsInItemTableProcessing.SMA_POS_REQUIRED,
                          Item.SMA_TAG_REQUIRED = itemsInItemTableProcessing.SMA_TAG_REQUIRED,
                          Item.SMA_UPC_OVERRIDE_GRP_NUM = itemsInItemTableProcessing.SMA_UPC_OVERRIDE_GRP_NUM,
                          Item.SMA_TENDER_RESTRICTION_CODE = itemsInItemTableProcessing.SMA_TENDER_RESTRICTION_CODE,
                          Item.SMA_REFUND_RECEIPT_EXCLUSION = itemsInItemTableProcessing.SMA_REFUND_RECEIPT_EXCLUSION,
                          Item.SMA_FEE_ITEM_TYPE = itemsInItemTableProcessing.SMA_FEE_ITEM_TYPE,
                          Item.SMA_FEE_TYPE = itemsInItemTableProcessing.SMA_FEE_TYPE,
                          Item.SMA_KOSHER_FLAG = itemsInItemTableProcessing.SMA_KOSHER_FLAG,
                          Item.SMA_LEGACY_ITEM = itemsInItemTableProcessing.SMA_LEGACY_ITEM,
                          Item.SMA_GUIDING_STARS = itemsInItemTableProcessing.SMA_GUIDING_STARS,
                          Item.SMA_COMP_ID = itemsInItemTableProcessing.SMA_COMP_ID,
                          Item.SMA_COMP_OBSERV_DATE = itemsInItemTableProcessing.SMA_COMP_OBSERV_DATE,
                          Item.SMA_COMP_PRICE = itemsInItemTableProcessing.SMA_COMP_PRICE,
                          Item.SMA_RETL_CHG_TYPE = itemsInItemTableProcessing.SMA_RETL_CHG_TYPE,
                          Item.PROMO_MIN_REQ_QTY = itemsInItemTableProcessing.PROMO_MIN_REQ_QTY,
                          Item.PROMO_LIM_VAL = itemsInItemTableProcessing.PROMO_LIM_VAL,
                          Item.PROMO_BUY_QTY = itemsInItemTableProcessing.PROMO_BUY_QTY,
                          Item.PROMO_GET_QTY = itemsInItemTableProcessing.PROMO_GET_QTY,
                          Item.HOW_TO_SELL = itemsInItemTableProcessing.HOW_TO_SELL,
                          Item.BRAND_LOW = itemsInItemTableProcessing.BRAND_LOW,
                          Item.MANUFACTURER = itemsInItemTableProcessing.MANUFACTURER,
                          Item.INNER_PACK_SIZE = itemsInItemTableProcessing.INNER_PACK_SIZE,
                          Item.AVG_WGT = itemsInItemTableProcessing.AVG_WGT,
                          Item.EFF_TS = itemsInItemTableProcessing.EFF_TS,
                          Item.LAST_UPDATE_ID = itemsInItemTableProcessing.LAST_UPDATE_ID,
                          Item.LAST_UPDATE_TIMESTAMP = itemsInItemTableProcessing.LAST_UPDATE_TIMESTAMP'''.format(ItemMainDeltaPath))
    
  appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(ItemMainDeltaPath))
  loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
  appended_recs = appended_recs.head(1)
  ABC(ItemMainFinalCount=appended_recs[0][0])

  spark.catalog.dropTempView(temp_table_name)
  loggerAtt.info("Merge into Item Main Delta table successful")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Insert into Item table

# COMMAND ----------

def insertItemRecord(itemsNotInItemTableInsert):
  loggerAtt.info("Append/Insert into Delta table initiated")
  temp_table_name = "itemsNotInItemTableInsert"
  itemsNotInItemTableInsert.createOrReplaceTempView(temp_table_name)
  
  initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(ItemMainDeltaPath))
  loggerAtt.info(f"Initial count of records in Item Main Delta Table: {initial_recs.head(1)}")
  initial_recs = initial_recs.head(1)
  ABC(ItemMainInitCount=initial_recs[0][0])
  
  spark.sql('''INSERT INTO delta.`{}` 
                SELECT SMA_STORE ,
                SMA_DEST_STORE ,
                SMA_GTIN_NUM ,
                ITEM_PARENT ,
                SMA_SUB_DEPT ,
                GROUP_NO ,
                SMA_COMM ,
                SMA_SUB_COMM ,
                SMA_DATA_TYPE ,
                SMA_DEMANDTEC_PROMO_ID ,
                SMA_BOTTLE_DEPOSIT_IND ,
                SMA_POS_SYSTEM ,
                SMA_SEGMENT ,
                SMA_HLTHY_IDEAS,
                SMA_ITEM_DESC ,
                SMA_ITEM_MVMT ,
                SMA_ITEM_POG_COMM ,
                SMA_UPC_DESC ,
                SMA_RETL_MULT_UOM ,
                SMA_SBW_IND ,
                SMA_PRIME_UPC ,
                SMA_ITEM_SIZE ,
                SMA_ITEM_UOM ,
                SMA_ITEM_BRAND ,
                SMA_ITEM_TYPE ,
                SMA_VEND_COUPON_FAM1 ,
                SMA_VEND_COUPON_FAM2 ,
                SMA_FSA_IND ,
                SMA_NAT_ORG_IND ,
                SMA_NEW_ITMUPC ,
                ATC ,
                SMA_PRIV_LABEL ,
                SMA_TAG_SZ ,
                SMA_FUEL_PROD_CATEG ,
                SMA_CONTRIB_QTY ,
                SMA_CONTRIB_QTY_UOM ,
                SMA_GAS_IND ,
                SMA_ITEM_STATUS ,
                SMA_STATUS_DATE ,
                SMA_VEND_NUM ,
                SMA_SOURCE_METHOD ,
                SMA_SOURCE_WHSE ,
                SMA_RECV_TYPE ,
                SMA_MEAS_OF_EACH ,
                SMA_MEAS_OF_EACH_WIP ,
                SMA_MEAS_OF_PRICE ,
                SMA_MKT_AREA ,
                SMA_UOM_OF_PRICE ,
                SMA_VENDOR_PACK ,
                SMA_SHELF_TAG_REQ ,
                SMA_QTY_KEY_OPTIONS ,
                SMA_MANUAL_PRICE_ENTRY ,
                SMA_FOOD_STAMP_IND ,
                SMA_HIP_IND ,
                SMA_WIC_IND ,
                SMA_TARE_PCT ,
                SMA_FIXED_TARE_WGT ,
                SMA_TARE_UOM ,
                SMA_POINTS_ELIGIBLE ,
                SMA_RECALL_FLAG ,
                SMA_TAX_CHG_IND ,
                SMA_RESTR_CODE ,
                SMA_WIC_SHLF_MIN ,
                SMA_WIC_ALT ,
                SMA_TAX_1 ,
                SMA_TAX_2 ,
                SMA_TAX_3 ,
                SMA_TAX_4 ,
                SMA_TAX_5 ,
                SMA_TAX_6 ,
                SMA_TAX_7 ,
                SMA_TAX_8 ,
                SMA_DISCOUNTABLE_IND ,
                SMA_CPN_MLTPY_IND ,
                SMA_MULT_UNIT_RETL ,
                SMA_RETL_MULT_UNIT ,
                SMA_LINK_UPC ,
                SMA_ACTIVATION_CODE ,
                SMA_GLUTEN_FREE ,
                SMA_NON_PRICED_ITEM ,
                SMA_ORIG_CHG_TYPE ,
                SMA_ORIG_LIN ,
                SMA_ORIG_VENDOR ,
                SMA_IFPS_CODE ,
                SMA_IN_STORE_TAG_PRINT ,
                SMA_BATCH_SERIAL_NBR ,
                SMA_BATCH_SUB_DEPT ,
                SMA_EFF_DOW ,
                SMA_SMR_EFF_DATE ,
                SMA_STORE_DIV ,
                SMA_STORE_STATE ,
                SMA_STORE_ZONE ,
                SMA_TAX_CATEG ,
                SMA_TIBCO_DATE ,
                SMA_TRANSMIT_DATE ,
                SMA_UNIT_PRICE_CODE ,
                SMA_WPG ,
                SMA_EMERGY_UPDATE_IND ,
                SMA_CHG_TYPE ,
                SMA_RETL_VENDOR ,
                SMA_POS_REQUIRED ,
                SMA_TAG_REQUIRED ,
                SMA_UPC_OVERRIDE_GRP_NUM ,
                SMA_TENDER_RESTRICTION_CODE ,
                SMA_REFUND_RECEIPT_EXCLUSION ,
                SMA_FEE_ITEM_TYPE ,
                SMA_FEE_TYPE ,
                SMA_KOSHER_FLAG ,
                SMA_LEGACY_ITEM ,
                SMA_GUIDING_STARS ,
                SMA_COMP_ID ,
                SMA_COMP_OBSERV_DATE ,
                SMA_COMP_PRICE ,
                SMA_RETL_CHG_TYPE ,
                PROMO_MIN_REQ_QTY ,
                PROMO_LIM_VAL ,
                PROMO_BUY_QTY ,
                PROMO_GET_QTY ,
                HOW_TO_SELL ,
                BRAND_LOW ,
                MANUFACTURER ,
                INNER_PACK_SIZE ,
                AVG_WGT ,
                EFF_TS ,
                INSERT_ID ,
                INSERT_TIMESTAMP ,
                LAST_UPDATE_ID ,
                LAST_UPDATE_TIMESTAMP FROM itemsNotInItemTableInsert'''.format(ItemMainDeltaPath))
  appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(ItemMainDeltaPath))
  loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
  appended_recs = appended_recs.head(1)
  ABC(ItemMainFinalCount=appended_recs[0][0])


  spark.catalog.dropTempView(temp_table_name)
  loggerAtt.info("Insert into Item Main Delta table successful")

# COMMAND ----------

def productRecall(itemsInItemTable):
  nonItemRecallFile = itemsInItemTable.filter(((col("SMA_CHG_TYPE")!= 12) & (col("SMA_CHG_TYPE")!= 13))) # invalid items?
  itemsInItemTable = itemsInItemTable.filter(((col('SMA_CHG_TYPE')==12) | (col('SMA_CHG_TYPE')==13)))
      
  if itemsInItemTable.count() >0:
    
    loggerAtt.info(f"Item not equal chgtype 12, 13: {nonItemRecallFile.count()}")
    loggerAtt.info(f"Item with chgtype 12, 13: {itemsInItemTable.count()}")
    
    initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(ItemMainDeltaPath))
    loggerAtt.info(f"Initial count of records in Promotion Main Delta Table: {initial_recs.head(1)}")
    initial_recs = initial_recs.head(1)
    ABC(productRecallInitCount=initial_recs[0][0])
    
    loggerAtt.info("Item Product Recall initiated")
    temp_table_name = "item_recall_df"
    itemsInItemTable.createOrReplaceTempView(temp_table_name)

    spark.sql('''MERGE INTO delta.`{}` as Item_Delta USING item_recall_df  
                   ON  item_recall_df.SMA_GTIN_NUM = Item_Delta.SMA_GTIN_NUM 
                   AND item_recall_df.SMA_STORE = Item_Delta.SMA_STORE 
                   AND item_recall_df.SMA_POS_REQUIRED= 'Y' 
                   AND item_recall_df.SMA_TAG_REQUIRED= 'N'  
                   AND item_recall_df.SMA_CHG_TYPE IN (12,13)
                 WHEN MATCHED THEN 
                   Update Set 
                     Item_Delta.SMA_RECALL_FLAG  = item_recall_df.SMA_RECALL_FLAG ,
                     Item_Delta.SMA_POS_REQUIRED = item_recall_df.SMA_POS_REQUIRED,
                     Item_Delta.SMA_TAG_REQUIRED = item_recall_df.SMA_TAG_REQUIRED,
                     Item_Delta.LAST_UPDATE_ID = item_recall_df.LAST_UPDATE_ID,
                     Item_Delta.LAST_UPDATE_TIMESTAMP= item_recall_df.LAST_UPDATE_TIMESTAMP'''.format(ItemMainDeltaPath))
    
    appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(ItemMainDeltaPath))
    loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
    appended_recs = appended_recs.head(1)
    ABC(productRecallFinalCount=appended_recs[0][0])


    spark.catalog.dropTempView(temp_table_name)
    loggerAtt.info("Merge into item main Delta table successful")
    
    return nonItemRecallFile, itemsInItemTable

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Item Main work flow

# COMMAND ----------

# def itemMainFunc(item_transformed_df, processing_file):
#   itemsInItemTable = None
#   itemInvalidRecords = None
#   itemsNotInItemTable = None
#   nonItemRecallFile = None
#   try:
#     ABC(itemMainSetUp = 0)
#     if item_transformed_df.count() > 0:
#       item_transformed_df = item_transformed_df.select([c for c in item_transformed_df.columns if c in Item_List])
#       item_nomaintainence = item_transformed_df.filter((col('SMA_POS_REQUIRED') == " " ) | (col('SMA_TAG_REQUIRED') == " "))
#       item_transformed_df = item_transformed_df.filter((col('SMA_POS_REQUIRED') != " " ) & (col('SMA_TAG_REQUIRED') != " "))
      
#       if item_transformed_df.count() > 0:
#         itemsInItemTable, itemsNotInItemTable = findItemsInItemTable(item_transformed_df)
#   except Exception as ex:
#     ABC(itemMainSetUp = 0)
#     loggerAtt.error(ex)
#     err = ErrorReturn('Error', ex,'itemMainSetUp')
#     errJson = jsonpickle.encode(err)
#     errJson = json.loads(errJson)
#     dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
  
#   if processing_file == "Recall":
#     try:
#       if itemsInItemTable is not None:
#         if itemsInItemTable.count() > 0:
#           ABC(DeltaTableCreateCheck = 1)
#           nonItemRecallFile, itemsInItemTable = productRecall(itemsInItemTable)
#     except Exception as ex:
#       ABC(DeltaTableCreateCheck = 0)
#       ABC(productRecallInitCount = '')
#       ABC(productRecallFinalCount = '')
#       loggerAtt.error(ex)
#       err = ErrorReturn('Error', ex,'itemMainSetUp')
#       errJson = jsonpickle.encode(err)
#       errJson = json.loads(errJson)
#       dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
    
#     try:
#       if nonItemRecallFile != None and itemsNotInItemTable != None:
#         invalidRecallRecordsList = [nonItemRecallFile, itemsNotInItemTable]
#         itemInvalidRecords = reduce(DataFrame.unionAll, invalidRecallRecordsList)
#       elif nonItemRecallFile != None:
#         itemInvalidRecords = nonItemRecallFile
#       elif itemsNotInItemTable != None:
#         itemInvalidRecords = itemsNotInItemTable    
      
#       if itemInvalidRecords != None:
#         if itemInvalidRecords.count() > 0:
#           productRecallinvalidRecordsCount = itemInvalidRecords.count()
#           loggerAtt.info(f"Product Recall Invalid Record file count: {productRecallinvalidRecordsCount}")
#           ABC(productRecallinvalidRecCount = productRecallinvalidRecordsCount)
#           itemInvalidRecords.write.format("parquet").mode("Append").save(productRecallErrorPath)
#     except Exception as ex:
#       ABC(productRecallinvalidRecCount = '')
#       loggerAtt.error(ex)
#       err = ErrorReturn('Error', ex,'itemMainSetUp')
#       errJson = jsonpickle.encode(err)
#       errJson = json.loads(errJson)
#       dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
#   else:
#     try:
#       if itemsInItemTable != None:
#         if itemsInItemTable.count() > 0:
#           itemInvalidRecords=itemsInItemTable.filter((col('SMA_CHG_TYPE')==12) | (col('SMA_CHG_TYPE')==13) | (col('SMA_ITEM_STATUS').isNull()))
#           itemInvalidRecordsCount = itemInvalidRecords.count()
#           if itemInvalidRecordsCount > 0:
#             loggerAtt.info(f"Invalid Record due to chg type 12,13 count: {itemInvalidRecordsCount}")
#             ABC(itemInvalidRecCount = itemInvalidRecordsCount)
#             itemInvalidRecords.write.format("parquet").mode("Append").save(invalidRecordsPath)
          
#           itemsInItemTable = itemsInItemTable.filter((col("SMA_CHG_TYPE")!= 12) & (col("SMA_CHG_TYPE")!= 13) & (col("SMA_ITEM_STATUS").isNotNull()))
          
#           ABC(DeltaTableCreateCheck = 1)
#           if itemsInItemTable.count() > 0:
#             updateItemRecord(itemsInItemTable)
#     except Exception as ex:
#       ABC(DeltaTableCreateCheck = 0)
#       ABC(ItemMainFinalCount = '')
#       ABC(ItemMainInitCount = '')
#       loggerAtt.error(ex)
#       err = ErrorReturn('Error', ex,'updateItemRecord')
#       errJson = jsonpickle.encode(err)
#       errJson = json.loads(errJson)
#       dbutils.notebook.exit(Merge(ABCChecks,errJson))     
      
#     try:
#       if itemsNotInItemTable != None:
#         if itemsNotInItemTable.count() > 0:
#           itemInvalidRecords = itemsNotInItemTable.filter((col('SMA_ITEM_STATUS').isNull()) | (col('SMA_ITEM_STATUS')=='D'))
          
#           itemInvalidRecordsCount = itemInvalidRecords.count()
#           if itemInvalidRecordsCount > 0:
#             loggerAtt.info(f"Invalid Record due to chg type null status and with status D: {itemInvalidRecordsCount}")
#             ABC(itemInvalidRecCount = itemInvalidRecordsCount)
#             itemInvalidRecords.write.format("parquet").mode("Append").save(invalidRecordsPath)
          
#           ABC(DeltaTableCreateCheck = 1)
#           itemsNotInItemTable = itemsNotInItemTable.filter((col('SMA_ITEM_STATUS').isNotNull()) & (col('SMA_ITEM_STATUS')!='D'))
#           insertItemRecord(itemsNotInItemTable)
#     except Exception as ex:
#       ABC(DeltaTableCreateCheck = 0)
#       ABC(ItemMainFinalCount = '')
#       ABC(ItemMainInitCount = '')
#       loggerAtt.error(ex)
#       err = ErrorReturn('Error', ex,'insertItemRecord')
#       errJson = jsonpickle.encode(err)
#       errJson = json.loads(errJson)
#       dbutils.notebook.exit(Merge(ABCChecks,errJson))     

# COMMAND ----------

def itemMainFunc(item_transformed_df, processing_file):
  itemsInItemTable = None
  itemInvalidRecords = None
  itemsNotInItemTable = None
  nonItemRecallFile = None
  DuplicateValueCnt = 0
  try:
    ABC(itemMainSetUp = 0)
    if item_transformed_df.count() > 0:
      item_transformed_df = item_transformed_df.select([c for c in item_transformed_df.columns if c in Item_List])
      item_nomaintainence = item_transformed_df.filter((col('SMA_POS_REQUIRED') == " " ) | (col('SMA_TAG_REQUIRED') == " "))
      item_transformed_df = item_transformed_df.filter((col('SMA_POS_REQUIRED') != " " ) & (col('SMA_TAG_REQUIRED') != " "))
      
  except Exception as ex:
    ABC(itemMainSetUp = 0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'itemMainSetUp')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
  
  loggerAtt.info("Removing duplicate record check for Item Main")
  try:
    ABC(DuplicateValueCheck = 1)
    if (item_transformed_df.groupBy('SMA_DEST_STORE','SMA_GTIN_NUM').count().filter("count > 1").count()) > 0:
        item_duplicate_records = item_transformed_df.groupBy('SMA_DEST_STORE','SMA_GTIN_NUM').count().filter(col('count') > 1)
        item_duplicate_records = item_duplicate_records.drop(item_duplicate_records['count'])
        item_duplicate_records = (item_transformed_df.join(item_duplicate_records,["SMA_GTIN_NUM", 'SMA_DEST_STORE'], "leftsemi"))
        DuplicateValueCnt = item_duplicate_records.count()
        loggerAtt.info("Duplicate Record Count: ("+str(DuplicateValueCnt)+"," +str(len(item_duplicate_records.columns))+")")
        ABC(DuplicateValueCount=DuplicateValueCnt)
        item_transformed_df = (item_transformed_df.join(item_duplicate_records,["SMA_GTIN_NUM", 'SMA_DEST_STORE'], "leftanti"))
    else:
        ABC(DuplicateValueCount=0)
        loggerAtt.info(f"No PROBLEM RECORDS")
  except Exception as ex:
    ABC(DuplicateValueCheck=0)
    ABC(DuplicateValueCount='')
    err = ErrorReturn('Error', ex,'Removing duplicate record Error Item Main')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))     
  
  if DuplicateValueCnt > 0:
    loggerAtt.info("Write Duplicate Record")
    try:
      Invalid_RecordsPath = Invalid_RecordsPath + "/" +Date+ "/" + "duplicates_ItemMain"
      writeInvalidRecord(item_duplicate_records, Invalid_RecordsPath, Date)
    except Exception as ex:
      ABC(InvalidRecordSaveCheck = 0)
      ABC(InvalidRecordCount = '')
      loggerAtt.error(ex)
      err = ErrorReturn('Error', ex,'write Duplicate Record Item Main')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
  
  try:
    ABC(findItemsInItemTableCheck = 1)
    if item_transformed_df.count() > 0:
      itemsInItemTable, itemsNotInItemTable = findItemsInItemTable(item_transformed_df)
  except Exception as ex:
    ABC(findItemsInItemTableCheck = 0)
    loggerAtt.error(ex)
    err = ErrorReturn('Error', ex,'findItemsInItemTable')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
  
  if processing_file == "Recall":
    try:
      if itemsInItemTable is not None:
        if itemsInItemTable.count() > 0:
          ABC(DeltaTableCreateCheck = 1)
          nonItemRecallFile, itemsInItemTable = productRecall(itemsInItemTable)
    except Exception as ex:
      ABC(DeltaTableCreateCheck = 0)
      ABC(productRecallInitCount = '')
      ABC(productRecallFinalCount = '')
      loggerAtt.error(ex)
      err = ErrorReturn('Error', ex,'itemMainSetUp')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
    
    try:
      if nonItemRecallFile != None and itemsNotInItemTable != None:
        invalidRecallRecordsList = [nonItemRecallFile, itemsNotInItemTable]
        itemInvalidRecords = reduce(DataFrame.unionAll, invalidRecallRecordsList)
      elif nonItemRecallFile != None:
        itemInvalidRecords = nonItemRecallFile
      elif itemsNotInItemTable != None:
        itemInvalidRecords = itemsNotInItemTable    
      
      if itemInvalidRecords != None:
        if itemInvalidRecords.count() > 0:
          productRecallinvalidRecordsCount = itemInvalidRecords.count()
          loggerAtt.info(f"Product Recall Invalid Record file count: {productRecallinvalidRecordsCount}")
          ABC(productRecallinvalidRecCount = productRecallinvalidRecordsCount)
          itemInvalidRecords.write.format("parquet").mode("Append").save(productRecallErrorPath)
    except Exception as ex:
      ABC(productRecallinvalidRecCount = '')
      loggerAtt.error(ex)
      err = ErrorReturn('Error', ex,'itemMainSetUp')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson)) 
  else:
    try:
      if itemsInItemTable != None:
        if itemsInItemTable.count() > 0:
          itemInvalidRecords=itemsInItemTable.filter((col('SMA_CHG_TYPE')==12) | (col('SMA_CHG_TYPE')==13) | (col('SMA_ITEM_STATUS').isNull()))
          itemInvalidRecordsCount = itemInvalidRecords.count()
          if itemInvalidRecordsCount > 0:
            loggerAtt.info(f"Invalid Record due to chg type 12,13 count: {itemInvalidRecordsCount}")
            ABC(itemInvalidRecCount = itemInvalidRecordsCount)
            itemInvalidRecords.write.format("parquet").mode("Append").save(invalidRecordsPath)
          
          itemsInItemTable = itemsInItemTable.filter((col("SMA_CHG_TYPE")!= 12) & (col("SMA_CHG_TYPE")!= 13) & (col("SMA_ITEM_STATUS").isNotNull()))
          
          ABC(DeltaTableCreateCheck = 1)
          if itemsInItemTable.count() > 0:
            updateItemRecord(itemsInItemTable)
    except Exception as ex:
      ABC(DeltaTableCreateCheck = 0)
      ABC(ItemMainFinalCount = '')
      ABC(ItemMainInitCount = '')
      loggerAtt.error(ex)
      err = ErrorReturn('Error', ex,'updateItemRecord')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))     
      
    try:
      if itemsNotInItemTable != None:
        if itemsNotInItemTable.count() > 0:
          itemInvalidRecords = itemsNotInItemTable.filter((col('SMA_ITEM_STATUS').isNull()) | (col('SMA_ITEM_STATUS')=='D'))
          
          itemInvalidRecordsCount = itemInvalidRecords.count()
          if itemInvalidRecordsCount > 0:
            loggerAtt.info(f"Invalid Record due to chg type null status and with status D: {itemInvalidRecordsCount}")
            ABC(itemInvalidRecCount = itemInvalidRecordsCount)
            itemInvalidRecords.write.format("parquet").mode("Append").save(invalidRecordsPath)
          
          ABC(DeltaTableCreateCheck = 1)
          itemsNotInItemTable = itemsNotInItemTable.filter((col('SMA_ITEM_STATUS').isNotNull()) & (col('SMA_ITEM_STATUS')!='D'))
          insertItemRecord(itemsNotInItemTable)
    except Exception as ex:
      ABC(DeltaTableCreateCheck = 0)
      ABC(ItemMainFinalCount = '')
      ABC(ItemMainInitCount = '')
      loggerAtt.error(ex)
      err = ErrorReturn('Error', ex,'insertItemRecord')
      errJson = jsonpickle.encode(err)
      errJson = json.loads(errJson)
      dbutils.notebook.exit(Merge(ABCChecks,errJson))     