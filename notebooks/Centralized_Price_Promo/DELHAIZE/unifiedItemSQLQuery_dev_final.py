# Databricks notebook source
# MAGIC %md
# MAGIC ## Unifed Item Join for Intraday POS file data

# COMMAND ----------

def fetchDataFromAllFeedIntraday(itemTempEffDeltaPath):
  loggerAtt.info("Fetching Unified data for Intraday feed initiated")
  try:
    initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(itemTempEffDeltaPath))
    loggerAtt.info(f"Item Temp count for : {initial_recs.head(1)}")
    initial_recs = initial_recs.head(1)
    
    unified_df = spark.read.format('delta').load(itemTempEffDeltaPath)
  except Exception as ex:
    loggerAtt.info("Fetching Unified data for Intraday feed failed and throwed error")
    loggerAtt.error(str(ex))
    err = ErrorReturn('Error', ex,'fetchDataFromAllFeedIntraday')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    ABC(FetchDataFromAllFeedIntradayCheck=0)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  loggerAtt.info("Fetching Unified data for Intraday feed successful")    
  
  return unified_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unifed Item Join for Daily/Weekly POS file data

# COMMAND ----------

def fetchDataFromAllFeedDaily(itemTempEffDeltaPath):
  loggerAtt.info("Fetching Unified data for Daily/Weekly feed initiated")
  try:

    initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(itemTempEffDeltaPath))
    loggerAtt.info(f"Unified count items going for Join Table: {initial_recs.head(1)}")
    productCount = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(productTempEffDeltaPath))
    loggerAtt.info(f"Product count items going for Join Table: {productCount.head(1)}")
    storeCount = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(storeTempEffDeltaPath))
    loggerAtt.info(f"Store count items going for Join Table: {storeCount.head(1)}")
    inventoryCount = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(inventoryTempEffDeltaPath))
    loggerAtt.info(f"Inventory count items going for Join Table: {inventoryCount.head(1)}")
    productToStoreCount = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(productToStoreTempEffDeltaPath))
    loggerAtt.info(f"Product To Store count items going for Join Table: {productToStoreCount.head(1)}")
    priceCount = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(priceTempEffDeltaPath))
    loggerAtt.info(f"Price count items going for Join Table: {priceCount.head(1)}")
    initial_recs = initial_recs.head(1)
    
    ABC(UnifiedItemTempBeforeJoinCount=initial_recs[0][0])
    unified_df = spark.sql("""
                  SELECT  SMA_BATCH_SERIAL_NBR,
                          SMA_TAX_1,
                          SMA_TAX_2,
                          SMA_TAX_3,
                          SMA_TAX_4,
                          SMA_TAX_5,
                          SMA_TAX_6,
                          SMA_TAX_7,
                          SMA_TAX_8,    
                          SMA_VEND_COUPON_FAM1,
                          SMA_VEND_COUPON_FAM2,
                          SMA_VEND_NUM,
                          SMA_WIC_IND,
                          SMA_UPC_DESC,
                          SMA_STORE,
                          SMA_SUB_DEPT,
                          SMA_STATUS_DATE,
                          SMA_RETL_VENDOR,
                          SMA_SBW_IND,
                          SMA_SELL_RETL,
                          SMA_RETL_MULT_UNIT,
                          SMA_RESTR_CODE,
                          SMA_MULT_UNIT_RETL,
                          SMA_LINK_UPC,
                          SMA_LINK_HDR_COUPON,
                          SMA_ITEM_DESC,
                          SMA_GTIN_NUM,
                          SMA_FOOD_STAMP_IND,
                          SMA_DEST_STORE,
                          SMA_FIXED_TARE_WGT,
                          SMA_BOTTLE_DEPOSIT_IND,
                          SMA_BATCH_SUB_DEPT,
                          SMA_COMM,
                          SMA_SUB_COMM,
                          SMA_NAT_ORG_IND,
                          SMA_LEGACY_ITEM,
                          SMA_ITEM_SIZE,
                          SMA_PRIV_LABEL,
                          SMA_VENDOR_PACK,
                          SMA_ITEM_UOM,
                          SCRTX_HDR_DESC,
                          SMA_ITEM_TYPE,
                          SMA_RETL_UOM,
                          SMA_RETL_MULT_UOM,
                          SMA_ITM_EFF_DATE,
                          SMA_GUIDING_STARS,
                          PRICE.SMA_SMR_EFF_DATE,
                          SMA_STORE_STATE,
                          SMA_FSA_IND,
                          SMA_SOURCE_WHSE,
                          AVG_WGT,
                          ALTERNATE_UPC,
                          DSS_PRODUCT_DESC,
                          DSS_PRIMARY_ID,
                          DSS_PRIMARY_DESC,
                          DSS_SUB_CATEGORY_DESC,
                          DSS_CATEGORY_ID,
                          DSS_CATEGORY_DESC,
                          DSS_SUPER_CATEGORY_DESC,
                          DSS_ALL_CATEGORY_ID,
                          DSS_ALL_CATEGORY_DESC,
                          DSS_MDSE_PROGRAM_ID,
                          DSS_MDSE_PROGRAM_DESC,
                          DSS_PRICE_MASTER_ID,
                          DSS_PRICE_MASTER_DESC,
                          DSS_BUYER_ID,
                          DSS_BUYER_DESC,
                          DSS_PRICE_SENS_ID,
                          DSS_PRICE_SENS_SHORT_DESC,
                          DSS_PRICE_SENS_LONG_DESC,
                          DSS_MANCODE_ID,
                          DSS_MANCODE_DESC,
                          DSS_PRIVATE_BRAND_ID,
                          DSS_PRIVATE_BRAND_DESC,
                          DSS_PRODUCT_STATUS_ID,
                          DSS_PRODUCT_STATUS_DESC,
                          DSS_PRODUCT_UOM_DESC,
                          DSS_PRODUCT_PACK_QTY,
                          DSS_DIRECTOR_ID,
                          DSS_DIRECTOR_DESC,
                          DSS_DIRECTOR_GROUP_DESC,
                          DSS_MAJOR_CATEGORY_ID,
                          DSS_MAJOR_CATEGORY_DESC,
                          DSS_PLANOGRAM_ID,
                          DSS_PLANOGRAM_DESC,
                          DSS_HEIGHT,
                          DSS_WIDTH,
                          DSS_DEPTH,
                          DSS_BRAND_ID,
                          DSS_BRAND_DESC,
                          DSS_LIFO_POOL_ID,
                          DSS_GROUP_TYPE,
                          DSS_DATE_UPDATED,
                          DSS_EQUIVALENT_SIZE_ID,
                          DSS_EQUIVALENT_SIZE_DESC,
                          DSS_EQUIV_SIZE,
                          DSS_SCAN_TYPE_ID,
                          DSS_SCAN_TYPE_DESC,
                          DSS_STORE_HANDLING_CODE,
                          DSS_SHC_DESC,
                          DSS_GS_RATING_DESC,
                          DSS_GS_DATE_RATED,
                          DSS_PRIVATE_LABEL_ID,
                          DSS_PRIVATE_LABEL_DESC,
                          DSS_PRODUCT_DIMESION_L_BYTE,
                          DSS_PRDT_NAME,
                          DSS_MFR_NAME,
                          DSS_BRAND_NAME,
                          DSS_KEYWORD,
                          DSS_META_DESC,
                          DSS_META_KEYWORD,
                          DSS_SRVG_SZ_DSC,
                          DSS_SRVG_SZ_UOM_DSC,
                          DSS_SRVG_PER_CTNR,
                          DSS_NEW_PRDT_FLG,
                          DSS_OVRLN_DSC,
                          DSS_ALT_SRVG_SZ_DSC,
                          DSS_ALT_SRVG_SZ_UOM,
                          DSS_AVG_UNIT_WT,
                          DSS_PRIVATE_BRAND_CD,
                          DSS_MENU_LBL_FLG,
                          INVENTORY_CODE,
                          INVENTORY_QTY_ON_HAND,
                          INVENTORY_SUPER_CATEGORY,
                          INVENTORY_MAJOR_CATEGORY,
                          INVENTORY_INTMD_CATEGORY,
                          BANNER_ID,
                          PRD2STORE_PROMO_CODE,
                          PRD2STORE_SPLN_AD_CD,
                          PRD2STORE_TAG_TYP_CD,
                          PRD2STORE_WINE_VALUE_FLAG,
                          PRD2STORE_BUY_QTY,
                          PRD2STORE_LMT_QTY,
                          PRD2STORE_SALE_STRT_DT,
                          PRD2STORE_SALE_END_DT,
                          PRD2STORE_AGE_FLAG,
                          PRD2STORE_AGE_CODE,
                          PRD2STORE_SWAP_SAVE_UPC,
                          PRICE_UNIT_PRICE,
                          PRICE_UOM_CD,
                          PRICE_MLT_Quantity,
                          PRICE_PROMO_RTL_PRC,
                          PRICE_PROMO_UNIT_PRICE,
                          PRICE_Promotional_Quantity,
                          PRICE_START_DATE,
                          PRICE_END_DATE,
                          TPRX001_STORE_SID_NBR,
                          TPRX001_ITEM_NBR,
                          TPRX001_ITEM_SRC_CD,
                          TPRX001_CPN_SRC_CD,
                          TPRX001_RTL_PRC_EFF_DT,
                          TPRX001_ITEM_PROMO_FLG,
                          TPRX001_PROMO_TYP_CD,
                          TPRX001_AD_TYP_CD,
                          TPRX001_PROMO_DSC,
                          TPRX001_MIX_MTCH_FLG,
                          TPRX001_PRC_STRAT_CD,
                          TPRX001_LOYAL_CRD_FLG,
                          TPRX001_SCAN_AUTH_FLG,
                          TPRX001_MDSE_AUTH_FLG,
                          TPRX001_SBT_FLG,
                          TPRX001_SBT_VEND_ID,
                          TPRX001_SBT_VEND_NET_CST,
                          TPRX001_SCAN_DAUTH_DT,
                          TPRX001_SCAN_PRVWK_RTL_MLT,
                          TPRX001_SCAN_PRVWK_RTL_PRC,
                          TPRX001_SCANPRVDAY_RTL_MLT,
                          TPRX001_SCANPRVDAY_RTL_PRC,
                          TPRX001_TAG_PRV_WK_RTL_MLT,
                          TPRX001_TAG_PRV_WK_RTL_PRC,
                          SCRTX_DET_FREQ_SHOP_TYPE,
                          SCRTX_DET_FREQ_SHOP_VAL,
                          SCRTX_DET_QTY_RQRD_FG,
                          SCRTX_DET_MIX_MATCH_CD,
                          SCRTX_DET_CENTRAL_ITEM,
                          SCRTX_DET_OP_CODE,
                          SCRTX_DET_NON_MDSE_ID,
                          SCRTX_DET_RCPT_DESCR,
                          SCRTX_DET_NG_ENTRY_FG,
                          SCRTX_DET_STR_CPN_FG,
                          SCRTX_DET_VEN_CPN_FG,
                          SCRTX_DET_MAN_PRC_FG,
                          SCRTX_DET_WIC_CVV_FG,
                          SCRTX_DET_DEA_GRP,
                          SCRTX_DET_COMP_TYPE,
                          SCRTX_DET_COMP_PRC,
                          SCRTX_DET_COMP_QTY,
                          SCRTX_DET_BLK_GRP,
                          SCRTX_DET_RSTRCSALE_BRCD_FG,
                          SCRTX_DET_RX_FG,
                          SCRTX_DET_INTRNL_ID,
                          SCRTX_DET_NON_RX_HEALTH_FG,
                          SCRTX_DET_PLU_BTCH_NBR,
                          SCRTX_DET_SLS_RESTRICT_GRP,
                          RTX_TYPE,
                          SMA_ITEM_STATUS,
                          BTL_DPST_AMT,
                          DSS_DC_ITEM_NUMBER,
                          itemMain.INSERT_ID,
                          itemMain.INSERT_TIMESTAMP,
                          itemMain.LAST_UPDATE_ID,
                          itemMain.LAST_UPDATE_TIMESTAMP
                    FROM delta.`{}` AS itemMain
                     Inner JOIN delta.`{}` AS PROD ON
                        PROD.DSS_PRODUCT_ID=itemMain.ALT_UPC_FETCH AND 
                        PROD.DSS_BANNER_ID =itemMain.BANNER_ID
                     Inner JOIN delta.`{}` AS STORE  ON
                        STORE.STORE_STORE_NUMBER=itemMain.SMA_DEST_STORE AND 
                        STORE.STORE_BANNER_ID=itemMain.BANNER_ID
                     Inner JOIN delta.`{}` AS INVENTORY  ON
                        INVENTORY.INVENTORY_STORE_ID=itemMain.SMA_DEST_STORE AND 
                        INVENTORY.INVENTORY_UPC=itemMain.SMA_GTIN_NUM AND 
                        INVENTORY.INVENTORY_BANNER_ID=itemMain.BANNER_ID
                     Inner JOIN delta.`{}` AS PRD2STORE  ON
                        PRD2STORE.PRD2STORE_STORE_ID=itemMain.SMA_DEST_STORE AND 
                        PRD2STORE.PRD2STORE_UPC=itemMain.SMA_GTIN_NUM
                     Inner JOIN delta.`{}` AS PRICE  ON
                        PRICE.PRICE_STORE_ID=itemMain.SMA_DEST_STORE AND 
                        PRICE.PRICE_UPC=itemMain.SMA_GTIN_NUM AND 
                        PRICE.PRICE_BANNER_ID=itemMain.BANNER_ID""".format(itemTempEffDeltaPath, productTempEffDeltaPath, storeTempEffDeltaPath, inventoryTempEffDeltaPath, productToStoreTempEffDeltaPath, priceTempEffDeltaPath))
    unified_df = unified_df.withColumn("PRD2STORE_BANNER_ID", col('BANNER_ID'))
    unifiedCount = unified_df.count()
    loggerAtt.info(f"Unified count of records in Table: {unifiedCount}")
    appended_recs = unifiedCount
    ABC(UnifiedItemTempAfterJoinCount=appended_recs)
  except Exception as ex:
    loggerAtt.info("Fetching Unified data for Daily/Weekly feed failed and throwed error")
    loggerAtt.error(str(ex))
    ABC(UnifiedItemTempBeforeJoinCount='')
    ABC(UnifiedItemTempAfterJoinCount='')
    ABC(FetchDataFromAllFeedDailyCheck=0)
    err = ErrorReturn('Error', ex,'fetchDataFromAllFeedDaily')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  loggerAtt.info("Fetching Unified data for Daily/Weekly feed successful")    
  return unified_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Find not in Item Temp Eff table after all feed join

# COMMAND ----------

def findItemsNotInItemTemp(unifiedCount, itemTempEffDeltaPath, exceptionItemMainTempArchivalpath):
  loggerAtt.info("Fetching items missed in Join initiated - findItemsNotInItemTemp")
  try:
    temp_table_name = "unifiedCount"
    unifiedCount.createOrReplaceTempView(temp_table_name)
    loggerAtt.info("Count in Item Temp table "+ str(unifiedCount.count()))
    processed_df = spark.sql('''SELECT hostItemExist.exist, item_df.* FROM  delta.`{}` item_df
                                LEFT JOIN (SELECT "Y" as exist,unifiedCount.SMA_GTIN_NUM, unifiedCount.SMA_DEST_STORE, unifiedCount.BANNER_ID
                                           FROM unifiedCount join delta.`{}` deltaItem 
                                           WHERE deltaItem.SMA_GTIN_NUM = unifiedCount.SMA_GTIN_NUM and deltaItem.SMA_DEST_STORE = unifiedCount.SMA_DEST_STORE and deltaItem.BANNER_ID = unifiedCount.BANNER_ID) AS hostItemExist
                                ON item_df.SMA_GTIN_NUM = hostItemExist.SMA_GTIN_NUM and item_df.SMA_DEST_STORE = hostItemExist.SMA_DEST_STORE and item_df.BANNER_ID = hostItemExist.BANNER_ID'''.format(itemTempEffDeltaPath, itemTempEffDeltaPath))
    itemsNotInItemTable = processed_df.filter((col('exist').isNull()))
    itemsInItemTable = processed_df.filter((col('exist').isNotNull()))
    itemsInItemTableCount = str(itemsInItemTable.count())
    itemsNotInItemTableCount = str(itemsNotInItemTable.count())
    loggerAtt.info("Count in Item Temp table came after join "+ itemsInItemTableCount)
    loggerAtt.info("Count in Item Temp table missed in join "+ itemsNotInItemTableCount)
    ABC(itemTempCameAfterJoinCount=itemsNotInItemTableCount)
    
    itemsNotInItemTable.write.partitionBy('SMA_DEST_STORE').mode('append').format('parquet').save(exceptionItemMainTempArchivalpath)
    
  except Exception as ex:
    loggerAtt.info("Fetching items missed in Join failed and throwed error - findItemsNotInItemTemp")
    loggerAtt.error(str(ex))
    ABC(itemTempCameAfterJoinCount='')
    ABC(findItemsNotInItemCheck='')
    err = ErrorReturn('Error', ex,'findItemsNotInItemTemp')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  loggerAtt.info("Fetching items missed in Join successful - findItemsNotInItemTemp")    
  spark.catalog.dropTempView(temp_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Update/Insert record into unified Item Master

# COMMAND ----------

def upsertItemRecordsDaily(unified_df, itemMasterDeltaPath):
  loggerAtt.info("Merge into Delta table initiated - upsertItemRecordsDaily")
  try:
    unified_df = unified_df.withColumn("LAST_UPDATE_TIMESTAMP", lit(currentTimeStamp).cast(TimestampType()))
    unified_df = unified_df.withColumn("LAST_UPDATE_ID", lit(pipelineid))
    unified_df = unified_df.withColumn("INSERT_TIMESTAMP", lit(currentTimeStamp).cast(TimestampType()))
    unified_df = unified_df.withColumn("INSERT_ID", lit(pipelineid))
    temp_table_name = "unified_df"
    unified_df.createOrReplaceTempView(temp_table_name)
    
    initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(itemMasterDeltaPath))
    loggerAtt.info(f"Initial count of records in Delta Table: {initial_recs.head(1)}")
    initial_recs = initial_recs.head(1)
    
    ABC(DeltaTableInitCount=initial_recs[0][0])
    spark.sql('''MERGE INTO delta.`{}` as itemMaster
    USING unified_df 
    ON itemMaster.SMA_GTIN_NUM = unified_df.SMA_GTIN_NUM and
       itemMaster.SMA_DEST_STORE = unified_df.SMA_DEST_STORE and
       itemMaster.PRD2STORE_BANNER_ID = unified_df.PRD2STORE_BANNER_ID
    WHEN MATCHED Then 
            Update Set  itemMaster.SMA_ACTIVATION_CODE = unified_df.SMA_ACTIVATION_CODE,
                        itemMaster.SMA_CHG_TYPE = unified_df.SMA_CHG_TYPE,
                        itemMaster.SMA_COMP_ID = unified_df.SMA_COMP_ID,
                        itemMaster.SMA_COMP_OBSERV_DATE = unified_df.SMA_COMP_OBSERV_DATE,
                        itemMaster.SMA_COMP_PRICE = unified_df.SMA_COMP_PRICE,
                        itemMaster.SMA_CONTRIB_QTY = unified_df.SMA_CONTRIB_QTY,
                        itemMaster.SMA_CONTRIB_QTY_UOM = unified_df.SMA_CONTRIB_QTY_UOM,
                        itemMaster.SMA_CPN_MLTPY_IND = unified_df.SMA_CPN_MLTPY_IND,
                        itemMaster.SMA_DATA_TYPE = unified_df.SMA_DATA_TYPE,
                        itemMaster.SMA_DEMANDTEC_PROMO_ID = unified_df.SMA_DEMANDTEC_PROMO_ID,
                        itemMaster.SMA_DISCOUNTABLE_IND = unified_df.SMA_DISCOUNTABLE_IND,
                        itemMaster.SMA_EFF_DOW = unified_df.SMA_EFF_DOW,
                        itemMaster.SMA_EMERGY_UPDATE_IND = unified_df.SMA_EMERGY_UPDATE_IND,
                        itemMaster.SMA_FEE_ITEM_TYPE = unified_df.SMA_FEE_ITEM_TYPE,
                        itemMaster.SMA_FEE_TYPE = unified_df.SMA_FEE_TYPE,
                        itemMaster.SMA_FUEL_PROD_CATEG = unified_df.SMA_FUEL_PROD_CATEG,
                        itemMaster.SMA_GAS_IND = unified_df.SMA_GAS_IND,
                        itemMaster.SMA_ITM_EFF_DATE = unified_df.SMA_ITM_EFF_DATE,
                        itemMaster.SMA_HIP_IND = unified_df.SMA_HIP_IND,
                        itemMaster.SMA_GLUTEN_FREE = unified_df.SMA_GLUTEN_FREE,
                        itemMaster.SMA_IFPS_CODE = unified_df.SMA_IFPS_CODE,
                        itemMaster.SMA_IN_STORE_TAG_PRINT = unified_df.SMA_IN_STORE_TAG_PRINT,
                        itemMaster.SMA_ITEM_BRAND = unified_df.SMA_ITEM_BRAND,
                        itemMaster.SMA_ITEM_MVMT = unified_df.SMA_ITEM_MVMT,
                        itemMaster.SMA_ITEM_POG_COMM = unified_df.SMA_ITEM_POG_COMM,
                        itemMaster.SMA_KOSHER_FLAG = unified_df.SMA_KOSHER_FLAG,
                        itemMaster.SMA_LINK_APPLY_TIME = unified_df.SMA_LINK_APPLY_TIME,
                        itemMaster.SMA_LINK_CHG_TYPE = unified_df.SMA_LINK_CHG_TYPE,
                        itemMaster.SMA_LINK_FAMCD_PROMOCD = unified_df.SMA_LINK_FAMCD_PROMOCD,
                        itemMaster.SMA_LINK_HDR_LOCATION = unified_df.SMA_LINK_HDR_LOCATION,
                        itemMaster.SMA_LINK_HDR_MAINT_TYPE = unified_df.SMA_LINK_HDR_MAINT_TYPE,
                        itemMaster.SMA_LINK_ITEM_NBR = unified_df.SMA_LINK_ITEM_NBR,
                        itemMaster.SMA_LINK_OOPS_ADWK = unified_df.SMA_LINK_OOPS_ADWK,
                        itemMaster.SMA_LINK_OOPS_FILE_ID = unified_df.SMA_LINK_OOPS_FILE_ID,
                        itemMaster.SMA_LINK_RCRD_TYPE = unified_df.SMA_LINK_RCRD_TYPE,
                        itemMaster.SMA_LINK_TYPE = unified_df.SMA_LINK_TYPE,
                        itemMaster.SMA_MANUAL_PRICE_ENTRY = unified_df.SMA_MANUAL_PRICE_ENTRY,
                        itemMaster.SMA_MEAS_OF_EACH = unified_df.SMA_MEAS_OF_EACH,
                        itemMaster.SMA_MEAS_OF_EACH_WIP = unified_df.SMA_MEAS_OF_EACH_WIP,
                        itemMaster.SMA_MEAS_OF_PRICE = unified_df.SMA_MEAS_OF_PRICE,
                        itemMaster.SMA_MKT_AREA = unified_df.SMA_MKT_AREA,
                        itemMaster.SMA_NEW_ITMUPC = unified_df.SMA_NEW_ITMUPC,
                        itemMaster.SMA_NON_PRICED_ITEM = unified_df.SMA_NON_PRICED_ITEM,
                        itemMaster.SMA_ORIG_CHG_TYPE = unified_df.SMA_ORIG_CHG_TYPE,
                        itemMaster.SMA_ORIG_LIN = unified_df.SMA_ORIG_LIN,
                        itemMaster.SMA_ORIG_VENDOR = unified_df.SMA_ORIG_VENDOR,
                        itemMaster.SMA_POINTS_ELIGIBLE = unified_df.SMA_POINTS_ELIGIBLE,
                        itemMaster.SMA_POS_REQUIRED = unified_df.SMA_POS_REQUIRED,
                        itemMaster.SMA_POS_SYSTEM = unified_df.SMA_POS_SYSTEM,
                        itemMaster.SMA_PRICE_CHG_ID = unified_df.SMA_PRICE_CHG_ID,
                        itemMaster.SMA_PRIME_UPC = unified_df.SMA_PRIME_UPC,
                        itemMaster.SMA_QTY_KEY_OPTIONS = unified_df.SMA_QTY_KEY_OPTIONS,
                        itemMaster.SMA_RECALL_FLAG = unified_df.SMA_RECALL_FLAG,
                        itemMaster.SMA_REFUND_RECEIPT_EXCLUSION = unified_df.SMA_REFUND_RECEIPT_EXCLUSION,
                        itemMaster.SMA_RETL_CHG_TYPE = unified_df.SMA_RETL_CHG_TYPE,
                        itemMaster.SMA_RETL_PRC_ENTRY_CHG = unified_df.SMA_RETL_PRC_ENTRY_CHG,
                        itemMaster.SMA_SEGMENT = unified_df.SMA_SEGMENT,
                        itemMaster.SMA_SHELF_TAG_REQ = unified_df.SMA_SHELF_TAG_REQ,
                        itemMaster.SMA_SOURCE_METHOD = unified_df.SMA_SOURCE_METHOD,
                        itemMaster.SMA_STORE_DIV = unified_df.SMA_STORE_DIV,
                        itemMaster.SMA_STORE_ZONE = unified_df.SMA_STORE_ZONE,
                        itemMaster.SMA_TAG_REQUIRED = unified_df.SMA_TAG_REQUIRED,
                        itemMaster.SMA_TAG_SZ = unified_df.SMA_TAG_SZ,
                        itemMaster.SMA_TARE_PCT = unified_df.SMA_TARE_PCT,
                        itemMaster.SMA_TARE_UOM = unified_df.SMA_TARE_UOM,
                        itemMaster.SMA_TAX_CATEG = unified_df.SMA_TAX_CATEG,
                        itemMaster.SMA_TAX_CHG_IND = unified_df.SMA_TAX_CHG_IND,
                        itemMaster.SMA_TENDER_RESTRICTION_CODE = unified_df.SMA_TENDER_RESTRICTION_CODE,
                        itemMaster.SMA_TIBCO_DATE = unified_df.SMA_TIBCO_DATE,
                        itemMaster.SMA_TRANSMIT_DATE = unified_df.SMA_TRANSMIT_DATE,
                        itemMaster.SMA_UNIT_PRICE_CODE = unified_df.SMA_UNIT_PRICE_CODE,
                        itemMaster.SMA_UOM_OF_PRICE = unified_df.SMA_UOM_OF_PRICE,
                        itemMaster.SMA_UPC_OVERRIDE_GRP_NUM = unified_df.SMA_UPC_OVERRIDE_GRP_NUM,
                        itemMaster.SMA_WIC_ALT = unified_df.SMA_WIC_ALT,
                        itemMaster.SMA_WIC_SHLF_MIN = unified_df.SMA_WIC_SHLF_MIN,
                        itemMaster.SMA_WPG = unified_df.SMA_WPG,
                        itemMaster.HOW_TO_SELL = unified_df.HOW_TO_SELL,
                        itemMaster.SMA_RECV_TYPE = unified_df.SMA_RECV_TYPE,
                        itemMaster.SMA_PL_UPC_REDEF = unified_df.SMA_PL_UPC_REDEF,
                        itemMaster.SMA_BATCH_SERIAL_NBR = COALESCE(unified_df.SMA_BATCH_SERIAL_NBR, itemMaster.SMA_BATCH_SERIAL_NBR),
                        itemMaster.SMA_TAX_1 = COALESCE(unified_df.SMA_TAX_1, itemMaster.SMA_TAX_1),
                        itemMaster.SMA_TAX_2 = COALESCE(unified_df.SMA_TAX_2, itemMaster.SMA_TAX_2),
                        itemMaster.SMA_TAX_3 = COALESCE(unified_df.SMA_TAX_3, itemMaster.SMA_TAX_3),
                        itemMaster.SMA_TAX_4 = COALESCE(unified_df.SMA_TAX_4, itemMaster.SMA_TAX_4),
                        itemMaster.SMA_TAX_5 = COALESCE(unified_df.SMA_TAX_5, itemMaster.SMA_TAX_5),
                        itemMaster.SMA_TAX_6 = COALESCE(unified_df.SMA_TAX_6, itemMaster.SMA_TAX_6),
                        itemMaster.SMA_TAX_7 = COALESCE(unified_df.SMA_TAX_7, itemMaster.SMA_TAX_7),
                        itemMaster.SMA_TAX_8 = COALESCE(unified_df.SMA_TAX_8, itemMaster.SMA_TAX_8),
                        itemMaster.SMA_VEND_COUPON_FAM1 = COALESCE(unified_df.SMA_VEND_COUPON_FAM1, itemMaster.SMA_VEND_COUPON_FAM1),
                        itemMaster.SMA_VEND_COUPON_FAM2 = COALESCE(unified_df.SMA_VEND_COUPON_FAM2, itemMaster.SMA_VEND_COUPON_FAM2),
                        itemMaster.SMA_VEND_NUM = COALESCE(unified_df.SMA_VEND_NUM, itemMaster.SMA_VEND_NUM),
                        itemMaster.SMA_WIC_IND = COALESCE(unified_df.SMA_WIC_IND, itemMaster.SMA_WIC_IND),
                        itemMaster.SMA_UPC_DESC = COALESCE(unified_df.SMA_UPC_DESC, itemMaster.SMA_UPC_DESC),
                        itemMaster.SMA_STORE = COALESCE(unified_df.SMA_STORE, itemMaster.SMA_STORE),
                        itemMaster.SMA_SUB_DEPT = COALESCE(unified_df.SMA_SUB_DEPT, itemMaster.SMA_SUB_DEPT),
                        itemMaster.SMA_STATUS_DATE = COALESCE(unified_df.SMA_STATUS_DATE, itemMaster.SMA_STATUS_DATE),
                        itemMaster.SMA_RETL_VENDOR = COALESCE(unified_df.SMA_RETL_VENDOR, itemMaster.SMA_RETL_VENDOR),
                        itemMaster.SMA_SBW_IND = COALESCE(unified_df.SMA_SBW_IND, itemMaster.SMA_SBW_IND),
                        itemMaster.SMA_SELL_RETL = COALESCE(unified_df.SMA_SELL_RETL, itemMaster.SMA_SELL_RETL),
                        itemMaster.SMA_RETL_MULT_UNIT = COALESCE(unified_df.SMA_RETL_MULT_UNIT, itemMaster.SMA_RETL_MULT_UNIT),
                        itemMaster.SMA_RESTR_CODE = COALESCE(unified_df.SMA_RESTR_CODE, itemMaster.SMA_RESTR_CODE),
                        itemMaster.SMA_MULT_UNIT_RETL = COALESCE(unified_df.SMA_MULT_UNIT_RETL, itemMaster.SMA_MULT_UNIT_RETL),
                        itemMaster.SMA_LINK_UPC = unified_df.SMA_LINK_UPC,
                        itemMaster.SMA_LINK_HDR_COUPON = COALESCE(unified_df.SMA_LINK_HDR_COUPON, itemMaster.SMA_LINK_HDR_COUPON),
                        itemMaster.SMA_ITEM_DESC = COALESCE(unified_df.SMA_ITEM_DESC, itemMaster.SMA_ITEM_DESC),
                        itemMaster.SMA_FOOD_STAMP_IND = COALESCE(unified_df.SMA_FOOD_STAMP_IND, itemMaster.SMA_FOOD_STAMP_IND),
                        itemMaster.SMA_FIXED_TARE_WGT = COALESCE(unified_df.SMA_FIXED_TARE_WGT, itemMaster.SMA_FIXED_TARE_WGT),
                        itemMaster.SMA_BOTTLE_DEPOSIT_IND = COALESCE(unified_df.SMA_BOTTLE_DEPOSIT_IND, itemMaster.SMA_BOTTLE_DEPOSIT_IND),
                        itemMaster.SMA_BATCH_SUB_DEPT = COALESCE(unified_df.SMA_BATCH_SUB_DEPT, itemMaster.SMA_BATCH_SUB_DEPT),
                        itemMaster.SMA_COMM = unified_df.SMA_COMM,
                        itemMaster.SMA_SUB_COMM = unified_df.SMA_SUB_COMM,
                        itemMaster.SMA_NAT_ORG_IND = unified_df.SMA_NAT_ORG_IND,
                        itemMaster.SMA_LEGACY_ITEM = unified_df.SMA_LEGACY_ITEM,
                        itemMaster.SMA_ITEM_SIZE = unified_df.SMA_ITEM_SIZE,
                        itemMaster.SMA_PRIV_LABEL = unified_df.SMA_PRIV_LABEL,
                        itemMaster.SMA_VENDOR_PACK = unified_df.SMA_VENDOR_PACK,
                        itemMaster.SMA_ITEM_UOM = unified_df.SMA_ITEM_UOM,
                        itemMaster.SMA_ITEM_TYPE = unified_df.SMA_ITEM_TYPE,
                        itemMaster.SMA_RETL_UOM = unified_df.SMA_RETL_UOM,
                        itemMaster.SMA_RETL_MULT_UOM = unified_df.SMA_RETL_MULT_UOM,
                        itemMaster.SMA_GUIDING_STARS = unified_df.SMA_GUIDING_STARS,
                        itemMaster.SMA_SMR_EFF_DATE = unified_df.SMA_SMR_EFF_DATE,
                        itemMaster.SMA_STORE_STATE = unified_df.SMA_STORE_STATE,
                        itemMaster.SMA_FSA_IND = COALESCE(unified_df.SMA_FSA_IND, itemMaster.SMA_FSA_IND),
                        itemMaster.SMA_SOURCE_WHSE = unified_df.SMA_SOURCE_WHSE,
                        itemMaster.AVG_WGT = unified_df.AVG_WGT,
                        itemMaster.ALTERNATE_UPC = unified_df.ALTERNATE_UPC,
                        itemMaster.DSS_PRODUCT_DESC = unified_df.DSS_PRODUCT_DESC,
                        itemMaster.DSS_PRIMARY_ID = unified_df.DSS_PRIMARY_ID,
                        itemMaster.DSS_PRIMARY_DESC = unified_df.DSS_PRIMARY_DESC,
                        itemMaster.DSS_SUB_CATEGORY_DESC = unified_df.DSS_SUB_CATEGORY_DESC,
                        itemMaster.DSS_CATEGORY_ID = unified_df.DSS_CATEGORY_ID,
                        itemMaster.DSS_CATEGORY_DESC = unified_df.DSS_CATEGORY_DESC,
                        itemMaster.DSS_SUPER_CATEGORY_DESC = unified_df.DSS_SUPER_CATEGORY_DESC,
                        itemMaster.DSS_ALL_CATEGORY_ID = unified_df.DSS_ALL_CATEGORY_ID,
                        itemMaster.DSS_ALL_CATEGORY_DESC = unified_df.DSS_ALL_CATEGORY_DESC,
                        itemMaster.DSS_MDSE_PROGRAM_ID = unified_df.DSS_MDSE_PROGRAM_ID,
                        itemMaster.DSS_MDSE_PROGRAM_DESC = unified_df.DSS_MDSE_PROGRAM_DESC,
                        itemMaster.DSS_PRICE_MASTER_ID = unified_df.DSS_PRICE_MASTER_ID,
                        itemMaster.DSS_PRICE_MASTER_DESC = unified_df.DSS_PRICE_MASTER_DESC,
                        itemMaster.DSS_BUYER_ID = unified_df.DSS_BUYER_ID,
                        itemMaster.DSS_BUYER_DESC = unified_df.DSS_BUYER_DESC,
                        itemMaster.DSS_PRICE_SENS_ID = unified_df.DSS_PRICE_SENS_ID,
                        itemMaster.DSS_PRICE_SENS_SHORT_DESC = unified_df.DSS_PRICE_SENS_SHORT_DESC,
                        itemMaster.DSS_PRICE_SENS_LONG_DESC = unified_df.DSS_PRICE_SENS_LONG_DESC,
                        itemMaster.DSS_MANCODE_ID = unified_df.DSS_MANCODE_ID,
                        itemMaster.DSS_MANCODE_DESC = unified_df.DSS_MANCODE_DESC,
                        itemMaster.DSS_PRIVATE_BRAND_ID = unified_df.DSS_PRIVATE_BRAND_ID,
                        itemMaster.DSS_PRIVATE_BRAND_DESC = unified_df.DSS_PRIVATE_BRAND_DESC,
                        itemMaster.DSS_PRODUCT_STATUS_ID = unified_df.DSS_PRODUCT_STATUS_ID,
                        itemMaster.DSS_PRODUCT_STATUS_DESC = unified_df.DSS_PRODUCT_STATUS_DESC,
                        itemMaster.DSS_PRODUCT_UOM_DESC = unified_df.DSS_PRODUCT_UOM_DESC,
                        itemMaster.DSS_PRODUCT_PACK_QTY = unified_df.DSS_PRODUCT_PACK_QTY,
                        itemMaster.DSS_DIRECTOR_ID = unified_df.DSS_DIRECTOR_ID,
                        itemMaster.DSS_DIRECTOR_DESC = unified_df.DSS_DIRECTOR_DESC,
                        itemMaster.DSS_DIRECTOR_GROUP_DESC = unified_df.DSS_DIRECTOR_GROUP_DESC,
                        itemMaster.DSS_MAJOR_CATEGORY_ID = unified_df.DSS_MAJOR_CATEGORY_ID,
                        itemMaster.DSS_MAJOR_CATEGORY_DESC = unified_df.DSS_MAJOR_CATEGORY_DESC,
                        itemMaster.DSS_PLANOGRAM_ID = unified_df.DSS_PLANOGRAM_ID,
                        itemMaster.DSS_PLANOGRAM_DESC = unified_df.DSS_PLANOGRAM_DESC,
                        itemMaster.DSS_HEIGHT = unified_df.DSS_HEIGHT,
                        itemMaster.DSS_WIDTH = unified_df.DSS_WIDTH,
                        itemMaster.DSS_DEPTH = unified_df.DSS_DEPTH,
                        itemMaster.DSS_BRAND_ID = unified_df.DSS_BRAND_ID,
                        itemMaster.DSS_BRAND_DESC = unified_df.DSS_BRAND_DESC,
                        itemMaster.DSS_LIFO_POOL_ID = unified_df.DSS_LIFO_POOL_ID,
                        itemMaster.DSS_GROUP_TYPE = unified_df.DSS_GROUP_TYPE,
                        itemMaster.DSS_DATE_UPDATED = unified_df.DSS_DATE_UPDATED,
                        itemMaster.DSS_EQUIVALENT_SIZE_ID = unified_df.DSS_EQUIVALENT_SIZE_ID,
                        itemMaster.DSS_EQUIVALENT_SIZE_DESC = unified_df.DSS_EQUIVALENT_SIZE_DESC,
                        itemMaster.DSS_EQUIV_SIZE = unified_df.DSS_EQUIV_SIZE,
                        itemMaster.DSS_SCAN_TYPE_ID = unified_df.DSS_SCAN_TYPE_ID,
                        itemMaster.DSS_SCAN_TYPE_DESC = unified_df.DSS_SCAN_TYPE_DESC,
                        itemMaster.DSS_STORE_HANDLING_CODE = unified_df.DSS_STORE_HANDLING_CODE,
                        itemMaster.DSS_SHC_DESC = unified_df.DSS_SHC_DESC,
                        itemMaster.DSS_GS_RATING_DESC = unified_df.DSS_GS_RATING_DESC,
                        itemMaster.DSS_GS_DATE_RATED = unified_df.DSS_GS_DATE_RATED,
                        itemMaster.DSS_PRIVATE_LABEL_ID = unified_df.DSS_PRIVATE_LABEL_ID,
                        itemMaster.DSS_PRIVATE_LABEL_DESC = unified_df.DSS_PRIVATE_LABEL_DESC,
                        itemMaster.DSS_PRODUCT_DIMESION_L_BYTE = unified_df.DSS_PRODUCT_DIMESION_L_BYTE,
                        itemMaster.DSS_PRDT_NAME = unified_df.DSS_PRDT_NAME,
                        itemMaster.DSS_MFR_NAME = unified_df.DSS_MFR_NAME,
                        itemMaster.DSS_BRAND_NAME = unified_df.DSS_BRAND_NAME,
                        itemMaster.DSS_KEYWORD = unified_df.DSS_KEYWORD,
                        itemMaster.DSS_META_DESC = unified_df.DSS_META_DESC,
                        itemMaster.DSS_META_KEYWORD = unified_df.DSS_META_KEYWORD,
                        itemMaster.DSS_SRVG_SZ_DSC = unified_df.DSS_SRVG_SZ_DSC,
                        itemMaster.DSS_SRVG_SZ_UOM_DSC = unified_df.DSS_SRVG_SZ_UOM_DSC,
                        itemMaster.DSS_SRVG_PER_CTNR = unified_df.DSS_SRVG_PER_CTNR,
                        itemMaster.DSS_NEW_PRDT_FLG = unified_df.DSS_NEW_PRDT_FLG,
                        itemMaster.DSS_OVRLN_DSC = unified_df.DSS_OVRLN_DSC,
                        itemMaster.DSS_ALT_SRVG_SZ_DSC = unified_df.DSS_ALT_SRVG_SZ_DSC,
                        itemMaster.DSS_ALT_SRVG_SZ_UOM = unified_df.DSS_ALT_SRVG_SZ_UOM,
                        itemMaster.DSS_AVG_UNIT_WT = unified_df.DSS_AVG_UNIT_WT,
                        itemMaster.DSS_PRIVATE_BRAND_CD = unified_df.DSS_PRIVATE_BRAND_CD,
                        itemMaster.DSS_MENU_LBL_FLG = unified_df.DSS_MENU_LBL_FLG,
                        itemMaster.DSS_DC_ITEM_NUMBER = unified_df.DSS_DC_ITEM_NUMBER,
                        itemMaster.INVENTORY_CODE = unified_df.INVENTORY_CODE,
                        itemMaster.INVENTORY_QTY_ON_HAND = unified_df.INVENTORY_QTY_ON_HAND,
                        itemMaster.INVENTORY_SUPER_CATEGORY = unified_df.INVENTORY_SUPER_CATEGORY,
                        itemMaster.INVENTORY_MAJOR_CATEGORY = unified_df.INVENTORY_MAJOR_CATEGORY,
                        itemMaster.INVENTORY_INTMD_CATEGORY = unified_df.INVENTORY_INTMD_CATEGORY,
                        itemMaster.PRD2STORE_PROMO_CODE = unified_df.PRD2STORE_PROMO_CODE,
                        itemMaster.PRD2STORE_SPLN_AD_CD = unified_df.PRD2STORE_SPLN_AD_CD,
                        itemMaster.PRD2STORE_TAG_TYP_CD = unified_df.PRD2STORE_TAG_TYP_CD,
                        itemMaster.PRD2STORE_WINE_VALUE_FLAG = unified_df.PRD2STORE_WINE_VALUE_FLAG,
                        itemMaster.PRD2STORE_BUY_QTY = unified_df.PRD2STORE_BUY_QTY,
                        itemMaster.PRD2STORE_LMT_QTY = unified_df.PRD2STORE_LMT_QTY,
                        itemMaster.PRD2STORE_SALE_STRT_DT = unified_df.PRD2STORE_SALE_STRT_DT,
                        itemMaster.PRD2STORE_SALE_END_DT = unified_df.PRD2STORE_SALE_END_DT,
                        itemMaster.PRD2STORE_AGE_FLAG = unified_df.PRD2STORE_AGE_FLAG,
                        itemMaster.PRD2STORE_AGE_CODE = unified_df.PRD2STORE_AGE_CODE,
                        itemMaster.PRD2STORE_SWAP_SAVE_UPC = unified_df.PRD2STORE_SWAP_SAVE_UPC,
                        itemMaster.PRICE_UNIT_PRICE = unified_df.PRICE_UNIT_PRICE,
                        itemMaster.PRICE_UOM_CD = unified_df.PRICE_UOM_CD,
                        itemMaster.PRICE_MLT_Quantity = unified_df.PRICE_MLT_Quantity,
                        itemMaster.PRICE_PROMO_RTL_PRC = unified_df.PRICE_PROMO_RTL_PRC,
                        itemMaster.PRICE_PROMO_UNIT_PRICE = unified_df.PRICE_PROMO_UNIT_PRICE,
                        itemMaster.PRICE_Promotional_Quantity = unified_df.PRICE_Promotional_Quantity,
                        itemMaster.PRICE_START_DATE = unified_df.PRICE_START_DATE,
                        itemMaster.PRICE_END_DATE = unified_df.PRICE_END_DATE,
                        itemMaster.TPRX001_STORE_SID_NBR = unified_df.TPRX001_STORE_SID_NBR,
                        itemMaster.TPRX001_ITEM_NBR = unified_df.TPRX001_ITEM_NBR,
                        itemMaster.TPRX001_ITEM_SRC_CD = unified_df.TPRX001_ITEM_SRC_CD,
                        itemMaster.TPRX001_CPN_SRC_CD = unified_df.TPRX001_CPN_SRC_CD,
                        itemMaster.TPRX001_RTL_PRC_EFF_DT = unified_df.TPRX001_RTL_PRC_EFF_DT,
                        itemMaster.TPRX001_ITEM_PROMO_FLG = unified_df.TPRX001_ITEM_PROMO_FLG,
                        itemMaster.TPRX001_PROMO_TYP_CD = unified_df.TPRX001_PROMO_TYP_CD,
                        itemMaster.TPRX001_AD_TYP_CD = unified_df.TPRX001_AD_TYP_CD,
                        itemMaster.TPRX001_PROMO_DSC = unified_df.TPRX001_PROMO_DSC,
                        itemMaster.TPRX001_MIX_MTCH_FLG = unified_df.TPRX001_MIX_MTCH_FLG,
                        itemMaster.TPRX001_PRC_STRAT_CD = unified_df.TPRX001_PRC_STRAT_CD,
                        itemMaster.TPRX001_LOYAL_CRD_FLG = unified_df.TPRX001_LOYAL_CRD_FLG,
                        itemMaster.TPRX001_SCAN_AUTH_FLG = unified_df.TPRX001_SCAN_AUTH_FLG,
                        itemMaster.TPRX001_MDSE_AUTH_FLG = unified_df.TPRX001_MDSE_AUTH_FLG,
                        itemMaster.TPRX001_SBT_FLG = unified_df.TPRX001_SBT_FLG,
                        itemMaster.TPRX001_SBT_VEND_ID = unified_df.TPRX001_SBT_VEND_ID,
                        itemMaster.TPRX001_SBT_VEND_NET_CST = unified_df.TPRX001_SBT_VEND_NET_CST,
                        itemMaster.TPRX001_SCAN_DAUTH_DT = unified_df.TPRX001_SCAN_DAUTH_DT,
                        itemMaster.TPRX001_SCAN_PRVWK_RTL_MLT = unified_df.TPRX001_SCAN_PRVWK_RTL_MLT,
                        itemMaster.TPRX001_SCAN_PRVWK_RTL_PRC = unified_df.TPRX001_SCAN_PRVWK_RTL_PRC,
                        itemMaster.TPRX001_SCANPRVDAY_RTL_MLT = unified_df.TPRX001_SCANPRVDAY_RTL_MLT,
                        itemMaster.TPRX001_SCANPRVDAY_RTL_PRC = unified_df.TPRX001_SCANPRVDAY_RTL_PRC,
                        itemMaster.TPRX001_TAG_PRV_WK_RTL_MLT = unified_df.TPRX001_TAG_PRV_WK_RTL_MLT,
                        itemMaster.TPRX001_TAG_PRV_WK_RTL_PRC = unified_df.TPRX001_TAG_PRV_WK_RTL_PRC,
                        itemMaster.SCRTX_DET_FREQ_SHOP_TYPE = COALESCE(unified_df.SCRTX_DET_FREQ_SHOP_TYPE, itemMaster.SCRTX_DET_FREQ_SHOP_TYPE),
                        itemMaster.SCRTX_DET_FREQ_SHOP_VAL = COALESCE(unified_df.SCRTX_DET_FREQ_SHOP_VAL, itemMaster.SCRTX_DET_FREQ_SHOP_VAL),
                        itemMaster.SCRTX_DET_QTY_RQRD_FG = COALESCE(unified_df.SCRTX_DET_QTY_RQRD_FG, itemMaster.SCRTX_DET_QTY_RQRD_FG),
                        itemMaster.SCRTX_DET_MIX_MATCH_CD = COALESCE(unified_df.SCRTX_DET_MIX_MATCH_CD, itemMaster.SCRTX_DET_MIX_MATCH_CD),
                        itemMaster.SCRTX_DET_CENTRAL_ITEM = COALESCE(unified_df.SCRTX_DET_CENTRAL_ITEM, itemMaster.SCRTX_DET_CENTRAL_ITEM),
                        itemMaster.SCRTX_DET_OP_CODE = COALESCE(unified_df.SCRTX_DET_OP_CODE, itemMaster.SCRTX_DET_OP_CODE),
                        itemMaster.SCRTX_DET_NON_MDSE_ID = COALESCE(unified_df.SCRTX_DET_NON_MDSE_ID, itemMaster.SCRTX_DET_NON_MDSE_ID),
                        itemMaster.SCRTX_DET_RCPT_DESCR = COALESCE(unified_df.SCRTX_DET_RCPT_DESCR, itemMaster.SCRTX_DET_RCPT_DESCR),
                        itemMaster.SCRTX_DET_NG_ENTRY_FG = COALESCE(unified_df.SCRTX_DET_NG_ENTRY_FG, itemMaster.SCRTX_DET_NG_ENTRY_FG),
                        itemMaster.SCRTX_DET_STR_CPN_FG = COALESCE(unified_df.SCRTX_DET_STR_CPN_FG, itemMaster.SCRTX_DET_STR_CPN_FG),
                        itemMaster.SCRTX_DET_VEN_CPN_FG = COALESCE(unified_df.SCRTX_DET_VEN_CPN_FG, itemMaster.SCRTX_DET_VEN_CPN_FG),
                        itemMaster.SCRTX_DET_MAN_PRC_FG = COALESCE(unified_df.SCRTX_DET_MAN_PRC_FG, itemMaster.SCRTX_DET_MAN_PRC_FG),
                        itemMaster.SCRTX_DET_WIC_CVV_FG = COALESCE(unified_df.SCRTX_DET_WIC_CVV_FG, itemMaster.SCRTX_DET_WIC_CVV_FG),
                        itemMaster.SCRTX_DET_DEA_GRP = COALESCE(unified_df.SCRTX_DET_DEA_GRP, itemMaster.SCRTX_DET_DEA_GRP),
                        itemMaster.SCRTX_DET_COMP_TYPE = COALESCE(unified_df.SCRTX_DET_COMP_TYPE, itemMaster.SCRTX_DET_COMP_TYPE),
                        itemMaster.SCRTX_DET_COMP_PRC = COALESCE(unified_df.SCRTX_DET_COMP_PRC, itemMaster.SCRTX_DET_COMP_PRC),
                        itemMaster.SCRTX_DET_COMP_QTY = COALESCE(unified_df.SCRTX_DET_COMP_QTY, itemMaster.SCRTX_DET_COMP_QTY),
                        itemMaster.SCRTX_DET_BLK_GRP = COALESCE(unified_df.SCRTX_DET_BLK_GRP, itemMaster.SCRTX_DET_BLK_GRP),
                        itemMaster.SCRTX_DET_RSTRCSALE_BRCD_FG = COALESCE(unified_df.SCRTX_DET_RSTRCSALE_BRCD_FG, itemMaster.SCRTX_DET_RSTRCSALE_BRCD_FG),
                        itemMaster.SCRTX_DET_RX_FG = COALESCE(unified_df.SCRTX_DET_RX_FG, itemMaster.SCRTX_DET_RX_FG),
                        itemMaster.SCRTX_DET_INTRNL_ID = COALESCE(unified_df.SCRTX_DET_INTRNL_ID, itemMaster.SCRTX_DET_INTRNL_ID),
                        itemMaster.SCRTX_DET_NON_RX_HEALTH_FG = COALESCE(unified_df.SCRTX_DET_NON_RX_HEALTH_FG, itemMaster.SCRTX_DET_NON_RX_HEALTH_FG),
                        itemMaster.SCRTX_DET_PLU_BTCH_NBR = COALESCE(unified_df.SCRTX_DET_PLU_BTCH_NBR, itemMaster.SCRTX_DET_PLU_BTCH_NBR),
                        itemMaster.SCRTX_DET_SLS_RESTRICT_GRP = COALESCE(unified_df.SCRTX_DET_SLS_RESTRICT_GRP, itemMaster.SCRTX_DET_SLS_RESTRICT_GRP),
                        itemMaster.RTX_TYPE = COALESCE(unified_df.RTX_TYPE, itemMaster.RTX_TYPE),
                        itemMaster.SMA_ITEM_STATUS = COALESCE(unified_df.SMA_ITEM_STATUS, itemMaster.SMA_ITEM_STATUS),
                        itemMaster.BTL_DPST_AMT = COALESCE(unified_df.BTL_DPST_AMT, itemMaster.BTL_DPST_AMT),
                        itemMaster.PERF_DETL_SUB_TYPE = unified_df.PERF_DETL_SUB_TYPE,
                        itemMaster.SALE_PRICE = (CASE WHEN unified_df.SALE_PRICE = '0000000.00' 
                                                      then NULL
                                                      else COALESCE(unified_df.SALE_PRICE, itemMaster.SALE_PRICE)
                                                 END),
                        itemMaster.SALE_QUANTITY = (CASE WHEN unified_df.SALE_QUANTITY = '0' 
                                                         then NULL
                                                         else COALESCE(unified_df.SALE_QUANTITY, itemMaster.SALE_QUANTITY)
                                                    END),
                        itemMaster.SMA_LINK_APPLY_DATE = unified_df.SMA_LINK_APPLY_DATE,
                        itemMaster.SMA_LINK_END_DATE = unified_df.SMA_LINK_END_DATE,
                        itemMaster.SMA_LINK_START_DATE = unified_df.SMA_LINK_START_DATE,
                        itemMaster.EFF_TS = unified_df.EFF_TS,
                        itemMaster.LAST_UPDATE_ID = unified_df.LAST_UPDATE_ID,
                        itemMaster.LAST_UPDATE_TIMESTAMP = unified_df.LAST_UPDATE_TIMESTAMP
                  WHEN NOT MATCHED THEN INSERT * '''.format(itemMasterDeltaPath))
    appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(itemMasterDeltaPath))
    loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
    appended_recs = appended_recs.head(1)
    ABC(DeltaTableFinalCount=appended_recs[0][0])
    spark.catalog.dropTempView(temp_table_name)
  except Exception as ex:
    loggerAtt.info("Merge into Delta table failed and throwed error")
    loggerAtt.error(str(ex))
    ABC(DeltaTableInitCount='')
    ABC(DeltaTableFinalCount='')
    err = ErrorReturn('Error', ex,'upsertItemRecordsDaily')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  loggerAtt.info("Merge into Delta table successful - upsertItemRecordsDaily")    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Update/Insert record for Intraday file

# COMMAND ----------

def upsertItemRecordsIntraDay(unified_df, itemMasterDeltaPath):
  loggerAtt.info("Merge into Delta table initiated - upsertItemRecordsIntraDay")
  try:
    unified_df = unified_df.withColumn("LAST_UPDATE_TIMESTAMP", lit(currentTimeStamp).cast(TimestampType()))
    unified_df = unified_df.withColumn("LAST_UPDATE_ID", lit(pipelineid))
    
    temp_table_name = "unified_df"
    unified_df.createOrReplaceTempView(temp_table_name)
    
    loggerAtt.info(f"Count of unified_df: {unified_df.count()}")
    
    initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(itemMasterDeltaPath))
    loggerAtt.info(f"Initial count of records in Delta Table: {initial_recs.head(1)}")
    initial_recs = initial_recs.head(1)
    
    ABC(DeltaTableInitCount=initial_recs[0][0])
    spark.sql('''MERGE INTO delta.`{}` as itemMaster
    USING unified_df 
    ON itemMaster.SMA_GTIN_NUM = unified_df.SMA_GTIN_NUM and
       itemMaster.SMA_DEST_STORE = unified_df.SMA_DEST_STORE and
       itemMaster.PRD2STORE_BANNER_ID = unified_df.PRD2STORE_BANNER_ID
    WHEN MATCHED Then 
            Update Set  itemMaster.SMA_BATCH_SERIAL_NBR = COALESCE(unified_df.SMA_BATCH_SERIAL_NBR, itemMaster.SMA_BATCH_SERIAL_NBR),
                        itemMaster.SMA_TAX_1 = COALESCE(unified_df.SMA_TAX_1, itemMaster.SMA_TAX_1),
                        itemMaster.SMA_TAX_2 = COALESCE(unified_df.SMA_TAX_2, itemMaster.SMA_TAX_2),
                        itemMaster.SMA_TAX_3 = COALESCE(unified_df.SMA_TAX_3, itemMaster.SMA_TAX_3),
                        itemMaster.SMA_TAX_4 = COALESCE(unified_df.SMA_TAX_4, itemMaster.SMA_TAX_4),
                        itemMaster.SMA_TAX_5 = COALESCE(unified_df.SMA_TAX_5, itemMaster.SMA_TAX_5),
                        itemMaster.SMA_TAX_6 = COALESCE(unified_df.SMA_TAX_6, itemMaster.SMA_TAX_6),
                        itemMaster.SMA_TAX_7 = COALESCE(unified_df.SMA_TAX_7, itemMaster.SMA_TAX_7),
                        itemMaster.SMA_TAX_8 = COALESCE(unified_df.SMA_TAX_8, itemMaster.SMA_TAX_8),
                        itemMaster.SMA_VEND_COUPON_FAM1 = COALESCE(unified_df.SMA_VEND_COUPON_FAM1, itemMaster.SMA_VEND_COUPON_FAM1),
                        itemMaster.SMA_VEND_COUPON_FAM2 = COALESCE(unified_df.SMA_VEND_COUPON_FAM2, itemMaster.SMA_VEND_COUPON_FAM2),
                        itemMaster.SMA_VEND_NUM = COALESCE(unified_df.SMA_VEND_NUM, itemMaster.SMA_VEND_NUM),
                        itemMaster.SMA_WIC_IND = COALESCE(unified_df.SMA_WIC_IND, itemMaster.SMA_WIC_IND),
                        itemMaster.SMA_UPC_DESC = COALESCE(unified_df.SMA_UPC_DESC, itemMaster.SMA_UPC_DESC),
                        itemMaster.SMA_STORE = COALESCE(unified_df.SMA_STORE, itemMaster.SMA_STORE),
                        itemMaster.SMA_SUB_DEPT = COALESCE(unified_df.SMA_SUB_DEPT, itemMaster.SMA_SUB_DEPT),
                        itemMaster.SMA_STATUS_DATE = COALESCE(unified_df.SMA_STATUS_DATE, itemMaster.SMA_STATUS_DATE),
                        itemMaster.SMA_RETL_VENDOR = COALESCE(unified_df.SMA_RETL_VENDOR, itemMaster.SMA_RETL_VENDOR),
                        itemMaster.SMA_SBW_IND = COALESCE(unified_df.SMA_SBW_IND, itemMaster.SMA_SBW_IND),
                        itemMaster.SMA_SELL_RETL = COALESCE(unified_df.SMA_SELL_RETL, itemMaster.SMA_SELL_RETL),
                        itemMaster.SMA_RETL_MULT_UNIT = COALESCE(unified_df.SMA_RETL_MULT_UNIT, itemMaster.SMA_RETL_MULT_UNIT),
                        itemMaster.SMA_RESTR_CODE = COALESCE(unified_df.SMA_RESTR_CODE, itemMaster.SMA_RESTR_CODE),
                        itemMaster.SMA_MULT_UNIT_RETL = COALESCE(unified_df.SMA_MULT_UNIT_RETL, itemMaster.SMA_MULT_UNIT_RETL),
                        itemMaster.SALE_PRICE = COALESCE(unified_df.SALE_PRICE, itemMaster.SALE_PRICE),
                        itemMaster.SMA_LINK_UPC = COALESCE(unified_df.SMA_LINK_UPC, itemMaster.SMA_LINK_UPC),
                        itemMaster.SMA_LINK_HDR_COUPON = COALESCE(unified_df.SMA_LINK_HDR_COUPON, itemMaster.SMA_LINK_HDR_COUPON),
                        itemMaster.SMA_ITEM_DESC = COALESCE(unified_df.SMA_ITEM_DESC, itemMaster.SMA_ITEM_DESC),
                        itemMaster.SMA_FOOD_STAMP_IND = COALESCE(unified_df.SMA_FOOD_STAMP_IND, itemMaster.SMA_FOOD_STAMP_IND),
                        itemMaster.SMA_FIXED_TARE_WGT = COALESCE(unified_df.SMA_FIXED_TARE_WGT, itemMaster.SMA_FIXED_TARE_WGT),
                        itemMaster.SMA_BOTTLE_DEPOSIT_IND = COALESCE(unified_df.SMA_BOTTLE_DEPOSIT_IND, itemMaster.SMA_BOTTLE_DEPOSIT_IND),
                        itemMaster.SMA_BATCH_SUB_DEPT = COALESCE(unified_df.SMA_BATCH_SUB_DEPT, itemMaster.SMA_BATCH_SUB_DEPT),
                        itemMaster.SCRTX_DET_FREQ_SHOP_TYPE = COALESCE(unified_df.SCRTX_DET_FREQ_SHOP_TYPE, itemMaster.SCRTX_DET_FREQ_SHOP_TYPE),
                        itemMaster.SCRTX_DET_FREQ_SHOP_VAL = COALESCE(unified_df.SCRTX_DET_FREQ_SHOP_VAL, itemMaster.SCRTX_DET_FREQ_SHOP_VAL),
                        itemMaster.SCRTX_DET_QTY_RQRD_FG = COALESCE(unified_df.SCRTX_DET_QTY_RQRD_FG, itemMaster.SCRTX_DET_QTY_RQRD_FG),
                        itemMaster.SCRTX_DET_MIX_MATCH_CD = COALESCE(unified_df.SCRTX_DET_MIX_MATCH_CD, itemMaster.SCRTX_DET_MIX_MATCH_CD),
                        itemMaster.SCRTX_DET_CENTRAL_ITEM = COALESCE(unified_df.SCRTX_DET_CENTRAL_ITEM, itemMaster.SCRTX_DET_CENTRAL_ITEM),
                        itemMaster.SCRTX_DET_OP_CODE = COALESCE(unified_df.SCRTX_DET_OP_CODE, itemMaster.SCRTX_DET_OP_CODE),
                        itemMaster.SCRTX_DET_NON_MDSE_ID = COALESCE(unified_df.SCRTX_DET_NON_MDSE_ID, itemMaster.SCRTX_DET_NON_MDSE_ID),
                        itemMaster.SCRTX_DET_RCPT_DESCR = COALESCE(unified_df.SCRTX_DET_RCPT_DESCR, itemMaster.SCRTX_DET_RCPT_DESCR),
                        itemMaster.SCRTX_DET_NG_ENTRY_FG = COALESCE(unified_df.SCRTX_DET_NG_ENTRY_FG, itemMaster.SCRTX_DET_NG_ENTRY_FG),
                        itemMaster.SCRTX_DET_STR_CPN_FG = COALESCE(unified_df.SCRTX_DET_STR_CPN_FG, itemMaster.SCRTX_DET_STR_CPN_FG),
                        itemMaster.SCRTX_DET_VEN_CPN_FG = COALESCE(unified_df.SCRTX_DET_VEN_CPN_FG, itemMaster.SCRTX_DET_VEN_CPN_FG),
                        itemMaster.SCRTX_DET_MAN_PRC_FG = COALESCE(unified_df.SCRTX_DET_MAN_PRC_FG, itemMaster.SCRTX_DET_MAN_PRC_FG),
                        itemMaster.SCRTX_DET_WIC_CVV_FG = COALESCE(unified_df.SCRTX_DET_WIC_CVV_FG, itemMaster.SCRTX_DET_WIC_CVV_FG),
                        itemMaster.SCRTX_DET_DEA_GRP = COALESCE(unified_df.SCRTX_DET_DEA_GRP, itemMaster.SCRTX_DET_DEA_GRP),
                        itemMaster.SCRTX_DET_COMP_TYPE = COALESCE(unified_df.SCRTX_DET_COMP_TYPE, itemMaster.SCRTX_DET_COMP_TYPE),
                        itemMaster.SCRTX_DET_COMP_PRC = COALESCE(unified_df.SCRTX_DET_COMP_PRC, itemMaster.SCRTX_DET_COMP_PRC),
                        itemMaster.SCRTX_DET_COMP_QTY = COALESCE(unified_df.SCRTX_DET_COMP_QTY, itemMaster.SCRTX_DET_COMP_QTY),
                        itemMaster.SCRTX_DET_BLK_GRP = COALESCE(unified_df.SCRTX_DET_BLK_GRP, itemMaster.SCRTX_DET_BLK_GRP),
                        itemMaster.SCRTX_DET_RSTRCSALE_BRCD_FG = COALESCE(unified_df.SCRTX_DET_RSTRCSALE_BRCD_FG, itemMaster.SCRTX_DET_RSTRCSALE_BRCD_FG),
                        itemMaster.SCRTX_DET_RX_FG = COALESCE(unified_df.SCRTX_DET_RX_FG, itemMaster.SCRTX_DET_RX_FG),
                        itemMaster.SCRTX_DET_INTRNL_ID = COALESCE(unified_df.SCRTX_DET_INTRNL_ID, itemMaster.SCRTX_DET_INTRNL_ID),
                        itemMaster.SCRTX_DET_NON_RX_HEALTH_FG = COALESCE(unified_df.SCRTX_DET_NON_RX_HEALTH_FG, itemMaster.SCRTX_DET_NON_RX_HEALTH_FG),
                        itemMaster.SCRTX_DET_PLU_BTCH_NBR = COALESCE(unified_df.SCRTX_DET_PLU_BTCH_NBR, itemMaster.SCRTX_DET_PLU_BTCH_NBR),
                        itemMaster.SCRTX_DET_SLS_RESTRICT_GRP = COALESCE(unified_df.SCRTX_DET_SLS_RESTRICT_GRP, itemMaster.SCRTX_DET_SLS_RESTRICT_GRP),
                        itemMaster.RTX_TYPE = COALESCE(unified_df.RTX_TYPE, itemMaster.RTX_TYPE),
                        itemMaster.SMA_ITEM_STATUS = COALESCE(unified_df.SMA_ITEM_STATUS, itemMaster.SMA_ITEM_STATUS),
                        itemMaster.BTL_DPST_AMT =   COALESCE(unified_df.BTL_DPST_AMT, itemMaster.BTL_DPST_AMT),
                        itemMaster.LAST_UPDATE_ID = unified_df.LAST_UPDATE_ID,
                        itemMaster.LAST_UPDATE_TIMESTAMP = unified_df.LAST_UPDATE_TIMESTAMP'''.format(itemMasterDeltaPath))
    appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(itemMasterDeltaPath))
    loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
    appended_recs = appended_recs.head(1)
    ABC(DeltaTableFinalCount=appended_recs[0][0])
    spark.catalog.dropTempView(temp_table_name)
  except Exception as ex:
    loggerAtt.info("Merge into Delta table failed and throwed error")
    loggerAtt.error(str(ex))
    ABC(DeltaTableInitCount='')
    ABC(DeltaTableFinalCount='')
    err = ErrorReturn('Error', ex,'upsertItemRecordsIntraDay')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  loggerAtt.info("Merge into Delta table successful - upsertItemRecordsIntraDay")    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Item Master Archival

# COMMAND ----------

def itemMasterArchival(itemMasterDeltaPath,Date,itemMasterArchivalpath):
  itemMasterArchivalDf = spark.read.format('delta').load(itemMasterDeltaPath)
  
  initial_recs = itemMasterArchivalDf.count()
  loggerAtt.info(f"Initial count of records in delta table: {initial_recs}")
  ABC(archivalInitCount=initial_recs)
  
  if itemMasterArchivalDf.count() > 0:
    itemMasterArchivalDf = itemMasterArchivalDf.filter(((col("SCRTX_DET_OP_CODE")== '4') & (datediff(to_date(current_date()),to_date(col('LAST_UPDATE_TIMESTAMP'))) >=1)))
    if itemMasterArchivalDf.count() >0:
      itemMasterArchivalDf.write.mode('Append').format('parquet').save(itemMasterArchivalpath + "/" +Date+ "/" +"pos_Archival_Records")
      deltaTable = DeltaTable.forPath(spark, itemDeltaPath)
      deltaTable.delete(((col("SCRTX_DET_OP_CODE")== '4') & (datediff(to_date(current_date()),to_date(col('LAST_UPDATE_TIMESTAMP'))) >=1)))

      after_recs = spark.read.format('delta').load(itemMasterDeltaPath).count()
      loggerAtt.info(f"After count of records in delta table: {after_recs}")
      ABC(archivalAfterCount=after_recs)

      loggerAtt.info('========Item Records Archival successful ========')
    else:
      loggerAtt.info('======== No Item Records Archival Done ========')
  else:
    loggerAtt.info('======== No Item Records Archival Done ========')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Promo Link and Unified Item Join - Sale Price Calculation

# COMMAND ----------

def fetchPromoLinkData(itemMasterDeltaPath, promoLinkingDeltaPath, itemMasterCount):
  loggerAtt.info("Fetching Unified data which link PromoLinking delta table and Item Master initiated")
  try:
    
    promoLinkDf = spark.read.format('delta').load(promoLinkingDeltaPath)
    if itemMasterCount > 0:
      promoLinkDf = promoLinkDf.filter((datediff(to_date(current_date()),to_date(col('LAST_UPDATE_TIMESTAMP'))) >= 0))
    
    initial_recs = promoLinkDf.count()
    loggerAtt.info(f"Unified count items going for Join Table: {initial_recs}")
    
    temp_table_name = "promoLinkDf"
    promoLinkDf.createOrReplaceTempView(temp_table_name)
    
    ABC(UnifiedCurrentPromoBeforeJoinCount=initial_recs)
    unifiedPromo_df = spark.sql("""
                  SELECT  SMA_MULT_UNIT_RETL,
                          SMA_GTIN_NUM,
                          SMA_DEST_STORE,
                          COUPON_NO,
                          PROMO.PERF_DETL_SUB_TYPE,
                          START_DATE,
                          END_DATE,
                          CHANGE_AMOUNT_PCT,
                          SMA_RETL_MULT_UNIT,
                          PROMO_REWARD_VALUE,
                          PROMO_ENHANCED_THRESHOLD_QTY,
                          PROMO_STATUS,
                          PROMO.LAST_UPDATE_ID,
                          PROMO.LAST_UPDATE_TIMESTAMP
                    FROM promoLinkDf
                     Inner JOIN delta.`{}` AS itemMain ON
                        promoLinkDf.PROMO_ITM_ID = itemMain.SMA_GTIN_NUM AND
                        promoLinkDf.PROMO_STORE_ID = itemMain.SMA_DEST_STORE
                     Inner JOIN delta.`{}` AS PROMO  ON
                        PROMO.COUPON_NO=promoLinkDf.PROMO_COUPON_NO AND 
                        PROMO.LOCATION=promoLinkDf.PROMO_STORE_ID""".format(itemMasterDeltaPath, couponDeltaPath))
    
    appended_recs = unifiedPromo_df.count()
    ABC(UnifiedCurrentPromoAfterJoinCount=appended_recs)
    loggerAtt.info(f"Unified Promo count items after Joining Table: {appended_recs}")
    spark.catalog.dropTempView(temp_table_name)
  except Exception as ex:
    loggerAtt.info("Fetching Unified data which link PromoLinking delta table and Item Master error")
    loggerAtt.error(str(ex))
    ABC(UnifiedCurrentPromoBeforeJoinCount='')
    ABC(UnifiedCurrentPromoAfterJoinCount='')
    ABC(FetchPromoCheck=0)
    err = ErrorReturn('Error', ex,'fetchPromoLinkData')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  loggerAtt.info("Fetching Unified data which link PromoLinking delta table and Item Master successful")    
  return unifiedPromo_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Item Master with Sale Price Calculation

# COMMAND ----------

def updatePromoRecords(unifiedPromoLinkDf, itemMasterDeltaPath):
  loggerAtt.info("Update into Item Master table with Promo Record initiated")
  try:
    unifiedPromoLinkDf = unifiedPromoLinkDf.withColumn("LAST_UPDATE_TIMESTAMP", lit(currentTimeStamp).cast(TimestampType()))
    unifiedPromoLinkDf = unifiedPromoLinkDf.withColumn("LAST_UPDATE_ID", lit(pipelineid))
    temp_table_name = "unifiedPromoLinkDf"
    unifiedPromoLinkDf.createOrReplaceTempView(temp_table_name)
    
    loggerAtt.info(f"Count of unifiedPromoLinkDf: {unifiedPromoLinkDf.count()}")
    
    initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(itemMasterDeltaPath))
    loggerAtt.info(f"Initial count of records in Delta Table: {initial_recs.head(1)}")
    initial_recs = initial_recs.head(1)
    
    ABC(DeltaTableInitCount=initial_recs[0][0])
    
    spark.sql('''MERGE INTO delta.`{}` as itemMaster
    USING unifiedPromoLinkDf 
    ON itemMaster.SMA_GTIN_NUM = unifiedPromoLinkDf.SMA_GTIN_NUM and
       itemMaster.SMA_DEST_STORE = unifiedPromoLinkDf.SMA_DEST_STORE and
       PROMO_STATUS = 'D'
    WHEN MATCHED Then 
            Update Set  itemMaster.SALE_PRICE = NULL,
                        itemMaster.SALE_QUANTITY = NULL,
                        itemMaster.SMA_LINK_START_DATE = NULL,
                        itemMaster.SMA_LINK_END_DATE = NULL,
                        itemMaster.SMA_LINK_APPLY_DATE = NULL,
                        itemMaster.PERF_DETL_SUB_TYPE = NULL,
                        itemMaster.SMA_LINK_HDR_COUPON = NULL,
                        itemMaster.LAST_UPDATE_ID = unifiedPromoLinkDf.LAST_UPDATE_ID,
                        itemMaster.LAST_UPDATE_TIMESTAMP = unifiedPromoLinkDf.LAST_UPDATE_TIMESTAMP'''.format(itemMasterDeltaPath))
    ABC(promoLinkDUpdate=1)
    spark.sql('''MERGE INTO delta.`{}` as itemMaster
    USING unifiedPromoLinkDf 
    ON itemMaster.SMA_GTIN_NUM = unifiedPromoLinkDf.SMA_GTIN_NUM and
       itemMaster.SMA_DEST_STORE = unifiedPromoLinkDf.SMA_DEST_STORE and
       PROMO_STATUS = 'C'
    WHEN MATCHED Then 
            Update Set  itemMaster.SALE_PRICE = unifiedPromoLinkDf.SALE_PRICE,
                        itemMaster.SALE_QUANTITY = unifiedPromoLinkDf.SALE_QUANTITY,
                        itemMaster.SMA_LINK_START_DATE = unifiedPromoLinkDf.SMA_LINK_START_DATE,
                        itemMaster.SMA_LINK_END_DATE = unifiedPromoLinkDf.SMA_LINK_END_DATE,
                        itemMaster.SMA_LINK_APPLY_DATE = unifiedPromoLinkDf.SMA_LINK_APPLY_DATE,
                        itemMaster.PERF_DETL_SUB_TYPE = unifiedPromoLinkDf.PERF_DETL_SUB_TYPE,
                        itemMaster.SMA_LINK_HDR_COUPON = unifiedPromoLinkDf.COUPON_NO,
                        itemMaster.LAST_UPDATE_ID = unifiedPromoLinkDf.LAST_UPDATE_ID,
                        itemMaster.LAST_UPDATE_TIMESTAMP = unifiedPromoLinkDf.LAST_UPDATE_TIMESTAMP'''.format(itemMasterDeltaPath))
    ABC(promoLinkCUpdate=1)
    spark.sql('''MERGE INTO delta.`{}` as itemMaster
    USING unifiedPromoLinkDf 
    ON itemMaster.SMA_GTIN_NUM = unifiedPromoLinkDf.SMA_GTIN_NUM and
       itemMaster.SMA_DEST_STORE = unifiedPromoLinkDf.SMA_DEST_STORE and
       PROMO_STATUS = 'M'
    WHEN MATCHED Then 
            Update Set  itemMaster.SALE_PRICE = unifiedPromoLinkDf.SALE_PRICE,
                        itemMaster.SALE_QUANTITY = unifiedPromoLinkDf.SALE_QUANTITY,
                        itemMaster.SMA_LINK_START_DATE = unifiedPromoLinkDf.SMA_LINK_START_DATE,
                        itemMaster.SMA_LINK_END_DATE = unifiedPromoLinkDf.SMA_LINK_END_DATE,
                        itemMaster.SMA_LINK_APPLY_DATE = unifiedPromoLinkDf.SMA_LINK_APPLY_DATE,
                        itemMaster.PERF_DETL_SUB_TYPE = unifiedPromoLinkDf.PERF_DETL_SUB_TYPE,
                        itemMaster.SMA_LINK_HDR_COUPON = unifiedPromoLinkDf.COUPON_NO,
                        itemMaster.LAST_UPDATE_ID = unifiedPromoLinkDf.LAST_UPDATE_ID,
                        itemMaster.LAST_UPDATE_TIMESTAMP = unifiedPromoLinkDf.LAST_UPDATE_TIMESTAMP'''.format(itemMasterDeltaPath))
    

    ABC(promoLinkMUpdate=1)
    appended_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(itemMasterDeltaPath))
    loggerAtt.info(f"After Appending count of records in Delta Table: {appended_recs.head(1)}")
    appended_recs = appended_recs.head(1)
    ABC(DeltaTableFinalCount=appended_recs[0][0])
    spark.catalog.dropTempView(temp_table_name)
  except Exception as ex:
    loggerAtt.info("Update into Item Master table with Promo Record error")
    loggerAtt.error(str(ex))
    ABC(DeltaTableInitCount='')
    ABC(DeltaTableFinalCount='')
    err = ErrorReturn('Error', ex,'updatePromoRecords')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  loggerAtt.info("Update into Item Master table with Promo Record successful")    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Archive previous day Item_Main records

# COMMAND ----------

def archivePreviousDayItemMain(itemMainDeltaPath, itemMainArchivalpath):
  loggerAtt.info("Delete previous day Item_Main - archivePreviousDayItemMain initiated")
  try:
    itemMainDf = spark.read.format('delta').load(itemMainDeltaPath)
    
    initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(itemMainDeltaPath))
    loggerAtt.info(f"Initial count of records in Item_Main Delta Table: {initial_recs.head(1)}")
    initial_recs = initial_recs.head(1)
    
    ABC(Item_MainArchivalBeforeCount=initial_recs[0][0])
    
    if itemMainDf.count() > 0:
      itemMainDf = itemMainDf.filter(((datediff(to_date(current_date()),to_date(col('SCRTX_HDR_ACT_DATE'))) >=1)))
      if itemMainDf.count() >0:
        loggerAtt.info("Archival records are present")
        itemMainDf.write.mode('Append').format('parquet').save(itemMainArchivalpath)
        
        deltaTable = DeltaTable.forPath(spark, itemMainDeltaPath)
        deltaTable.delete((datediff(to_date(current_date()),to_date(col('SCRTX_HDR_ACT_DATE'))) >=1))
    else:
      loggerAtt.info("No Archival records are present")
      
    after_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(itemMainDeltaPath))
    loggerAtt.info(f"After count of records in delta table: {after_recs.head(1)}")
    after_recs = after_recs.head(1)
    ABC(Item_MainArchivalAfterCount=after_recs[0][0])
  except Exception as ex:
    loggerAtt.info("Delete previous day Item_Main - archivePreviousDayItemMain error")
    loggerAtt.error(str(ex))
    ABC(archivePreviousDayItemMainCheck=0)
    ABC(DeltaTableInitCount='')
    ABC(archivalAfterCount='')
    err = ErrorReturn('Error', ex,'upsertItemRecordsIntraDay')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  loggerAtt.info("Delete previous day Item_Main - archivePreviousDayItemMain Successful")    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Archive record with status D on Promotion Linking records

# COMMAND ----------

def archivePromoLink(promotionLinkingArchivalpath, promoLinkingDeltaPath):
  loggerAtt.info("Delete record with status D - archivePromoLink initiated")
  try:
    promoLinkDf = spark.read.format('delta').load(promoLinkingDeltaPath)
    
    initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(promoLinkingDeltaPath))
    loggerAtt.info(f"Initial count of records in Promo Linking Delta Table: {initial_recs.head(1)}")
    initial_recs = initial_recs.head(1)
    ABC(promoLinkingArchivalBeforeCount=initial_recs[0][0])
    
    if promoLinkDf.count() > 0:
      promoLinkDf = promoLinkDf.filter((col("PROMO_STATUS") == 'D'))
      if promoLinkDf.count() > 0:
        loggerAtt.info("No of delete records Promotion Linking " + str(promoLinkDf.count()))
        loggerAtt.info("Archival records are present")
        
        promoLinkDf.write.mode('Append').format('parquet').save(promotionLinkingArchivalpath)
        
        deltaTable = DeltaTable.forPath(spark, promoLinkingDeltaPath)
        deltaTable.delete((col("PROMO_STATUS") == 'D'))
    else:
      loggerAtt.info("No Archival records are present")
      
    after_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(promoLinkingDeltaPath))
    loggerAtt.info(f"After count of records in delta table: {after_recs.head(1)}")
    after_recs = after_recs.head(1)
    ABC(promoLinkingArchivalAfterCount=after_recs[0][0])
  except Exception as ex:
    loggerAtt.info("Delete record with status D - archivePromoLink error")
    loggerAtt.error(str(ex))
    ABC(promoLinkingArchivalBeforeCount='')
    ABC(promoLinkingArchivalAfterCount='')
    ABC(archivePromoLinkCheck=0)
    err = ErrorReturn('Error', ex,'archivePromoLink')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  loggerAtt.info("Delete record with status D - archivePromoLink successful")    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Archive Item_Master records

# COMMAND ----------

def archivePreviousDayItemMaster(itemMasterDeltaPath, itemMasterArchivalpath):
  loggerAtt.info("Delete Item Master with status 'D' - archivePreviousDayItemMaster initiated")
  try:
    itemMasterDf = spark.read.format('delta').load(itemMasterDeltaPath)
    
    initial_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(itemMasterDeltaPath))
    loggerAtt.info(f"Initial count of records in Item_Main Delta Table: {initial_recs.head(1)}")
    initial_recs = initial_recs.head(1)
    
    ABC(itemMasterArchivalBeforeCount=initial_recs[0][0])
    
    if itemMasterDf.count() > 0:
      itemMasterDf = itemMasterDf.filter((col("SMA_ITEM_STATUS") == 'D'))
      if itemMasterDf.count() >0:
        loggerAtt.info("No of delete records Item Master" + str(itemMasterDf.count()))
        loggerAtt.info("Archival records are present")
        itemMasterDf.write.mode('Append').format('parquet').save(itemMasterArchivalpath)
        
        deltaTable = DeltaTable.forPath(spark, itemMasterDeltaPath)
        deltaTable.delete((col("SMA_ITEM_STATUS") == 'D'))
    else:
      loggerAtt.info("No Archival records are present")
      
    after_recs = spark.sql("""SELECT count(*) as count from delta.`{}`;""".format(itemMasterDeltaPath))
    loggerAtt.info(f"After count of records in delta table: {after_recs.head(1)}")
    after_recs = after_recs.head(1)
    ABC(itemMasterArchivalAfterCount=after_recs[0][0])
  except Exception as ex:
    loggerAtt.info("Delete Item Master with status 'D' - archivePreviousDayItemMaster error")
    loggerAtt.error(str(ex))
    ABC(itemMasterArchivalBeforeCount='')
    ABC(itemMasterArchivalAfterCount='')
    ABC(archivePreviousDayItemMasterCheck=0)
    err = ErrorReturn('Error', ex,'archivePreviousDayItemMaster')
    errJson = jsonpickle.encode(err)
    errJson = json.loads(errJson)
    dbutils.notebook.exit(Merge(ABCChecks,errJson))
  loggerAtt.info("Delete Item Master with status 'D' - archivePreviousDayItemMaster Successful")    