# Databricks notebook source
# MAGIC %md
# MAGIC ## Very Important
# MAGIC 
# MAGIC #### Call this notebook from the itemmaster notebook before performing the itemMasterWrite() function to write data from delta to CDM.

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/ahold-centralized-price-promo/WeeklyAdfeed/Inbound/RDS/2022/01/17/")

# COMMAND ----------

### Step 1:
## update Set all the weekly Ad related columns in itemMasterAhold to Null with Non-null values in key ID



# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType

# COMMAND ----------

### Step 2:
## Read the weekly ad data into a DF


# COMMAND ----------

df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter", "|").load("dbfs:/mnt/ahold-centralized-price-promo/WeeklyAdfeed/Inbound/RDS/2022/01/17/*")

display(df)
df.createOrReplaceTempView("Weekly_Stage")

# COMMAND ----------

df = spark.sql("""SELECT *, TO_DATE(CAST(UNIX_TIMESTAMP(SUBSTR(EVENT_NAME, 0, 10), 'MM-dd-yyyy') AS TIMESTAMP)) AS AD_PROMO_START_DATE FROM Weekly_Stage""")
df.createOrReplaceTempView("weeklyAd")


# COMMAND ----------

df = spark.sql("""SELECT *, TO_DATE(date_add(CAST(UNIX_TIMESTAMP(AD_PROMO_START_DATE, 'MM-dd-yyyy') AS TIMESTAMP), 6)) AS AD_PROMO_END_DATE FROM weeklyAd""")
#df = df.withColumn("AD_PROMO_END_DATE", calculateDate(col("EVENT_NAME")))
df.createOrReplaceTempView("weeklyAd1")

# COMMAND ----------


df1= spark.sql("""select Distinct lpad(Substring(ATTR_AUSA_ITEM_NO,0,10),12,'0') as item,lpad(CUSTOMERLOCATIONKEY,4,'0') as CUSTOMERLOCATIONKEY  from weeklyAd1""")
df1.createOrReplaceTempView("df1")

# COMMAND ----------

# MAGIC %sql
# MAGIC Desc weeklyAd_Delta

# COMMAND ----------

# Weekly AD Delta Table from other Notebook to get the Distinct Store & Item

# df1= spark.sql("""select Distinct lpad(Substring(ITEM_NUMBER,0,10),12,'0') as item,lpad(STORE_NUMBER,4,'0') as CUSTOMERLOCATIONKEY  from weeklyAd_Delta""")
# df1.createOrReplaceTempView("df1")

# COMMAND ----------

# Weekly AD Delta Table from other Notebook to get the Distinct Store & Item

df1= spark.sql("""select  lpad(Substring(ITEM_NUMBER,0,10),12,'0') as item,lpad(STORE_NUMBER,4,'0') as CUSTOMERLOCATIONKEY,*  from weeklyAd_Delta""")
df1.createOrReplaceTempView("df1")

# COMMAND ----------

# MAGIC %sql SELECT * FROM df1 WHERE CUSTOMERLOCATIONKEY LIKE '0169' AND item like '000000523124'

# COMMAND ----------

# MAGIC %sql SELECT CUSTOMERLOCATIONKEY, item, COUNT(*) FROM df1 GROUP BY  CUSTOMERLOCATIONKEY, item HAVING COUNT(*)> 1

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

windowSpec = Window.partitionBy(["customerlocationkey", "item"]).orderBy(desc("AD_PROMO_START_DATE"))
dfx = df1.withColumn("row_number", row_number().over(windowSpec))
dfx = dfx.filter(dfx.row_number == 1)
dfx = dfx.drop('row_number')
dfx.createOrReplaceTempView("dfx")

df2= spark.sql("""select distinct * from dfx """)
df2.createOrReplaceTempView("df2")
#150706
#165692

# COMMAND ----------

# MAGIC %sql SELECT CUSTOMERLOCATIONKEY, item, COUNT(*) FROM df2 GROUP BY  CUSTOMERLOCATIONKEY, item HAVING COUNT(*)> 1

# COMMAND ----------


df3= spark.sql(""" select ITM.* from df2 inner join default.itemmasterAhold ITM on df2.Customerlocationkey=ITM.SMA_DEST_STORE
 and df2.item=ITM.SMA_LEGACY_ITEM """)
df3.createOrReplaceTempView("df3")

# COMMAND ----------


df4= spark.sql("""select df3.* from  df1 inner join  df3 on df1.CUSTOMERLOCATIONKEY=df3.SMA_DEST_STORE and df1.item=df3.SMA_LEGACY_ITEM""")
df4.createOrReplaceTempView("df4")

# COMMAND ----------

# MAGIC %md Another step to set previous values to Nulls in itemmasterahold

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- df5=spark.sql("""
# MAGIC Merge into default.itemmasterAhold df4  
# MAGIC using df2 df1 
# MAGIC on df1.customerlocationkey= df4.SMA_DEST_STORE
# MAGIC and df1.item= df4.SMA_Legacy_Item
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE SET
# MAGIC   df4.EVENT_NAME=df1.EVENT_NAME,
# MAGIC   df4.AD_WEEK=df1.AD_WEEK,
# MAGIC   df4.BRAND_DIVISION=df1.DIVISION,
# MAGIC   df4.MARKET_SEGMENT=df1.MARKET,
# MAGIC   df4.ATTR_PAGE =df1.ATTR_PAGE,
# MAGIC   df4.ATTR_BLOCK =df1.ATTR_BLOCK,
# MAGIC   df4.ATTR_AD_PLACEMENT=df1.ATTR_AD_PLACEMENT,
# MAGIC   df4.ATTR_AD_FORMAT=df1.ATTR_AD_FORMAT,
# MAGIC   df4.ATTR_HEADLINE=df1.ATTR_HEADLINE,
# MAGIC   df4.ATTR_SUBHEADLINE =df1.ATTR_SUBHEADLINE,
# MAGIC   df4.ATTR_AUSA_ITEM_NO =df1.item,
# MAGIC   df4.ATTR_PRICE_TYPE_DESC=df1.ATTR_PRICE_TYPE_DESC,
# MAGIC   df4.AD_COMMENTS =df1.AD_COMMENTS,
# MAGIC   df4.SID_COPYCARD =df1.SID_COPYCARD,
# MAGIC   df4.ADDITIONAL_SID =df1.ADDITIONAL_SID,
# MAGIC   df4.SOURCE_PROMOTIONID =df1.SOURCE_PROMOTIONID,
# MAGIC   df4.CustomerlocationKey =df1.CustomerlocationKey,
# MAGIC   df4.DEMANDTEC_PRISM_SID =df1.DEMANDTEC_PRISM_SID,
# MAGIC   df4.INSERT_ID =df1.INSERT_ID,
# MAGIC   df4.INSERT_TIMESTAMP =df1.INSERT_TIMESTAMP
# MAGIC --   """)
# MAGIC -- df5.createOrReplaceTempView("df5")

# COMMAND ----------

# MAGIC %sql SELECT  CUSTOMERLOCATIONKEY, item, insert_timestamp, count(*) FROM df1 GROUP BY  CUSTOMERLOCATIONKEY, item, insert_timestamp HAVING COUNT(*)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from itemmasterahold where DEMANDTEC_PRISM_SID is not null

# COMMAND ----------

ITMDF=spark.sql("""
select 
  WKAD.EVENT_NAME,
  WKAD.AD_WEEK, 
  WKAD.DIVISION ,
  WKAD.MARKET,
  WKAD.ATTR_PAGE ,
  WKAD.ATTR_BLOCK ,
  WKAD.ATTR_AD_PLACEMENT ,
  WKAD.ATTR_AD_FORMAT ,
  WKAD.ATTR_HEADLINE ,
  WKAD.ATTR_SUBHEADLINE ,
  WKAD.ITEM_NUMBER ,
  WKAD.ATTR_PRICE_TYPE_DESC ,
  WKAD.AD_COMMENTS ,
  WKAD.SID_COPYCARD ,
  WKAD.ADDITIONAL_SID ,
  WKAD.SOURCE_PROMOTIONID ,
  WKAD.STORE_NUMBER ,
  WKAD.DEMANDTEC_PRISM_SID ,
  WKAD.INSERT_ID,
  WKAD.INSERT_TIMESTAMP
from df1 WKAD inner join df2 df on  WKAD.Customerlocationkey=df2.
default.itemmasterAhold ITM on WKAD.Customerlocationkey=ITM.SMA_DEST_STORE
and WKAD.item like ITM.SMA_Legacy_Item
""")

ITMDF.createOrReplaceTempView("ITMDF")


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ITMDF

# COMMAND ----------

#Upsert to Item Master 


df3= spark.sql("""
Merge into default.itemmasterAhold as target 
using ITMDF as source
on source.STORE_NUMBER= target.SMA_DEST_STORE
and source.ITEM_NUMBER= target.SMA_Legacy_Item
WHEN MATCHED 
THEN UPDATE SET
    target.EVENT_NAME=source.EVENT_NAME,
    target.AD_WEEK =source.AD_WEEK,
    target.BRAND_DIVISION =source.DIVISION,
    target.MARKET_SEGMENT =source.MARKET,
    target.ATTR_PAGE =source.ATTR_PAGE,
    target.ATTR_BLOCK =source.ATTR_BLOCK,
    target.ATTR_AD_PLACEMENT =source.ATTR_AD_PLACEMENT,
    target.ATTR_AD_FORMAT =source.ATTR_AD_FORMAT,
    target.ATTR_HEADLINE =source.ATTR_HEADLINE,
    target.ATTR_SUBHEADLINE =source.ATTR_SUBHEADLINE,
    target.ATTR_AUSA_ITEM_NO =source.ITEM_NUMBER,
    target.ATTR_PRICE_TYPE_DESC=source.ATTR_PRICE_TYPE_DESC,
    target.AD_COMMENTS =source.AD_COMMENTS,
    target.SID_COPYCARD =source.SID_COPYCARD,
    target.ADDITIONAL_SID =source.ADDITIONAL_SID,
    target.SOURCE_PROMOTIONID =source.SOURCE_PROMOTIONID,
    target.CUSTOMERLOCATIONKEY =source.STORE_NUMBER,
    target.DEMANDTEC_PRISM_SID=source.DEMANDTEC_PRISM_SID,
    target.LAST_UPDATE_TIMESTAMP=source.INSERT_TIMESTAMP,
    target.LAST_UPDATE_ID=source.INSERT_ID 
""")



# COMMAND ----------

df_test = spark.sql("""SELECT * from default.itemMasterAhold WHERE (SMA_LEGACY_ITEM, SMA_DEST_STORE) IN (SELECT lpad(Substring(ITEM_NUMBER,0,10),12,'0') as SMA_LEGACY_ITEM,lpad(STORE_NUMBER,4,'0') as SMA_DEST_STORE FROM default.weeklyAd_delta)""")
display(df_test)


# COMMAND ----------

print(df_test.count())

# COMMAND ----------

df_test.createOrReplaceTempView("df_test")

# COMMAND ----------

df_del = spark.sql('''select 
  EVENT_NAME,
  AD_WEEK, 
  DIVISION ,
  MARKET,
  ATTR_PAGE ,
  ATTR_BLOCK ,
  ATTR_AD_PLACEMENT ,
  ATTR_AD_FORMAT ,
  ATTR_HEADLINE ,
  ATTR_SUBHEADLINE ,
  lpad(Substring(ITEM_NUMBER,0,10),12,'0') AS ITEM_NUMBER,
  ATTR_PRICE_TYPE_DESC ,
  AD_COMMENTS ,
  SID_COPYCARD ,
  ADDITIONAL_SID ,
  SOURCE_PROMOTIONID ,
  lpad(STORE_NUMBER,4,'0') as STORE_NUMBER ,
  DEMANDTEC_PRISM_SID ,
  INSERT_ID,
  INSERT_TIMESTAMP from weeklyad_delta''')
df_del.createOrReplaceTempView("df_del")

# COMMAND ----------

# df_update = spark.sql('''
# WITH CTE as(
#              SELECT a.EVENT_NAME as EVENT_NAME,df_del.EVENT_NAME  as EVENT_NAME1,
#     a.AD_WEEK as AD_WEEK,df_del.AD_WEEK as AD_WEEK1,
#     a.BRAND_DIVISION as BRAND_DIVISION ,df_del.DIVISION as BRAND_DIVISION1,
#     a.MARKET_SEGMENT as MARKET_SEGMENT ,df_del.MARKET as MARKET_SEGMENT1,
#     a.ATTR_PAGE as ATTR_PAGE ,df_del.ATTR_PAGE as ATTR_PAGE1,
#     a.ATTR_BLOCK as ATTR_BLOCK ,df_del.ATTR_BLOCK as ATTR_BLOCK1,
#     a.ATTR_AD_PLACEMENT as ATTR_AD_PLACEMENT ,df_del.ATTR_AD_PLACEMENT  as ATTR_AD_PLACEMENT1,
#     a.ATTR_AD_FORMAT as ATTR_AD_FORMAT ,df_del.ATTR_AD_FORMAT  as ATTR_AD_FORMAT1,
#     a.ATTR_HEADLINE as ATTR_HEADLINE ,df_del.ATTR_HEADLINE as ATTR_HEADLINE1,
#     a.ATTR_SUBHEADLINE as ATTR_SUBHEADLINE ,df_del.ATTR_SUBHEADLINE as ATTR_SUBHEADLINE1,
#     a.ATTR_AUSA_ITEM_NO as ATTR_AUSA_ITEM_NO ,df_del.ITEM_NUMBER as ATTR_AUSA_ITEM_NO1,
#     a.ATTR_PRICE_TYPE_DESC as ATTR_PRICE_TYPE_DESC,df_del.ATTR_PRICE_TYPE_DESC as ATTR_PRICE_TYPE_DESC1,
#     a.AD_COMMENTS as AD_COMMENTS ,df_del.AD_COMMENTS as AD_COMMENTS1 ,
#     a.SID_COPYCARD as SID_COPYCARD ,df_del.SID_COPYCARD as SID_COPYCARD1,
#     a.ADDITIONAL_SID as ADDITIONAL_SID ,df_del.ADDITIONAL_SID as ADDITIONAL_SID1,
#     a.SOURCE_PROMOTIONID as SOURCE_PROMOTIONID ,df_del.SOURCE_PROMOTIONID as SOURCE_PROMOTIONID1,
#     a.CUSTOMERLOCATIONKEY as CUSTOMERLOCATIONKEY ,df_del.STORE_NUMBER as CUSTOMERLOCATIONKEY1,
#     a.DEMANDTEC_PRISM_SID as DEMANDTEC_PRISM_SID,df_del.DEMANDTEC_PRISM_SID as DEMANDTEC_PRISM_SID1,
#     a.LAST_UPDATE_TIMESTAMP as LAST_UPDATE_TIMESTAMP,df_del.INSERT_TIMESTAMP as LAST_UPDATE_TIMESTAMP1,
#     a.LAST_UPDATE_ID as LAST_UPDATE_ID,df_del.INSERT_ID  as LAST_UPDATE_ID1
  
#   FROM default.itemMasterahold a
#   INNER JOIN df_del ON df_del.STORE_NUMBER = a.SMA_DEST_STORE AND df_del.ITEM_NUMBER = a.SMA_LEGACY_ITEM
#            )

# UPDATE CTE SET
#     EVENT_NAME=EVENT_NAME1,
#     AD_WEEK =AD_WEEK1,
#     BRAND_DIVISION =BRAND_DIVISION1,
#     MARKET_SEGMENT =MARKET_SEGMENT1,
#     ATTR_PAGE =ATTR_PAGE1,
#     ATTR_BLOCK =ATTR_BLOCK1,
#     ATTR_AD_PLACEMENT =ATTR_AD_PLACEMENT1,
#     ATTR_AD_FORMAT =ATTR_AD_FORMAT1,
#     ATTR_HEADLINE =ATTR_HEADLINE1,
#     ATTR_SUBHEADLINE =ATTR_SUBHEADLINE1,
#     ATTR_AUSA_ITEM_NO =ATTR_AUSA_ITEM_NO1,
#     ATTR_PRICE_TYPE_DESC=ATTR_PRICE_TYPE_DESC1,
#     AD_COMMENTS =AD_COMMENTS1,
#     SID_COPYCARD =SID_COPYCARD1,
#     ADDITIONAL_SID =ADDITIONAL_SID1,
#     SOURCE_PROMOTIONID =SOURCE_PROMOTIONID1,
#     CUSTOMERLOCATIONKEY =CUSTOMERLOCATIONKEY1,
#     DEMANDTEC_PRISM_SID=DEMANDTEC_PRISM_SID1,
#     LAST_UPDATE_TIMESTAMP=LAST_UPDATE_TIMESTAMP1,
#     LAST_UPDATE_ID=LAST_UPDATE_ID1
# ''')

# COMMAND ----------

# MAGIC %sql select * from itemmasterAhold where DEMANDTEC_PRISM_SID is not Null

# COMMAND ----------

#Weekly AD Columns to Update 

df2= spark.sql("""select lpad(Substring(ITEM_NUMBER,0,10),12,'0') as item,lpad(STORE_NUMBER,4,'0') as CUSTOMERLOCATIONKEY,*  from weeklyAd_Delta""")
df2.createOrReplaceTempView("df2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df2 inner join ITMDF IT on 
# MAGIC df2.Customerlocationkey=IT.SMA_DEST_STORE
# MAGIC and df2.item like IT.SMA_Legacy_Item

# COMMAND ----------



# COMMAND ----------

### Step 3: Get the itemMasterAhold records to be updated from the list of source id's and location keys from weekly ad
##


# COMMAND ----------

#dfit= spark.sql("""select *,SMA_Legacy_Item as NewItem from default.itemmasterAhold""")
#dfit.createOrReplaceTempView("dfit")

# COMMAND ----------

# MAGIC %sql
# MAGIC ---select * from dfit where SMA_DEST_STORE like '6445' and Newitem like'000000503038'

# COMMAND ----------

### Step 4: 
## Perform inner join on the common data

# COMMAND ----------

# %sql SELECT * from df1 WHERE df1.SMA_DEST_STORE

# COMMAND ----------

# %sql
# select dt.*  from dfit dt inner join df1  on df1.CUSTOMERLOCATIONKEY like dt.SMA_DEST_STORE
# and dt.Newitem like df1.item
# and dt.SMA_DEST_STORE like '6445' and dt.Newitem like'000000503038'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select dt.*  from df1 inner join dfit dt  on df1.CUSTOMERLOCATIONKEY like dt.SMA_DEST_STORE
# MAGIC -- and dt.Newitem like df1.item
# MAGIC -- and dt.SMA_DEST_STORE like '6445' and dt.Newitem like'000000503038'

# COMMAND ----------

# MAGIC %sql
# MAGIC ---select * from ITMWK limit 10
# MAGIC -- select dt.*  from df1 inner join dfit dt  on df1.CUSTOMERLOCATIONKEY like dt.SMA_DEST_STORE
# MAGIC -- and dt.Newitem like df1.item
# MAGIC ---and dt.SMA_DEST_STORE like '6445' and dt.Newitem like'000000503038'

# COMMAND ----------

# weekly ad Columns to NUll
#%sql select dt.*  from df1 inner join dfit dt  on df1.CUSTOMERLOCATIONKEY like dt.SMA_STORE
#and dt.Newitem like df1.item
#Where dt.ID is not Null

ITMWK =spark.sql("""select dt.*  from df1 inner join dfit dt  on df1.CUSTOMERLOCATIONKEY like dt.SMA_DEST_STORE
and dt.Newitem like df1.item
and dt.SMA_DEST_STORE like '6445' --and dt.Newitem like'000000503038'
""")
ITMWK.createOrReplaceTempView("ITMWK")
display(ITMWK)

# COMMAND ----------

### Step 5:
## Perform upsert into itemMasterAhold table

spark.sql("""
Merge into default.itemmasterAhold as target 
using ITMDF as source
on source.Customerlocationkey= target.SMA_DEST_STORE
and source.item= target.SMA_Legacy_Item
WHEN MATCHED 
THEN UPDATE SET*
WHEN NOT MATCHED
THEN INSERT *""")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df1 
# MAGIC ---2503852

# COMMAND ----------


df2= spark.sql("""select *,substring(TPRX001_ITEM_NBR,5,10) as NewItem from default.itemmaster""")
df2.createOrReplaceTempView("df2")



# COMMAND ----------


dfdd1=spark.sql("""Select * from WeeklyAd_Delta where Store_number=6003""")
dfdd1.createOrReplaceTempView("dfdd1")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM(
# MAGIC Select df1.* from Default.ItemmasterAhold df1 Left join dfdd1  df2
# MAGIC on df1.SMA_Dest_Store== df2.Store_number and df2.Item_number == df1.SMA_Legacy_Item)
# MAGIC where demandtec_prism_sid is not null
# MAGIC -- Where df2.Store_number='6003'
