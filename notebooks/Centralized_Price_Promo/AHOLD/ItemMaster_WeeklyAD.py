# Databricks notebook source
# MAGIC %md
# MAGIC ## Very Important
# MAGIC 
# MAGIC #### Call this notebook from the itemmaster notebook before performing the itemMasterWrite() function to write data from delta to CDM.

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col, lit, row_number, desc
from pyspark.sql.types import StringType,BooleanType,DateType
from datetime import date
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# Weekly AD Delta Table from other Notebook to get the Distinct Store & Item
current_date_value = dbutils.widgets.get("current_date_value")

df1= spark.sql("""select  lpad(Substring(ITEM_NUMBER,0,10),12,'0') as item,lpad(STORE_NUMBER,4,'0') as CUSTOMERLOCATIONKEY,*  from weeklyAd_Delta """)
df1 = df1.withColumn("current_date_value", lit(current_date_value))
df1.createOrReplaceTempView("df1")

df1 = spark.sql("""select * from df1 WHERE current_date_value >= AD_PROMO_START_DATE AND current_date_value <= AD_PROMO_END_DATE""")

windowSpec = Window.partitionBy(["customerlocationkey", "item"]).orderBy(desc("AD_PROMO_START_DATE"))
dfx = df1.withColumn("row_number", row_number().over(windowSpec))
dfx = dfx.filter(dfx.row_number == 1)
dfx = dfx.drop('row_number')
dfx.createOrReplaceTempView("dfx")

df2= spark.sql("""select distinct * from dfx """)
df2.createOrReplaceTempView("df2")

df3= spark.sql(""" select ITM.* from df2 inner join default.itemmasterAhold ITM on df2.Customerlocationkey=ITM.SMA_DEST_STORE
 and df2.item=ITM.SMA_LEGACY_ITEM """)
df3.createOrReplaceTempView("df3")

df4= spark.sql("""select df3.* from  df1 inner join  df3 on df1.CUSTOMERLOCATIONKEY=df3.SMA_DEST_STORE and df1.item=df3.SMA_LEGACY_ITEM""")
df4.createOrReplaceTempView("df4")

# COMMAND ----------

##display(df1)

# COMMAND ----------

# MAGIC %sql 
# MAGIC UPDATE itemmasterahold set 
# MAGIC   EVENT_NAME=Null,
# MAGIC   AD_WEEK=Null,
# MAGIC   BRAND_DIVISION=Null,
# MAGIC   MARKET_SEGMENT=Null,
# MAGIC   ATTR_PAGE =Null,
# MAGIC   ATTR_BLOCK =Null,
# MAGIC   ATTR_AD_PLACEMENT=Null,
# MAGIC   ATTR_AD_FORMAT=Null,
# MAGIC   ATTR_HEADLINE=Null,
# MAGIC   ATTR_SUBHEADLINE =Null,
# MAGIC   ATTR_AUSA_ITEM_NO =Null,
# MAGIC   ATTR_PRICE_TYPE_DESC=Null,
# MAGIC   AD_COMMENTS =Null,
# MAGIC   SID_COPYCARD =Null,
# MAGIC   ADDITIONAL_SID =Null,
# MAGIC   SOURCE_PROMOTIONID =Null,
# MAGIC   CustomerlocationKey =Null,
# MAGIC   DEMANDTEC_PRISM_SID =Null
# MAGIC WHERE DEMANDTEC_PRISM_SID is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Merge into default.itemmasterAhold df4  
# MAGIC using df2 df1 
# MAGIC on df1.customerlocationkey= df4.SMA_DEST_STORE
# MAGIC and df1.item= df4.SMA_Legacy_Item
# MAGIC and df4.PRD2STORE_BANNER_ID Like "%AHOLD%" 
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
# MAGIC --   df4.LAST_UPDATE_ID =df1.INSERT_ID,
# MAGIC --   df4.LAST_UPDATE_TIMESTAMP =df1.INSERT_TIMESTAMP

# COMMAND ----------

# MAGIC %sql 
# MAGIC --SELECT * FROM weeklyad_delta
# MAGIC --0000000036
# MAGIC --0000001315

# COMMAND ----------

# MAGIC %sql 
# MAGIC --SELECT distinct event_Name FROM weeklyad_delta ---where Demandtec_Prism_SID='5222130'

# COMMAND ----------

# MAGIC %sql
# MAGIC --desc itemmasterAhold

# COMMAND ----------

# MAGIC %sql 
# MAGIC ---SELECT Distinct PRD2STORE_BANNER_ID FROM itemmasterAhold --WHERE SMA_DEST_STORE = '2610' AND SMA_LEGACY_ITEM = '000000084016'

# COMMAND ----------

# %sql SELECT SMA_DEST_STORE, SMA_LEGACY_ITEM, ATTR_AUSA_ITEM_NO, DEMANDTEC_PRISM_SID, EVENT_NAME, COUNT(*) FROM itemmasterAhold GROUP BY SMA_DEST_STORE, SMA_LEGACY_ITEM, ATTR_AUSA_ITEM_NO, DEMANDTEC_PRISM_SID, EVENT_NAME HAVING COUNT(*) > 1 AND EVENT_NAME is not NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select distinct * from default.itemmasterAhold where SMA_DEST_STORE='0851' and SMA_LEGACY_ITEM='000000040102' and Event_name='01-21-2022 SS'

# COMMAND ----------

# %sql SELECT Event_name ,SMA_DEST_STORE, SMA_LEGACY_ITEM,count(*)as cnt FROM default.itemmasterAhold where DEMANDTEC_PRISM_SID is not Null
# group by Event_name,SMA_DEST_STORE, SMA_LEGACY_ITEM
# having cnt >1

# COMMAND ----------

# %sql SELECT count(*) FROM default.itemmasterAhold where DEMANDTEC_PRISM_SID is not Null

# COMMAND ----------

# %sql SELECT * FROM default.itemmasterAhold where DEMANDTEC_PRISM_SID is not Null