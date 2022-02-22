# Databricks notebook source
import pandas as pd
from pyspark.sql.types import * 
from pyspark.sql import *
from pyspark.sql.functions  import *
from pytz import timezone
import datetime
from delta.tables import *
import pytz
import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

aholdDeltaTables = ['itemmasterahold', 'itemmainahold', 'promotionmain', 'promotionlink', 'store_delta', 'vendor_delta', 'couponaholdtable']
columnsSelect = ['timestamp','version','operation','job','notebook', 'operationMetrics']
aholdinsertDF = pd.DataFrame(columns=['pipelineId', 'Feed', 'pipelineName','DeltTableName','TimeZone','timestamp','version','filePath','fileName','operation','job','notebook', 'operationMetrics'])
delhaizeinsertDF = pd.DataFrame(columns=['pipelineId', 'Feed', 'pipelineName','DeltTableName','TimeZone','timestamp','version','filePath','fileName','operation','job','notebook', 'operationMetrics'])
delhaizeDeltaTables = ['productdeltatable', 'producttemp','producttostoredelta', 'prodtostoretemp',  'dapricedelta', 'pricechange', 'dapricetemp', 'alternateupc_delta', 'inventory_delta', 'inventorytemp', 'store_delta_delhaize', 'storetemp', 'bottledeposit','promolink', 'coupondelhaizetable', 'itemmaster','item_main','itemmainefftemp']

# COMMAND ----------

aholdinsertDF = aholdinsertDF.astype({'version':'int64'})
delhaizeinsertDF = delhaizeinsertDF.astype({'version':'int64'})
eastern = pytz.timezone('US/Eastern')

# COMMAND ----------

DAmapTableDeltas = {'product': ["producttemp","productdeltatable","itemmainefftemp"], 'price':["dapricedelta","dapricetemp","itemmainefftemp"], 'producttostore':["producttostoredelta", "prodtostoretemp","itemmainefftemp"], 'store':["store_delta_delhaize","storetemp","itemmainefftemp"], 'pos':["bottledeposit","itemmainefftemp", "item_main", "promolink", "CouponDelhaizeTable"], 'itemMaster':["itemmaster","itemmainefftemp", "item_main", "promolink", "CouponDelhaizeTable","dapricetemp","producttemp","prodtostoretemp","storetemp","inventorytemp"], 'altUPC':["alternateupc_delta","itemmainefftemp"], 'inventory': ["inventorytemp", "inventory_delta","itemmainefftemp"]}
AUSAmapTableDeltas = {'store':["store_delta"],'vendor':["vendor_delta", "store_delta"],'coupon':["couponaholdtable"],'item':["itemmainahold","promotionmain","promotionlink","PriceChange"],'itemMaster':["promotionmain","promotionlink","itemmainahold", "itemmasterahold","PriceChange","tax_holiday"]}

# COMMAND ----------

def updateDeltaVersioning(feed, pipelineName, pipelineId, filepath, fileName):
  global aholdinsertDF
  global delhaizeinsertDF
  if feed == 'Ahold':
    print('AHOLD')
    for i in AUSAmapTableDeltas.get(pipelineName):
      df = spark.sql("""DESCRIBE HISTORY {};""".format(str(i)))
      df.drop(col("operationParameters"))
      pdf = df.toPandas()
      pdf = pdf[columnsSelect].loc[0]
      pdf.timestamp = pdf.timestamp.tz_localize(pytz.utc).tz_convert(eastern)
      pdf['Feed'] =str(feed)
      pdf['TimeZone'] = 'EST'
      pdf['DeltTableName'] = str(i)
      pdf['pipelineName'] = str(pipelineName)
      pdf['pipelineId'] = str(pipelineId)
      pdf['filePath'] = str(filepath)
      pdf['fileName'] = str(fileName)
      aholdinsertDF = aholdinsertDF.append(pdf, ignore_index=True, sort=False)
      
    insertSDF = spark.createDataFrame(aholdinsertDF)
    insertSDF.write.format("delta").mode("append").saveAsTable("aholdCppVersion")  
  elif feed == 'Delhaize':
    print('DELHAIZE')
    for i in DAmapTableDeltas.get(pipelineName):
      df = spark.sql("""DESCRIBE HISTORY {};""".format(str(i)))
      df.drop(col("operationParameters"))
      pdf = df.toPandas()
      pdf = pdf[columnsSelect].loc[0]
      pdf.timestamp = pdf.timestamp.tz_localize(pytz.utc).tz_convert(eastern)
      pdf['Feed'] =str(feed)
      pdf['TimeZone'] = 'EST'
      pdf['DeltTableName'] = str(i)
      pdf['pipelineName'] = str(pipelineName)
      pdf['pipelineId'] = str(pipelineId)
      pdf['filePath'] = str(filepath)
      pdf['fileName'] = str(fileName)
      delhaizeinsertDF = delhaizeinsertDF.append(pdf, ignore_index=True, sort=False)
  
    insertSDF = spark.createDataFrame(delhaizeinsertDF)
    insertSDF.write.format("delta").mode("append").saveAsTable("delhaizeCppVersion")

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

#updateDeltaVersioning('Ahold', 'coupon', 'bbcTest123', 'abc/abc/', 'abc/2021/08/19/')

# COMMAND ----------

#%sql
#DELETE FROM delhaizecppversion

# COMMAND ----------

