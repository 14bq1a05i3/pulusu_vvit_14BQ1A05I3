# Databricks notebook source
import json
from pytz import timezone
import datetime
import jsonpickle
from json import JSONEncoder
import pandas as pd
import os
import ntpath
from datetime import datetime
from datetime import date
import pytz
import numpy as np
from pyspark.sql.types import * 
from pyspark.sql import *
from pyspark.sql.functions  import *
from delta.tables import *
from pyspark.sql.functions import regexp_extract
import zipfile

# COMMAND ----------

#dbutils.widgets.removeAll()
dbutils.widgets.text("filePath","")
dbutils.widgets.text("directory","")
dbutils.widgets.text("MountPoint","")
dbutils.widgets.text("Container","")
dbutils.widgets.text("clientId", "")
dbutils.widgets.text("keyVaultName", "")
dbutils.widgets.text("FileDate", "")

Filepath =dbutils.widgets.get("filePath")
Directory=dbutils.widgets.get("directory")
MountPoint = dbutils.widgets.get("MountPoint")
Container = dbutils.widgets.get("Container")
clientId = dbutils.widgets.get("clientId")
keyVaultName = dbutils.widgets.get("keyVaultName")
FileDate = dbutils.widgets.get("FileDate")

# COMMAND ----------

files = '/mnt' + '/' + Directory + '/' + Filepath
source= 'abfss://' + Directory + '@' + Container + '.dfs.core.windows.net/'
today = date.today() # dd/mm/YYYY
curentDay = today.strftime("%Y/%m/%d") # YYYY/mm/dd
print("curentDay =", curentDay)

# COMMAND ----------

# MAGIC %run /Centralized_Price_Promo/Mount_Point_Creation

# COMMAND ----------

mounting(MountPoint, source, clientId, keyVaultName)

# COMMAND ----------


try:  
  path_fdln = filesTemp = '/dbfs'+files+'Foodlion/Inbound/RDS/'+FileDate # files = '/mnt/delhaize-centralized-price-promo/POSemergency/'
  print(path_fdln)
  fdln_fdpaths = [path_fdln+"/"+fd for fd in os.listdir(path_fdln)]
  print(fdln_fdpaths)
except:
  pass

try:
  path_han = filesTemp = '/dbfs'+files+'Hannaford/Inbound/RDS/'+FileDate
  print(path_han)
  han_fdpaths = [path_han+"/"+fd for fd in os.listdir(path_han)]
  print(han_fdpaths)
except:
  pass


# COMMAND ----------

try:
  fdln_fdpaths
except Exception as e:
  fdln_fdpaths = None

if fdln_fdpaths is None:
  pass
else: 
  FDLNfile_list = []
  counter = 0
  for fdpath  in fdln_fdpaths:
      statinfo = os.stat(fdpath)
      create_date = datetime.fromtimestamp(statinfo.st_ctime)
      modified_date = datetime.fromtimestamp(statinfo.st_mtime)
      head, tail = os.path.split(fdpath)
      fileName = tail
      filePath = os.path.split(fdpath)
      effFilePath = filePath[0].split('/')[4]+'/'+filePath[0].split('/')[5]+'/'+filePath[0].split('/')[6]+'/'+filePath[0].split('/')[7]
      directory = filePath[0].split('/')[3]
      filePath = 'mnt/'+directory+'/'+effFilePath+'/'+filePath[0].split('/')[8]+'/'+filePath[0].split('/')[9]+'/'+filePath[0].split('/')[10]
      file  = {'Index': counter,'filePath': filePath,'fullFilePath': fdpath, 'feed': 'FDLN', 'fileName': fileName, 'effPath':effFilePath , 'direcory': directory, 'createDate': create_date, 'modifiedDate':modified_date}
      FDLNfile_list.append(file)
      counter = counter+1
  print(f'Foodlion file list : {FDLNfile_list}')

# COMMAND ----------

try:
  han_fdpaths
except Exception as e:
  han_fdpaths = None

if han_fdpaths is None:
  pass
else: 
  counter = 0
  HANfile_list = []
  for fdpath in han_fdpaths:
      statinfo = os.stat(fdpath)
      create_date = datetime.fromtimestamp(statinfo.st_ctime)
      modified_date = datetime.fromtimestamp(statinfo.st_mtime)
      head, tail = os.path.split(fdpath)
      fileName = tail
      filePath = os.path.split(fdpath)
      effFilePath = filePath[0].split('/')[4]+'/'+filePath[0].split('/')[5]+'/'+filePath[0].split('/')[6]+'/'+filePath[0].split('/')[7]
      directory = filePath[0].split('/')[3]
      filePath = 'mnt/'+directory+'/'+effFilePath+'/'+filePath[0].split('/')[8]+'/'+filePath[0].split('/')[9]+'/'+filePath[0].split('/')[10]
      file  = {'Index': counter, 'filePath': filePath, 'fullFilePath': fdpath, 'feed':'HAN', 'fileName': fileName, 'effPath':effFilePath , 'direcory': directory, 'createDate': create_date, 'modifiedDate':modified_date}
      HANfile_list.append(file)
      counter = counter+1
  print(f'HANNAFORD file list : {HANfile_list}')

# COMMAND ----------

if (han_fdpaths is None) and (fdln_fdpaths is None):
  dbutils.notebook.exit('Files Not present')
elif fdln_fdpaths is None:
  file_list = HANfile_list
  dfFiles = pd.DataFrame.from_records(file_list)
  print(f'Only Hannaford files :{dfFiles}')
elif han_fdpaths is None:
  file_list = FDLNfile_list
  dfFiles = pd.DataFrame.from_records(file_list)
  print(f'Only Foodlion files :{dfFiles}')
else:
  file_list = HANfile_list + FDLNfile_list
  dfFiles = pd.DataFrame.from_records(file_list)
  print(f'Combined Hannaford & Foodlion files :{dfFiles}')
  

# COMMAND ----------

fileGen = []
for index, row in dfFiles.iterrows():
  #filePathNew = '/'+row['filePath']+'/'+row['fileName']
  fullFilePath = str(row['fullFilePath'])
  df = 'rawdf'+str(row['Index'])+'_'+row['feed']
  feed = row['feed']
  Index = row['Index']
  createDate = row['createDate']
  fileName = row['fileName']
  fileGen.append([Index, feed, df, fullFilePath, createDate,fileName])

# COMMAND ----------

fileGen

# COMMAND ----------

#pd.read_csv('/dbfs/mnt/delhaize-centralized-price-promo/POSemergency/Hannaford/Inbound/RDS/2021/02/22/SCZP.EMER.SCZE123E.DATAOUT.BSAM.20210222.103229.txt', names=['_col0_'],delimiter='|',header=None, index_col=0)
#pd.read_csv("/dbfs/FileStore/tables/2esy8tnj1455052720017/part_001-86465.tsv")

# COMMAND ----------

counter1 = 0
result1 = pd.DataFrame(columns = ['Index','createDate','feed','_col0_'])
result2 = pd.DataFrame(columns = ['Index','createDate','feed','_col0_'])
for i in fileGen:
  try:
    Index = i[counter1]
    #print(Index)
    feed = i[1]
    #print(feed)
    fullFilePath = i[3]
    #print(fullFilePath)
    createDate= i[4]
    #print(createDate)
    fileName = i[5]
    
    if fullFilePath.endswith(".gz"):
      print(f"File Path : {fullFilePath}")
      print(f"File Name : {fileName}")
      raw_df = pd.read_csv(fullFilePath, compression='gzip', header=None, sep='|', names=['_col0_'],quotechar='"', error_bad_lines=False)
      raw_df['feed'] = feed
      raw_df['Index'] = Index
      raw_df['createDate'] = createDate
      print(raw_df.head(3))
      #print(filePath)
      result1 = result1.append(raw_df)
      #print(result1.count())
      print(f'Completed Processing : {i[1]}')
      
    else:
      raw_df = pd.DataFrame(columns = ['_col0_'])
      raw_df = pd.read_csv(fullFilePath, delimiter='|',names=['_col0_'],header=None)
      #pandas_df = raw_df.toPandas()
      raw_df['feed'] = feed
      raw_df['Index'] = Index
      raw_df['createDate'] = createDate
      print(raw_df.head(3))
      #print(filePath)
      result2 = result2.append(raw_df)
      #print(result2.count())
      print(f'Completed Processing : {i[1]}')
  except:
    pass
  
print(result1.count())
print(result2.count())
resultFinal = result1.append(result2)
print(resultFinal.count())
#print(result.head(3))

# COMMAND ----------

resultFinal['feed'].unique()

# COMMAND ----------

try:
  newHAN = resultFinal[resultFinal['feed'] == 'HAN'] 
  print(f'Count of Hannaford Records : {newHAN.count()}')
  newFDLN = resultFinal[resultFinal['feed'] == 'FDLN']
  print(f'Count of Foodlion Records : {newFDLN.count()}')
except:
    pass

# COMMAND ----------

#newHAN.append(newFDLN)

# COMMAND ----------

newdf = newHAN.append(newFDLN)
newdf['rowNumber'] = np.arange(1,len(newdf)+1,1)
newdfWrite = newdf
newdfWrite = newdfWrite.drop(['Index','createDate','feed'], axis=1)
sparkDF=spark.createDataFrame(newdfWrite)
sparkDF.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(files+'/MergedTill9AM/'+FileDate)

# COMMAND ----------

display(newdfWrite)

# COMMAND ----------

#sparkDF.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save('/mnt/delhaize-centralized-price-promo/POSemergency/Merge9AM/'+curentDay)
if fdln_fdpaths is None:
  pass
else:
  dbutils.fs.mv(files+'Foodlion/Inbound/RDS/'+FileDate, files+'/MergedTill9AM/Foodlion/'+FileDate, recurse=True)
  
if han_fdpaths is None:
  pass
else:
  dbutils.fs.mv(files+'Hannaford/Inbound/RDS/'+FileDate, files+'/MergedTill9AM/Hannaford/'+FileDate, recurse=True)
#dbutils.fs.mv("/mnt/delhaize-centralized-price-promo/POSemergency/Hannaford/Inbound/RDS/2021/02/22", '/mnt/delhaize-centralized-price-promo/POSemergency/Merge9AM/Archival/Hannaford/'+curentDay, recurse=True)


# COMMAND ----------

dbutils.notebook.exit('Success')