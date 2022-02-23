# Databricks notebook source
#%md
#Notebook : EXTRACT_product_catmktg</BR>
#Version : 1.0</BR>
#Date : 15-Jul-2021</BR>
#Author :Automated_notebook_Generation </BR>
#Script details : Extract generated for product_catmktg with necessary transformations.</BR>
#</BR>
##Base Parameters:</BR>
#FileOps = "/FIONA/AHOLD/SHARED_NOTEBOOKS/FileOps"

# COMMAND ----------

# COMMAND ----------
#Importing the necessary libraries
import os
import json
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession, SQLContext
from datetime import datetime
import pyspark.sql.functions as f
from datetime import datetime,timedelta

# COMMAND ----------

# COMMAND ----------
#Setting the spark context and spark session
sparkSession = SparkSession.builder.appName("EXTRACT_product_catmktg").config("spark.executor.heartbeatInterval","60s").getOrCreate()
sc = sparkSession.sparkContext
sqlContext = SQLContext(sc) 

# COMMAND ----------

# COMMAND ----------
#Parameters used for the extract -"product_catmktg"
FileOps = "/FIONA/AHOLD/SHARED_NOTEBOOKS/FileOps"
dbutils.widgets.text("baseParameters", "")
dbutils.widgets.get("baseParameters")
file_ops_input=json.dumps({'Activity':'mount_point' })
out_FileOps=dbutils.notebook.run(FileOps, 120, {"file_ops_input": file_ops_input})
mount_point=(json.loads(out_FileOps))
MOUNT_PATH_PYTHON=mount_point.get('MOUNT_PATH_PYTHON')
MOUNT_PATH_SPARKS=mount_point.get('MOUNT_PATH_SPARKS')
PROD_PATH_SPARKS="/mnt/rs05ue2pipadl03"
OutputPath = MOUNT_PATH_SPARKS+"/FIONA/CDM/OUTBOUND_VENDOR_EXTRACTS_APP/PRODUCT_HIER_CATMKTG/producthier.ah.20210306.txt"


# COMMAND ----------

# COMMAND ----------
#Read the necessary source files for the extract -"product_catmktg"
gen2_cdm_banner_df=spark.read.format("parquet").load(PROD_PATH_SPARKS+"/FIONA/CDM/RBS/BANNER")
gen2_cdm_banner_df.createOrReplaceTempView("cdm_banner") 
gen2_cdm_product_df=spark.read.format("parquet").load(PROD_PATH_SPARKS+"/FIONA/CDM/RBS/Product/Product/{*}/*.parquet")
gen2_cdm_product_df.createOrReplaceTempView("cdm_product") 
gen2_cdm_product_brand_mfr_df=spark.read.format("parquet").load(PROD_PATH_SPARKS+"/FIONA/CDM/RBS/Product/ProductBrandMfr")
gen2_cdm_product_brand_mfr_df.createOrReplaceTempView("cdm_product_brand_mfr") 


# COMMAND ----------

# COMMAND ----------
####Extract File Creation For the extract "product_catmktg"
df_product_catmktg_out=sqlContext.sql(""" SELECT distinct "UPC" AS ia_dim_lvl_desc,Concat(trim(a.UPC_DSC)," (",lpad(a.UPC_NBR,14,0),")") AS ia_prod_desc  ,case when a.DISCNTNU_IND=0 Then "N" WHEN a.DISCNTNU_IND=1 Then "Y" ELSE a.DISCNTNU_IND END AS discontinued_ind  ,lpad(a.UPC_NBR,14,0) AS upc_cd  ,trim(a.UPC_DSC) AS upc_desc  ,lpad(a.prdt_lvl_1_id,10,0) AS acn_cat_mod_brnd_cd1,concat("(SS)-",a.PRDT_LVL_1_DSC,"(",lpad(a.prdt_lvl_1_id,10,0),")") AS custom_cm_lvl1_desc  ,lpad(a.PRDT_LVL_2_id,10,0) acn_cat_mod_brnd_cd2,concat("(SS)-",a.PRDT_LVL_2_DSC,"(",lpad(a.prdt_lvl_2_id,10,0),"))") AS custom_cm_lvl2_desc  ,lpad(a.PRDT_LVL_3_id,10,0) acn_cat_mod_brnd_cd3,concat("(SS)-",a.PRDT_LVL_3_DSC,"(",lpad(a.prdt_lvl_3_id,10,0),")))") AS custom_cm_lvl3_desc  ,lpad(a.PRDT_LVL_4_id,10,0) acn_cat_mod_brnd_cd4,concat("(SS)-",a.PRDT_LVL_4_DSC,"(",lpad(a.prdt_lvl_4_id,10,0),"))))") AS custom_cm_lvl4_desc  ,lpad(a.PRDT_LVL_5_id,10,0) acn_cat_mod_brnd_cd5,concat("(SS)-",a.PRDT_LVL_5_DSC,"(",lpad(a.prdt_lvl_5_id,10,0),")))))") AS custom_cm_lvl5_desc  ,lpad(a.PRDT_LVL_6_id,10,0) acn_cat_mod_brnd_cd6,concat("(SS)-",a.PRDT_LVL_6_DSC,"(",lpad(a.prdt_lvl_6_id,10,0),"))))))") AS custom_cm_lvl6_desc," " as acn_cat_mod_brnd_cd7,concat("(SS)-",a.PRDT_LVL_7_DSC,"(",lpad(a.prdt_lvl_7_id,10,0),")))))))") AS custom_cm_lvl7_desc," " as acn_cat_mod_brnd_cd8,concat("(SS)-",a.PRDT_LVL_8_DSC,"(",rpad(lpad(a.PRDT_LVL_8_id,10,0),15,0),"))))))))") AS custom_cm_lvl8_desc," " as  acn_cat_mod_brnd_cd9,concat("(SS)-",a.PRDT_LVL_9_DSC,"(",rpad(lpad(a.PRDT_LVL_9_id,10,0),15,0),")))))))))") AS custom_cm_lvl9_desc ,Translate(lpad(a.prdt_lvl_10_id,15,0),",","") item_cd,concat("(SS)-",a.ITEM_DSC,"(",Translate(lpad(a.prdt_lvl_10_id,15,0),",",""),"):") item_desc  ,date_format(a.ADD_DT,"YYYYMMdd") add_dt,date_format(a.DEL_DT,"YYYYMMdd") delete_dt  ,case when b.prod_brand_lo_dsc is null then "UNDEFINED BRAND-LO" ELSE b.prod_brand_lo_dsc END acn_brnd_mod_lvl_desc ,LPAD(NVL(a.MFR_ID,0),6,0) mfr_num,case when a.MFR_DSC is null then "UNDEFINED MFR" else a.MFR_DSC end mfr_desc  ,case when a.UOM_CD="" then null else a.UOM_CD end retail_uom_desc,a.RTL_PACK_QTY retail_pack_size  ,case when a.PVT_LBL_DSC IS NULL OR a.PVT_LBL_DSC="UNDEFINED" then "N" ELSE "Y" END pvt_lbl_ind  ,"" fat_free_ind,0 consum_unit_conv_fctr,0 equ_unit_conv_fctr,lpad(format_number(a.RTL_UNIT_SZ,3),8,0) retail_unit_size  ,case when a.BRND_DSC is null  then "UNDEFINED BRAND" else a.BRND_DSC end  brnd_desc  ,concat("(SS)-",a.PRDT_LVL_4_DSC,"(",lpad(a.prdt_lvl_4_id,10,0),"))))") dept_lvl_desc  ,concat("(SS)-",a.PRDT_LVL_5_DSC,"(",lpad(a.prdt_lvl_5_id,10,0),")))))") port_lvl_desc  ,concat("(SS)-",a.PRDT_LVL_6_DSC,"(",lpad(a.prdt_lvl_6_id,10,0),"))))))") cat_lvl_desc  ,concat("(SS)-",a.PRDT_LVL_7_DSC,"(",lpad(a.prdt_lvl_7_id,10,0),")))))))") subcat_lvl_desc  ,concat("(SS)-",a.PRDT_LVL_8_DSC,"(",rpad(lpad(a.PRDT_LVL_8_id,10,0),15,0),"))))))))") seg_lvl_desc,a.PVT_LBL_ID as CUSTM_ATTR08_NUM  from cdm_product a  join cdm_banner c on a.banner_id =c.banner_id  left join cdm_product_brand_mfr b   on a.upc_nbr =b.upc_nbr   and a.banner_id =b.bnnr_id  where c.company_desc = "AHOLD USA RETAIL COMPANY" AND a.UPC_NBR <> RPAD (" ", 14,0)   AND a.UPC_NBR IS NOT NULL  """)
df_product_catmktg_out.createOrReplaceTempView("product_catmktg_out_vw")


# COMMAND ----------

## create out file
fname=OutputPath.split('/')[-1]
Out=OutputPath.replace(fname, "")
df_product_catmktg_out.repartition(1).write.option("sep","|").option("header","false").option("compression","None").option("ignoreLeadingWhiteSpace","false").option("ignoreTrailingWhiteSpace", "false").option("emptyValue",None).mode("append").csv(Out)


# COMMAND ----------

# COMMAND ----------
## out folder cleanup
file_ops_input=json.dumps({'Activity':'Clean_up', 'Output_File': OutputPath, 'Output_Folder': Out})
out_FileOps=dbutils.notebook.run("/FIONA/AHOLD/SHARED_NOTEBOOKS/FileOps", 120, {"file_ops_input": file_ops_input})
print(json.loads(out_FileOps))

# COMMAND ----------

#### Output results from above steps - END

 # COMMAND ----------
####Validation Script For the extract "product_catmktg"
##Reading the Extract files for "product_catmktg"
df_edw_data_product_catmktg=spark.read.format("CSV").option("sep","|").option("header","false").load(Out+"EDW_"+fname)
df_fiona_data_product_catmktg=spark.read.format("CSV").option("sep","|").option("header","false").load(Out+fname)
df_edw_data_product_catmktg.createOrReplaceTempView("edw_data_product_catmktg_vw")
df_fiona_data_product_catmktg.createOrReplaceTempView("fiona_data_product_catmktg_vw")


# COMMAND ----------

##Checking counts between EDW and Fiona for extract "product_catmktg"
df_edw_data_product_catmktg_cnt=df_edw_data_product_catmktg.count()
df_fiona_data_product_catmktg_cnt=df_fiona_data_product_catmktg.count()
df_data_diff_product_catmktg_out=sqlContext.sql("""select * from edw_data_product_catmktg_vw minus select * from fiona_data_product_catmktg_vw""")
df_data_diff_product_catmktg_out.createOrReplaceTempView("data_diff_product_catmktg_vw")
df_data_pivot_product_catmktg_out=sqlContext.sql("""select "EDW" as Source,* from edw_data_product_catmktg_vw union select "FIONA" as Source,* from fiona_data_product_catmktg_vw""")
df_product_catmktg_cnt_diff=sqlContext.sql(""" select "product_catmktg" as Extract_Name,{0} as EDW_COUNT,{1} as FIONA_COUNT,concat(cast(round((({0}-{1})/{0})*100,2) as string) ,"%") as Count_variance,count(*) as Data_difference_count,concat(cast(round(((count(*))/{0})*100,2) as string),"%") as Data_diff_variance from data_diff_product_catmktg_vw """.format(df_edw_data_product_catmktg_cnt,df_fiona_data_product_catmktg_cnt) )


# COMMAND ----------

display(df_product_catmktg_cnt_diff)

# COMMAND ----------

# COMMAND ----------
## Generate Compare Report for extract -"product_catmktg" 
df_product_catmktg_cnt_diff.repartition(1).write.option("sep","|").option("header","true").mode("append").csv(Out)
file_ops_input=json.dumps({'Activity':'Clean_up', 'Output_File': Out+'product_catmktg_Compare_report.csv', 'Output_Folder': Out})
out_FileOps=dbutils.notebook.run("/FIONA/AHOLD/SHARED_NOTEBOOKS/FileOps", 120, {"file_ops_input": file_ops_input})
print(json.loads(out_FileOps))

# COMMAND ----------

# COMMAND ----------
## Generate data for pivot report for extract -"product_catmktg"
df_data_pivot_product_catmktg_out.repartition(1).write.option("sep","|").option("header","true").mode("append").csv(Out)
file_ops_input=json.dumps({'Activity':'Clean_up', 'Output_File': Out+'product_catmktg_PIVOT_report.csv', 'Output_Folder': Out})
out_FileOps=dbutils.notebook.run("/FIONA/AHOLD/SHARED_NOTEBOOKS/FileOps", 120, {"file_ops_input": file_ops_input})
print(json.loads(out_FileOps))

# COMMAND ----------

# COMMAND ----------
#Clear the widgets if any 
dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.functions import col, array, when, array_remove,lit,upper ,size ,round ,concat,length,ltrim,coalesce ,expr
# get conditions for all columns except id
conditions_ = [when(coalesce(ltrim(df_edw_data_product_catmktg[c]),lit("a"))!=coalesce(ltrim(df_fiona_data_product_catmktg[c]),lit("a")), lit(c)).otherwise("") for c in df_edw_data_product_catmktg.columns if c not in ('_c3')]
select_expr =[
col('_c3'),
*[array(ltrim(df_edw_data_product_catmktg[c]).cast("string"),ltrim(df_fiona_data_product_catmktg[c]).cast("string"),concat(coalesce(round(((ltrim(df_edw_data_product_catmktg[c])- ltrim(df_fiona_data_product_catmktg[c]))/(when (ltrim(df_edw_data_product_catmktg[c]).cast('integer')!="0",ltrim(df_edw_data_product_catmktg[c])).otherwise(lit("1")))
)*100,2),lit("100")),lit("%"))).cast("string").alias(c) for c in df_fiona_data_product_catmktg.columns if c not in ('_c3')],
array_remove(array(*conditions_), "").cast("string").alias("column_names")
]
df_result =df_edw_data_product_catmktg.join(df_fiona_data_product_catmktg, ["_C3"],"outer").select(*select_expr).filter(length(col("column_names"))>2)
display(df_result.filter(length(df_result["column_names"]) >2 ) )


# COMMAND ----------

df_result.repartition(1).write.option("sep","|").option("header","true").mode("append").csv(Out)
file_ops_input=json.dumps({'Activity':'Clean_up', 'Output_File': Out+'product_catmktg_data_difference_report.csv', 'Output_Folder': Out})
out_FileOps=dbutils.notebook.run("/FIONA/AHOLD/SHARED_NOTEBOOKS/FileOps", 120, {"file_ops_input": file_ops_input})
print(json.loads(out_FileOps))
