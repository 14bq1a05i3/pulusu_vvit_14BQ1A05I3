# Databricks notebook source
df_gen2_cdm_file = spark.read.format("parquet").load("/mnt/rs05ue2pipadl03/FIONA/CDM/RBS/Product/PRODUCT_PURCH/PRODUCT_PURCH/prod_purch.parquet")
df_gen2_cdm_file.createOrReplaceTempView("df_gen2_cdm_file")

print(df_gen2_cdm_file.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM df_gen2_cdm_file WHERE DSD_WHSE_IND!="0"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM df_gen2_cdm_file WHERE CHN_CD="050" -- AND BNNR_ID=1000

# COMMAND ----------

df_gen2_dev_FIONA_file = spark.read.format("CSV").option("sep", "|").option("header", "false").load("/mnt/rs06ue2dipadl03/FIONA/CDM/OUTBOUND_VENDOR_EXTRACTS_APP/purch_item_050_prd2ahold/scdw_extract_purch_item3_20210314.TXT")
df_gen2_dev_FIONA_file.createOrReplaceTempView("df_gen2_dev_FIONA_file")

print(df_gen2_dev_FIONA_file.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM df_gen2_dev_FIONA_file WHERE _c0="010" LIMIT 2

# COMMAND ----------

df_gen2_dev_edw_file = spark.read.format("CSV").option("sep", "|").option("header", "false").load("/mnt/rs06ue2dipadl03/FIONA/CDM/OUTBOUND_VENDOR_EXTRACTS_APP/purch_item_010_prd2ahold/EDW_scdw_extract_purch_item1_20210314.TXT")
df_gen2_dev_edw_file.createOrReplaceTempView("df_gen2_dev_edw_file")

print(df_gen2_dev_edw_file.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM df_gen2_dev_edw_file WHERE _c2="1"

# COMMAND ----------

gen2_cdm_banner_df=spark.read.format("parquet").load("/mnt/rs05ue2pipadl03/FIONA/CDM/RBS/BANNER")
gen2_cdm_banner_df.createOrReplaceTempView("cdm_banner") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct 
# MAGIC         b.chn_cd,
# MAGIC         lpad(cast(b.item_nbr as string),14,"0"),
# MAGIC         b.dsd_whse_ind,
# MAGIC         (CASE 
# MAGIC              WHEN COUNT(*)>6
# MAGIC                  THEN max(date_format(b.first_recd_date,"yyyyMMdd")) 
# MAGIC                  ELSE "00010101" 
# MAGIC              END) as time_beg_dt,
# MAGIC         max(date_format(b.first_recd_date,"yyyyMMdd")) as first_recd_date 
# MAGIC      FROM cdm_banner c 
# MAGIC      JOIN df_gen2_cdm_file b 
# MAGIC           ON b.bnnr_id=c.banner_id AND c.company_desc="AHOLD USA RETAIL COMPANY" 
# MAGIC      WHERE b.chn_cd = "010" AND b.item_nbr > 0 
# MAGIC      GROUP BY b.chn_cd,lpad(cast(b.item_nbr as string),14,"0"),b.dsd_whse_ind

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT banner_id, company_desc FROM cdm_banner WHERE company_desc LIKE "%AHOLD USA RETAIL COMPANY%"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT banner_id FROM cdm_banner WHERE company_desc LIKE "%AHOLD USA RETAIL COMPANY%"-- LIMIT 2

# COMMAND ----------

df_purch_item_010_prd2ahold_out=sqlContext.sql("""
SELECT distinct 
        b.chn_cd,
        lpad(cast(b.item_nbr as string),14,"0"),
        b.dsd_whse_ind,
        (CASE WHEN COUNT(*) > 6 THEN max(date_format(b.first_recd_date,"yyyyMMdd")) ELSE "00010101" END) as time_beg_dt,
        max(date_format(b.first_recd_date,"yyyyMMdd")) as first_recd_date 
     FROM cdm_banner c 
     JOIN df_gen2_cdm_file b 
          ON b.bnnr_id=c.banner_id AND c.company_desc="AHOLD USA RETAIL COMPANY" 
     WHERE b.chn_cd = "010" AND b.item_nbr > 0 
     GROUP BY b.chn_cd,lpad(cast(b.item_nbr as string),14,"0"),b.dsd_whse_ind
""")
df_purch_item_010_prd2ahold_out.createOrReplaceTempView("purch_item_010_prd2ahold_out_vw")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT bnnr_id, COUNT(*) FROM df_gen2_cdm_file GROUP BY bnnr_id ORDER BY bnnr_id 
# MAGIC --WHERE dsd_whse_ind="0"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM purch_item_010_prd2ahold_out_vw WHERE time_beg_dt!="00010101"
# MAGIC --WHERE dsd_whse_ind="0"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT banner_id, COUNT(*) FROM cdm_banner WHERE company_desc="AHOLD USA RETAIL COMPANY" GROUP BY banner_id ORDER BY banner_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM df_gen2_dev_edw_file --LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM purch_item_010_prd2ahold_out_vw LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM df_gen2_dev_FIONA_file --LIMIT 10

# COMMAND ----------



# COMMAND ----------

df_gen2_cdm_file = spark.read.format("parquet").load("/mnt/rs05ue2pipadl03/FIONA/CDM/RBS/Product/PRODUCT_PURCH/PRODUCT_PURCH/prod_purch.parquet")
df_gen2_cdm_file.createOrReplaceTempView("df_gen2_cdm_file")

print(df_gen2_cdm_file.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM df_gen2_cdm_file WHERE CHN_CD="050"

# COMMAND ----------

df_gen2_dev_edw_file = spark.read.format("CSV").option("sep", "|").option("header", "false").load("/mnt/rs06ue2dipadl03/FIONA/CDM/OUTBOUND_VENDOR_EXTRACTS_APP/purch_item_050_prd2ahold/scdw_extract_purch_item3_20210314.TXT")
df_gen2_dev_edw_file.createOrReplaceTempView("df_gen2_dev_edw_file")

print(df_gen2_dev_edw_file.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM df_gen2_dev_edw_file WHERE _c0="050"

# COMMAND ----------


