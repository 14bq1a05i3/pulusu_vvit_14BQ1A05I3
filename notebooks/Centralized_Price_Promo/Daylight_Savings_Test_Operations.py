# Databricks notebook source
# from dateutil import tz
# from datetime import datetime
# NYC = tz.gettz('America/New_York')
# current_time = datetime.now()
# print(tz.resolve_imaginary())

# COMMAND ----------

from dateutil import tz
from datetime import datetime, timedelta, date, time

NYC = tz.gettz('America/New_York')

current_est_time1 = datetime.now(NYC)
# print(current_est_time1)

#### Step 1:-    ------>>>>>>Possible solution but not a fit in this Scenario
#### Write this output into a text file
#### Read the timestamp values from the text file using Look Up
#### Clean up the text file after every run
# print("POS Run Date:  ")
# POS_timestamp_value = current_est_time1 - timedelta(days=1)
# print(POS_timestamp_value)


#### Step 2:- Exit notebook with the timestamp value ---->>>> Preferred and Best solution in this Scenario
# dbutils.notebook.exit(notebook_outputs)

# dbutils.notebook.exit(datetime.now())


print("Run Date Value:  ")
#notebook_outputs = [current_est_time1, POS_timestamp_value]
RunDate_Value = current_est_time1.strftime('%Y-%m-%d %H:%M:%S')
print(RunDate_Value)

print("POS Run Date:  ")
POS_timestamp_value = current_est_time1 - timedelta(days=1)
POSDate_Value = POS_timestamp_value.strftime('%Y-%m-%d %H:%M:%S')
print(POSDate_Value)


# COMMAND ----------

print("Scenario 1: Only Date")
RunDate_Date = current_est_time1.strftime('%Y-%m-%d')
POSDate_Date = POS_timestamp_value.strftime('%Y-%m-%d')
notebook_outputs1 = {'RunDate': RunDate_Date, 'POSDate': POSDate_Date}
print(notebook_outputs1)
print("\n")

dbutils.notebook.exit(notebook_outputs1)

notebook_outputs2 = {'RunDate': RunDate_Value, 'POSDate': POSDate_Value}
print("Scenario 2: Date & Time")
print(notebook_outputs2)

# dbutils.notebook.exit(notebook_outputs2)

print("\n")
# Current Suggestion
print("Scenario 3: Timestamp")
notebook_outputs3 = {'RunDate': current_est_time1, 'POSDate': POS_timestamp_value}
print(notebook_outputs3)

print("\n")

# COMMAND ----------

# MAGIC %md Test Cases

# COMMAND ----------

# On the Day before Daylight Savings End --- First Saturday of November, 2021
print(datetime(2021, 11, 6, tzinfo=tz.gettz('America/New_York')))

# COMMAND ----------

# On the Day of Daylight Savings End --- First Sunday of November, 2021
print("Pipeline Run Date:  ")
print(datetime(2021, 11, 7, tzinfo=tz.gettz('America/New_York')))
current_timestamp_value = datetime(2021, 11, 7, tzinfo=tz.gettz('America/New_York'))
# POS Run date (Run Date - 1 Day)
print("POS Run Date:  ")
POS_timestamp_value = current_timestamp_value - timedelta(days=1)
print(POS_timestamp_value)

# COMMAND ----------

# On the Day After Daylight Savings End --- Monday after First Sunday of November, 2021
print(datetime(2021, 11, 8, tzinfo=tz.gettz('America/New_York')))

# COMMAND ----------

# On the Day Before Daylight Savings Start --- Day Before Second Sunday of March, 2022
print(datetime(2022, 3, 12, tzinfo=tz.gettz('America/New_York')))

# COMMAND ----------

# On the Day of Daylight Savings Start --- Second Sunday of March, 2022
print(datetime(2022, 3, 13, tzinfo=tz.gettz('America/New_York')))

# COMMAND ----------

# On the Day after Daylight Savings Start --- Day after Second Sunday of March, 2022
print(datetime(2022, 3, 14, tzinfo=tz.gettz('America/New_York')))

# COMMAND ----------

# On the Day of Daylight Savings Start --- Second Sunday of March, 2022
print(datetime(2022, 3, 13, 1, 59, 59, tzinfo=tz.gettz('America/New_York')))

# COMMAND ----------

# On the Day of Daylight Savings Start --- Second Sunday of March, 2022
print(datetime(2022, 3, 13, 2, 0, 0, tzinfo=tz.gettz('America/New_York')))

# COMMAND ----------

# On the Day of Daylight Savings End --- First Sunday of November, 2021
print(datetime(2021, 11, 7, 1, 59, 59, tzinfo=tz.gettz('America/New_York')))

# COMMAND ----------

# On the Day of Daylight Savings End --- First Sunday of November, 2021
print(datetime(2021, 11, 7, 2, 0, 0, tzinfo=tz.gettz('America/New_York')))

# COMMAND ----------

# On the Begining of Month
Beginning_of_Month = datetime(2021, 12, 1, tzinfo=tz.gettz('America/New_York'))
POS_date_for_this = Beginning_of_Month - timedelta(days=1)

print(Beginning_of_Month)
print(POS_date_for_this)

# COMMAND ----------

# On the Begining of Year
Beginning_of_Year = datetime(2022, 1, 1, tzinfo=tz.gettz('America/New_York'))
POS_date_for_Year = Beginning_of_Year - timedelta(days=1)

print(Beginning_of_Year)
print(POS_date_for_Year)

# COMMAND ----------

