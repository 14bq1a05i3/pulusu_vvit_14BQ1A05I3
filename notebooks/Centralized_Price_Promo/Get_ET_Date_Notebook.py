# Databricks notebook source
from dateutil import tz
from datetime import datetime, timedelta

NYC = tz.gettz('America/New_York')
current_est_time1 = datetime.now(NYC)

#testing
current_est_time1 = current_est_time1 - timedelta(days=5)

previous_est_date = current_est_time1 - timedelta(days=1)

current_date = current_est_time1.strftime('%Y-%m-%d %H:%M:%S')
previous_date = previous_est_date.strftime('%Y-%m-%d %H:%M:%S')

run_outputs = {'current_date': current_date, 'previous_date': previous_date}

dbutils.notebook.exit(run_outputs)