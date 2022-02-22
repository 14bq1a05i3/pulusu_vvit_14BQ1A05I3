# Databricks notebook source
import logging
import time
import datetime

def logger(p_filename,p_dir='/tmp/'):
  file_date = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')
  p_logfile = p_dir + p_filename + file_date + '.log'
  logger = logging.getLogger('log4j')
  logger.setLevel(logging.INFO) 
  # create file handler which logs even debug messages
  fh = logging.FileHandler(p_logfile,mode='a')
  # create console handler with a higher log level
  ch = logging.StreamHandler()
  ch.setLevel(logging.DEBUG)
  # create formatter and add it to the handlers
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  fh.setFormatter(formatter)
  ch.setFormatter(formatter)
  if (logger.hasHandlers()):
       logger.handlers.clear()
  logger.addHandler(fh)
  logger.addHandler(ch)
  return logger, p_logfile, file_date


# COMMAND ----------

