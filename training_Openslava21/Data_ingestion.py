# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *
import random

# COMMAND ----------

# MAGIC %sh curl -O 'https://raw.githubusercontent.com/Vladis7771/Openslava2021/master/Data_Sources/Gen1-3.csv'

# COMMAND ----------

# MAGIC %sh curl -O 'https://raw.githubusercontent.com/Vladis7771/Openslava2021/master/Data_Sources/Gen4_6.xlsx'

# COMMAND ----------

# MAGIC %sh curl -O 'https://raw.githubusercontent.com/Vladis7771/Openslava2021/master/Data_Sources/Gen7-8.json'

# COMMAND ----------

# MAGIC %sh curl -O 'https://raw.githubusercontent.com/Vladis7771/Openslava2021/master/Data_Sources/power_stats.csv'

# COMMAND ----------

files = dbutils.fs.ls("file:/databricks/driver/")
sources= ["Gen1-3.csv","Gen4_6.xlsx","Gen7-8.json","power_stats.csv"]

for file in files:
  if file.name in sources:
    print(file.name)