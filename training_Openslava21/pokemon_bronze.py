# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *
import random

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS Bronze")
spark.sql("USE Bronze")

# COMMAND ----------

# MAGIC %run "../training_Openslava21/Data_ingestion"

# COMMAND ----------

raw_data_path = "file:/databricks/driver/"
bronze_storage = 'dbfs:/FileStore/tables/Bronze/'

# COMMAND ----------

pokemon_schema = StructType([StructField("pokedex_number",StringType(),True),
                   StructField("name",StringType(),True),
                   StructField("generation",StringType(),True),
                   StructField("status",StringType(),True),
                   StructField("species",StringType(),True),
                   StructField("type",StringType(),True),
                   StructField("height_m",StringType(),True),
                   StructField("weight_kg",StringType(),True),
                   StructField("image",StringType(),True)])

# COMMAND ----------

# MAGIC %md
# MAGIC * Reading CSV format
# MAGIC   * paramters:
# MAGIC         - format(source)
# MAGIC                 Specifies the input data source format.
# MAGIC         - schema(schema)
# MAGIC                 Specifies the input schema.
# MAGIC         - sep (option)
# MAGIC                 sets the single character as a separator for each field and value. If None is set, it uses the default value, ,.
# MAGIC         - header (option)
# MAGIC                 uses the first line as names of columns. If None is set, it uses the default value, false.
# MAGIC         - load
# MAGIC                 used to create Dataframe from spark reader by specifying source path

# COMMAND ----------

# DBTITLE 1,Read generation 1_3
gen1_3 = (spark.read
          # FILL IN
          .load(f"{raw_data_path}Gen1-3.csv")) # dbfs:/FileStore/tables/Gen1_3.csv

# COMMAND ----------

# MAGIC %md
# MAGIC * Writing to Delta format
# MAGIC   * paramters:
# MAGIC         - format(output)
# MAGIC                 Specifies the output format.
# MAGIC         - mode
# MAGIC                 Specifies the way, how data is saved to final destination.

# COMMAND ----------

# DBTITLE 0,Write generation 1_3
gen1_3.write.FILL IN.save(f"{bronze_storage}gen1_3")

spark.sql(f"CREATE TABLE IF NOT EXISTS gen1_3 USING DELTA Location '{bronze_storage}gen1_3'")

# COMMAND ----------

# MAGIC %md
# MAGIC **instalation of library to cluster** "com.crealytics:spark-excel_2.12:0.14.0"
# MAGIC * Reading XLSX format
# MAGIC   * paramters:
# MAGIC         - format(source)
# MAGIC                 Specifies the input data source format. ("com.crealytics.spark.excel")
# MAGIC         - schema(schema)
# MAGIC                 Specifies the input schema.
# MAGIC         - treatEmptyValuesAsNulls (option)
# MAGIC                 sets reader to treat empty values as null
# MAGIC         - header (option)
# MAGIC                 uses the first line as names of columns. If None is set, it uses the default value, false.
# MAGIC         - dataAddress (option)
# MAGIC                 uses the specify sheet and cell where the table starts ("'gen4_6'!A1")    
# MAGIC         - load
# MAGIC                 used to create Dataframe from spark reader by specifying source path

# COMMAND ----------

# DBTITLE 1,Read generation 4_6
gen4_6 = (spark.read
          #FILL IN
          .load(f"{raw_data_path}Gen4_6.xlsx")
         )

# COMMAND ----------

gen4_6.write.format("delta").mode("overwrite").save(f"{bronze_storage}gen4_6")

spark.sql(f"CREATE TABLE IF NOT EXISTS gen4_6 USING DELTA Location '{bronze_storage}gen4_6'")

# COMMAND ----------

# MAGIC %md
# MAGIC * Reading JSON format
# MAGIC   * paramters:
# MAGIC         - schema(schema)
# MAGIC                 Specifies the input schema.
# MAGIC         - json(source)
# MAGIC                 Specifies the input data source format/path.

# COMMAND ----------

# DBTITLE 1,Read generation 7_8
gen7_8 = (spark.read
          #FILL IN
          (f"{raw_data_path}Gen7-8.json")
         )

# COMMAND ----------

gen7_8.write.format("delta").mode("overwrite").save(f"{bronze_storage}gen7_8")

spark.sql(f"CREATE TABLE IF NOT EXISTS gen7_8 USING DELTA Location '{bronze_storage}gen7_8'")

# COMMAND ----------

stats_schema = StructType([StructField("pokedex_number",StringType(),True),
                             StructField("total_points",StringType(),True),
                             StructField("hp",StringType(),True),
                             StructField("attack",StringType(),True),
                             StructField("defense",StringType(),True),
                             StructField("sp_attack",StringType(),True),
                             StructField("sp_defense",StringType(),True),
                             StructField("speed",StringType(),True),
                             StructField("normal",StringType(),True),
                             StructField("fire",StringType(),True),
                             StructField("water",StringType(),True),
                             StructField("electric",StringType(),True),
                             StructField("grass",StringType(),True),
                             StructField("ice",StringType(),True),
                             StructField("fight",StringType(),True),
                             StructField("poison",StringType(),True),
                             StructField("ground",StringType(),True),
                             StructField("flying",StringType(),True),
                             StructField("psychic",StringType(),True),
                             StructField("bug",StringType(),True),
                             StructField("rock",StringType(),True),
                             StructField("ghost",StringType(),True),
                             StructField("dragon",StringType(),True),
                             StructField("dark",StringType(),True),
                             StructField("steel",StringType(),True),
                             StructField("fairy",StringType(),True)]
                           )

# COMMAND ----------

# DBTITLE 1,Read power_stats
power_stats = (spark.read
               #FILL IN
          .load(f"{raw_data_path}power_stats.csv"))

# COMMAND ----------

power_stats.write.format("delta").mode("overwrite").save(f"{bronze_storage}power_stats")

spark.sql(f"CREATE TABLE IF NOT EXISTS power_stats USING DELTA Location '{bronze_storage}power_stats'")