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

# DBTITLE 1,Read generation 1_3
gen1_3 = (spark.read
          .format("csv")
          .schema(pokemon_schema)
          .option("sep", ",")
          .option("header","true")
          .load(f"{raw_data_path}Gen1-3.csv")) # dbfs:/FileStore/tables/Gen1_3.csv

# COMMAND ----------

display(gen1_3)

# COMMAND ----------

# DBTITLE 0,Write generation 1_3
gen1_3.write.format("delta").mode("overwrite").save(f"{bronze_storage}gen1_3")

spark.sql(f"CREATE TABLE IF NOT EXISTS gen1_3 USING DELTA Location '{bronze_storage}gen1_3'")

# COMMAND ----------

# MAGIC %md
# MAGIC **instalation of library to cluster** "com.crealytics:spark-excel_2.12:0.14.0"

# COMMAND ----------

# DBTITLE 1,Read generation 4_6
gen4_6 = (spark.read.format("com.crealytics.spark.excel")
          .option("header", "true")
          .option("treatEmptyValuesAsNulls", "true")
          .schema(pokemon_schema)
          #.option("addColorColumns", "False")
          .option("dataAddress", "'gen4_6'!A1")
          .load(f"{raw_data_path}Gen4_6.xlsx")
         
         )

# COMMAND ----------

gen4_6.write.format("delta").mode("overwrite").save(f"{bronze_storage}gen4_6")

spark.sql(f"CREATE TABLE IF NOT EXISTS gen4_6 USING DELTA Location '{bronze_storage}gen4_6'")

# COMMAND ----------

# DBTITLE 1,Read generation 7_8
gen7_8 = (spark.read.schema(pokemon_schema).json(f"{raw_data_path}Gen7-8.json")
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
power_stats = (spark.read.format("csv")
          .schema(stats_schema)
          .option("sep", ",")
          .option("header","true")
          .load(f"{raw_data_path}power_stats.csv"))

# COMMAND ----------

power_stats.write.format("delta").mode("overwrite").save(f"{bronze_storage}power_stats")

spark.sql(f"CREATE TABLE IF NOT EXISTS power_stats USING DELTA Location '{bronze_storage}power_stats'")