# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *
import random

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS Silver")
spark.sql("USE Silver")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/Bronze"))

# COMMAND ----------

bronze_schema = "dbfs:/FileStore/tables/Bronze/"
silver_storage = 'dbfs:/FileStore/tables/Silver/'

# COMMAND ----------

pokemon_schema = StructType([StructField("pokedex_number",StringType(),True),
                   StructField("name",StringType(),True),
                   StructField("generation",IntegerType(),True),
                   StructField("status",StringType(),True),
                   StructField("species",StringType(),True),
                   StructField("type",StringType(),True),
                   StructField("height_m",DoubleType(),True),
                   StructField("weight_kg",DoubleType(),True),
                   StructField("image",StringType(),True)])

# COMMAND ----------

gen1_3_silver = (spark.sql("Select * from Bronze.gen1_3")
                .withColumn("name",translate(initcap(col("name")),"/","u"))
                )
display(gen1_3_silver)

# COMMAND ----------

gen4_6_silver = (spark.sql("Select * from Bronze.gen4_6")
                .withColumn("name",initcap(col("name")))
                 .withColumn("species",regexp_replace(col("species")," PokÃ©mon",""))
                )
display(gen4_6_silver)

# COMMAND ----------

gen7_8_silver = (spark.sql("Select * from Bronze.gen7_8")
                 .withColumn("name",initcap(col("name")))
                 .dropna("all")
                )
display(gen7_8_silver)

# COMMAND ----------

pokedex = (gen1_3_silver.union(gen4_6_silver)
                        .union(gen7_8_silver)
                        .withColumn("pokedex_number",col("pokedex_number").cast("int"))
                        .withColumn("generation",col("generation").cast("int"))
                        .withColumn("height_m",col("height_m").cast("double"))
                        .withColumn("weight_kg",col("weight_kg").cast("double"))
          )
display(pokedex)

# COMMAND ----------

pokedex.write.format("delta").mode("overwrite").save(f"{silver_storage}pokedex")

spark.sql(f"CREATE TABLE IF NOT EXISTS pokedex USING DELTA Location '{silver_storage}pokedex'")

# COMMAND ----------

integer_columns = ["pokedex_number","total_points","hp","attack","defense","sp_attack","sp_defense","speed"]
double_columns = ["normal","fire","water","electric","grass","ice","fight","poison","ground","flying","psychic","bug","rock","ghost","dragon","dark","steel","fairy"]

power_stats = (spark.sql("Select * from Bronze.power_stats"))

for column in power_stats.columns:
  if column in integer_columns:
    power_stats = power_stats.withColumn(column, col(column).cast("int"))
  elif column in double_columns:
    power_stats = power_stats.withColumn(column, col(column).cast("double"))
  else:
    print(f"{column} is not in lists")

display(power_stats)

# COMMAND ----------

display(power_stats.describe())

# COMMAND ----------

power_stats = power_stats.withColumn("hp", when(col("hp").isNull(), (col("total_points")-col("attack")-col("defense")-col("sp_attack")-col("sp_defense")-col("speed"))).otherwise(col("hp"))
                                               )

# COMMAND ----------

display(power_stats.filter(col("pokedex_number").isin([240,78,1])))

# COMMAND ----------

power_stats = power_stats.withColumn("total_points",when(col("total_points")==0, (col("hp")+col("attack")+col("defense")+col("sp_attack")+col("sp_defense")+col("speed"))).otherwise(col("total_points"))
                                               )

# COMMAND ----------

display(power_stats.filter(col("pokedex_number").isin([24,45,780])))

# COMMAND ----------

power_stats.write.format("delta").mode("overwrite").save(f"{silver_storage}power_stats")

spark.sql(f"CREATE TABLE IF NOT EXISTS power_stats USING DELTA Location '{silver_storage}power_stats'")