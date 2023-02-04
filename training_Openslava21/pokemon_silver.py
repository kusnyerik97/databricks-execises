# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *
import random

# COMMAND ----------

spark.sql("          FILL IN                   Silver")
spark.sql("          FILL IN                   Silver")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/Bronze"))

# COMMAND ----------

bronze_schema = "dbfs:/FileStore/tables/Bronze/"
silver_storage = 'dbfs:/FileStore/tables/Silver/'

# COMMAND ----------

pokemon_schema = StructType([StructField("pokedex_number",IntegerType(),True),
                   StructField("name",StringType(),True),
                   StructField("generation",IntegerType(),True),
                   StructField("status",StringType(),True),
                   StructField("species",StringType(),True),
                   StructField("type",StringType(),True),
                   StructField("height_m",DoubleType(),True),
                   StructField("weight_kg",DoubleType(),True),
                   StructField("image",StringType(),True)])

# COMMAND ----------

# MAGIC %md
# MAGIC  functions which we will use :
# MAGIC    * translate - used to switch character for character in sting column
# MAGIC    * initcap - capitalizes string 

# COMMAND ----------

# DBTITLE 1,Clean Gen 1_3
gen1_3_silver = (spark.sql("Select * from Bronze.gen1_3")
                # FILL IN 
                )
display(gen1_3_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC  functions which we will use :
# MAGIC    * regexp_replace - used to replace whole substring ( supports wildcard characters)
# MAGIC    * initcap - capitalizes string 

# COMMAND ----------

# DBTITLE 1,Clean Gen 4_6
gen4_6_silver = (spark.sql("Select * from Bronze.gen4_6")
                #FILL IN 
                )
display(gen4_6_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC  functions which we will use :
# MAGIC    * initcap - capitalizes string 
# MAGIC    * dropna - drops null values ( can be specified for which columns or for all columns)

# COMMAND ----------

# DBTITLE 1,Clean Gen 7_8
gen7_8_silver = (spark.sql("Select * from Bronze.gen7_8")
                 #FILL IN 
                )
display(gen7_8_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC  functions which we will use :
# MAGIC    * union - appends dataframes to each other ( requires same schema)
# MAGIC    * cast - used to change data type of column

# COMMAND ----------

# DBTITLE 1,Union generations
pokedex = (gen1_3_silver#FILL IN 
                        .withColumn("height_m",col("height_m").cast("double"))
                        .withColumn("weight_kg",col("weight_kg").cast("double"))
          )
display(pokedex)

# COMMAND ----------

# DBTITLE 1,Write final table to delta storage
pokedex.write.format("delta").mode("overwrite").save(f"{silver_storage}pokedex")

spark.sql(f"CREATE TABLE IF NOT EXISTS pokedex USING DELTA Location '{silver_storage}pokedex'")

# COMMAND ----------

# DBTITLE 1,Change of column data type using for loop
integer_columns = ["pokedex_number","total_points","hp","attack","defense","sp_attack","sp_defense","speed"]
double_columns = ["normal","fire","water","electric","grass","ice","fight","poison","ground","flying","psychic","bug","rock","ghost","dragon","dark","steel","fairy"]

power_stats = (spark.sql("Select * from Bronze.power_stats"))

#FILL IN 

display(power_stats)

# COMMAND ----------

display(power_stats#FILL IN )

# COMMAND ----------

display(power_stats.filter(col("hp")#FILL IN )

# COMMAND ----------

# DBTITLE 1,Solving missing HP values - Using Pyspark when/then/otherwise function
power_stats = (power_stats
               .withColumn("hp", when(col("hp").isNull(), 
                                      (col("total_points")-col("attack")-col("defense")-col("sp_attack")-col("sp_defense")-col("speed")))
                                 .otherwise(col("hp"))
                           )
              )

# COMMAND ----------

display(power_stats.filter(col("pokedex_number").isin([#FILL IN ])))

# COMMAND ----------

display(power_stats.filter(col("total_points")==0))

# COMMAND ----------

# DBTITLE 1,Solving 0 value for total_points - Using Pyspark when/then/otherwise function
power_stats = (power_stats.withColumn("total_points",when(col("total_points")==0,
                                                         (col("hp")+col("attack")+col("defense")+col("sp_attack")+col("sp_defense")+col("speed")))
                                                    .otherwise(col("total_points"))
                                      )
              )

# COMMAND ----------

display(power_stats.filter(col("pokedex_number").isin([#FILL IN ])))

# COMMAND ----------

power_stats.write.format("delta").mode("overwrite").save(f"{silver_storage}power_stats")

spark.sql(f"CREATE TABLE IF NOT EXISTS power_stats USING DELTA Location '{silver_storage}power_stats'")