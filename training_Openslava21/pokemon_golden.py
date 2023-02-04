# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *
import random
import datetime

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS Golden")
spark.sql("USE Golden")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/Silver"))

# COMMAND ----------

silver_storage = 'dbfs:/FileStore/tables/Silver/'
golden_storage = 'dbfs:/FileStore/tables/Golden/'

# COMMAND ----------

# DBTITLE 1,Join and create PokemonDB , write it to golden storage
# read both Dataframes
pokedex = spark.sql("select * from silver.pokedex")
power_stats = spark.sql("select * from silver.power_stats")

# Join Dataframes
pokemondb = pokedex #FILL IN use pokemon_number for join

# Write result to Golden
pokemondb.write.format("delta").mode("overwrite").save(f"{golden_storage}pokemondb")

spark.sql(f"CREATE TABLE IF NOT EXISTS pokemondb USING DELTA Location '{golden_storage}pokemondb'")

# COMMAND ----------

def match_generator(MyPokemons,numOfMatchesPerPokemon):
  
  """ 
  - creates random matches between specified MyPokemons from the list of their pokedex numbers
  - asignes random date from 01/01/2018 to 01/01/2020
  - returns DataFrame
  
  """   
  assert type(MyPokemons) == list , f"MyPokemons should be list of pokedex numbers, not {type(MyPokemons)} ... example - [6,9,65,95,104] "
  assert type(numOfMatchesPerPokemon) == int , f"numOfMatchesPerPokemon should be of type integer, not {type(numOfMatchesPerPokemon)}.... example - 200000"
  
  # creating random list of 100000 pokemons as enemy pokemons" 
  enemy_pokemons = [random.randrange(1, 891, 1) for i in range(numOfMatchesPerPokemon)]
  df_match = []

  # matching each pokemon to fight
  for myp in MyPokemons:
    for enemp in enemy_pokemons:
      match = (myp,enemp)
      df_match.append(match)
  
  # creation of dataframe of matches (still without dates)
  match_schema = StructType([       
      StructField('My_pokemon', IntegerType(), True),
      StructField('Enemy_pokemon', IntegerType(), True)])
  match = spark.createDataFrame(data = df_match, schema = match_schema).withColumn("date_of_match",to_date(lit("2018-01-01"))).collect()
  
  # creation of random dates between 01/01/2018 and 01/01/2020
  df_M = []
  for row in match:
    row = (row.My_pokemon,row.Enemy_pokemon,row.date_of_match+ datetime.timedelta(days=random.randrange(1, 730, 1)))
    df_M.append(row)
  
  # creation of dataframe of matches (with dates)
  match_schema2 = StructType([       
    StructField('My_pokemon', IntegerType(), True),
    StructField('Enemy_pokemon', IntegerType(), True),
    StructField('date', DateType(), True)]
    ) 
  match2 = spark.createDataFrame(data = df_M, schema = match_schema2)
  
  return match2

# COMMAND ----------

def results_generator(pokemon_db,match_table):
  
  #preselecting my_pokemons table
  my_table = pokemon_db.select("pokedex_number",lower("type").alias("my_type"),col("attack").alias("my_attack"),col("defense").alias("my_defence"),"normal","fire","water","electric","grass","ice","fight","poison","ground","flying","psychic","bug","rock","ghost","dragon","dark","steel","fairy")
  
  # preselecting match_table
  match_table = (match_table.join(my_table, my_table["pokedex_number"] == match_table["My_pokemon"],"left"))

  #unpivoting enemy types in match table
  unpivotExpr = 'stack(18, "normal",normal,"fire",fire,"water",water,"electric",electric,"grass",grass,"ice",ice,"fight",fight,"poison",poison,"ground",ground,"flying",flying,"psychic",psychic,"bug",bug,"rock",rock,"ghost",ghost,"dragon",dragon,"dark",dark,"steel",steel,"fairy",fairy) as (Enemy_quantifier,total)'
  match_table = match_table.select("My_pokemon","Enemy_pokemon","date","pokedex_number","my_type","my_attack","my_defence", expr(unpivotExpr)) 
  
  
  enemy = pokemon_db.select(col("pokedex_number").alias("enemy_pok_num"),col("name").alias("enemy_name"),lower("type").alias("enemy_type"),col("attack").alias("enemy_attack"),col("defense").alias("enemy_defence"),"image")
  match_table = (match_table.join(enemy, match_table.Enemy_pokemon == enemy.enemy_pok_num ,"left")
               .filter(col("Enemy_quantifier")==col("enemy_type"))
               .withColumn("result", when((col("my_attack")/col("total"))-(col("enemy_defence")*col("total"))>((col("enemy_attack")*col("total"))-(col("my_defence")/col("total"))),lit(1))
                           .otherwise(lit(0))))
  result_table = match_table.select("My_pokemon","Enemy_pokemon","date","result","enemy_name","enemy_type","image")
  return result_table

# COMMAND ----------

# giving the list of my pokemons by pokedex number
my_pokemons = [6,9,65,95,104] #FILL IN with your favourite :) 

# using UDF to create 1,000,000 matches (5x 200,000)
match = match_generator(MyPokemons=my_pokemons,numOfMatchesPerPokemon=200000)

# COMMAND ----------

# creating results
match_results = results_generator(pokemondb,match)

# Write result to Golden
match_results.write.format("delta").mode("overwrite").save(f"{golden_storage}match_results")

spark.sql(f"CREATE TABLE IF NOT EXISTS match_results USING DELTA Location '{golden_storage}match_results'")