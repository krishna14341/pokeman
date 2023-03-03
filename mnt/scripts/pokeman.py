from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf


conf = SparkConf().setAppName("pokeman").setMaster("local")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

spark = SparkSession.builder \
    .appName("omg") \
    .getOrCreate()

# #: ID for each pokemon
# Name: Name of each pokemon
# Type 1: Each pokemon has a type, this determines weakness/resistance to attacks
# Type 2: Some pokemon are dual type and have 2
# Total: sum of all stats that come after this, a general guide to how strong a pokemon is
# HP: hit points, or health, defines how much damage a pokemon can withstand before fainting
# Attack: the base modifier for normal attacks (eg. Scratch, Punch)
# Defense: the base damage resistance against normal attacks
# Sp. Atk: special attack, the base modifier for special attacks (e.g. fire blast, bubble beam)
# Sp. Def: the base damage resistance against special attacks
# Speed: determines which pokemon attacks first each round

spark = SparkSession.builder.appName("Pokemon").getOrCreate()
pokemon_data = spark.read.csv("/mnt/data/Pokemon.csv", header=True, inferSchema=True)

pokemon_data.cache()
pokemon_data.printSchema()
pokemon_data.show()

# 1) what are the top 5 strongest non-legendary monsters?
#   --- step-1 filter non-legendary ---> Legendary == False
#   --- step-2 In order to get strong pokeman we need to order dataframe based on Total
#   --- step-3 to get top 5 we need to limit output to 5 rows

print("1) what are the top 5 strongest non-legendary monsters?")
strongest_pokemon = pokemon_data.filter("Legendary == False").orderBy(F.desc('Total'),'Name').limit(5)
strongest_pokemon.show(truncate=False)

# 2) Which Pokemon type has the highest average HP?
print("2) Which Pokemon type has the highest average HP?")
avg_hp_df = pokemon_data.groupBy("Type 1").agg(F.avg("HP").alias("avg_hp")).orderBy(F.desc('avg_hp'))
highest_avg_hp_type = avg_hp_df.first()
print("  The type with the highest average HP is:\t", highest_avg_hp_type["Type 1"],  "with an average HP of", highest_avg_hp_type["avg_hp"])


# 3) Which is the most common special Attack?
print("\n3) Which is the most common special Attack?")
special_attack_df = pokemon_data.groupBy("Type 1").agg(F.count("Type 1").alias("atk_cnt")).orderBy(F.desc("atk_cnt"))
print("  The most common special attack is:\t\t", special_attack_df.first()["Type 1"])


special_attack_df.repartition(1).write.mode("overwrite").parquet("/mnt/output/output.parquet")
spark.stop()
