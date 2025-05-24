from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofweek, avg

# Start Spark session
spark = SparkSession.builder \
    .appName("Traffic Pattern Analysis") \
    .getOrCreate()

# Load data
df = spark.read.csv("hdfs:///traffic_data/Metro_Interstate_Traffic_Volume.csv", header=True, inferSchema=True)

# Extract hour and day
df = df.withColumn("hour", hour("date_time"))
df = df.withColumn("day_of_week", dayofweek("date_time"))

# 1. Hourly traffic volume
df.groupBy("hour").avg("traffic_volume").orderBy("hour").show()

# 2. Day-of-week pattern
df.groupBy("day_of_week").avg("traffic_volume").orderBy("day_of_week").show()

# 3. Weather impact
df.groupBy("weather_main").avg("traffic_volume").orderBy("avg(traffic_volume)", ascending=False).show()

spark.stop()
