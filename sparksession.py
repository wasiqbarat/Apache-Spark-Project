from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("SpotifySparkApp").master("local[*]").getOrCreate()

# Verify the session
print("SparkSession created with master set to localhost.")
