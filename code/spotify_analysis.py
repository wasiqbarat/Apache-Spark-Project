from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, min, regexp_extract, col, when
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import pyspark.sql.functions as F

def create_spark_session():
    return SparkSession.builder \
        .appName("Spotify Charts Analysis") \
        .getOrCreate()

def extract_track_id(url):
    try:
        track_id = url.split('track/')[-1]
        return track_id
    except:
        return None

def analyze_spotify_charts(spark, input_path, year, month):
    df = spark.read.parquet(input_path)
    
    filtered_df = df.filter(
        (col("chart") == "top200") & 
        (col("year") == year) & 
        (col("month") == month)
    )
    
    avg_streams = filtered_df.groupBy("artist", "region", "date") \
        .agg(avg("streams").alias("avg_streams"))
    
    window_spec = Window.partitionBy("region", "date")
    
    max_streams = avg_streams.withColumn(
        "max_streams",
        F.max("avg_streams").over(window_spec)
    ).filter(col("avg_streams") == col("max_streams"))
    
    artist_ranks = filtered_df.join(
        max_streams,
        ["artist", "region", "date"]
    )
    
    best_ranks = artist_ranks.groupBy("artist", "region", "date") \
        .agg(F.min("rank").alias("best_rank"))
    
    extract_track_id_udf = F.udf(extract_track_id, StringType())
    
    final_results = artist_ranks.join(
        best_ranks,
        ["artist", "region", "date"]
    ).filter(col("rank") == col("best_rank")) \
    .withColumn("track_id", extract_track_id_udf(col("url"))) \
    .select(
        "artist",
        "title",
        "best_rank",
        "date",
        "region",
        "track_id",
        "avg_streams"
    )
    
    return final_results

def main():
    spark = create_spark_session()
    
    input_path = "./charts.parquet"
    year = 2023
    month = 1
    
    results = analyze_spotify_charts(spark, input_path, year, month)
    
    results.show(truncate=False)
    
    spark.stop()

if __name__ == "__main__":
    main() 