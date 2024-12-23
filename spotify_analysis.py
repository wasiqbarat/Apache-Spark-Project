from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, avg, min, regexp_extract
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import os
import shutil

def create_spark_session():
    return SparkSession.builder \
        .appName("Spotify Charts Analysis") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.local.dir", os.path.join(os.getcwd(), "spark_temp")) \
        .getOrCreate()

def analyze_spotify_charts(spark, input_path, target_year, target_month):
    # Read the Parquet file
    df = spark.read.parquet(input_path)
    
    # Clean and transform the data
    df_cleaned = df.withColumn("date", to_date(col("date"))) \
                  .withColumn("streams", col("streams").cast(IntegerType())) \
                  .withColumn("rank", col("rank").cast(IntegerType())) \
                  .filter(col("date").isNotNull())
    
    # Extract year and month
    df_with_date = df_cleaned.withColumn("year", year(col("date"))) \
                            .withColumn("month", month(col("date")))
    
    # Filter for top 200 chart and target year/month
    filtered_df = df_with_date.filter(
        (col("chart") == "top200") & 
        (col("year") == target_year) & 
        (col("month") == target_month)
    )
    
    print("\nStep 1: Filtered Data Sample")
    filtered_df.select("title", "artist", "date", "rank", "region", "streams").show(5, truncate=False)
    
    # Calculate average streams by artist, region, and date
    avg_streams = filtered_df.groupBy("artist", "region", "date") \
        .agg(avg("streams").alias("avg_streams"))
    
    print("\nStep 2: Average Streams Sample")
    avg_streams.show(5, truncate=False)
    
    # Find artists with highest average streams per region and date
    window_spec = Window.partitionBy("region", "date")
    max_streams = avg_streams.withColumn(
        "max_streams",
        F.max("avg_streams").over(window_spec)
    ).filter(col("avg_streams") == col("max_streams"))
    
    print("\nStep 3: Max Streams Sample")
    max_streams.show(5, truncate=False)
    
    # Join with original data to get song details
    artist_ranks = filtered_df.join(
        max_streams,
        ["artist", "region", "date"]
    )
    
    print("\nStep 4: Artist Ranks Sample")
    artist_ranks.show(5, truncate=False)
    
    # Find best rank for each artist
    best_ranks = artist_ranks.groupBy("artist", "region", "date") \
        .agg(F.min("rank").alias("best_rank"))
    
    # Create final results
    final_results = artist_ranks.join(
        best_ranks,
        ["artist", "region", "date"]
    ).filter(col("rank") == col("best_rank")) \
    .withColumn("track_id", regexp_extract(col("url"), "track/([^?]+)", 1)) \
    .select(
        "artist",
        "title",
        "best_rank",
        "date",
        "region",
        "track_id",
        "avg_streams"
    ).orderBy("date", "region")
    
    return final_results

def main():
    spark = create_spark_session()
    
    # Create temp directory for Spark
    os.makedirs("spark_temp", exist_ok=True)
    
    input_path = "./charts.parquet"
    year = 2021
    month = 9
    
    print(f"\nAnalyzing Spotify charts for {year}-{month:02d}")
    
    # Get and show results
    results = analyze_spotify_charts(spark, input_path, year, month)
    print("\nFinal Results:")
    results.show(20, truncate=False)
    
    print("\nTotal records in final results:", results.count())
    
    spark.stop()
    
    # Clean up temp directory
    if os.path.exists("spark_temp"):
        shutil.rmtree("spark_temp")

if __name__ == "__main__":
    main() 