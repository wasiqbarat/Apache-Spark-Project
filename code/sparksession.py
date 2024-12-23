import os
import sys
import shutil
import pandas as pd
from pathlib import Path
from pyspark.sql import SparkSession


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['HADOOP_HOME'] = str(Path("C:/hadoop").absolute())


base_dir = Path("C:/spark_local")
if base_dir.exists():
    shutil.rmtree(base_dir)
base_dir.mkdir(parents=True)

temp_dir = base_dir / "temp"
temp_dir.mkdir(parents=True)
os.chmod(str(temp_dir), 0o777)

spark = SparkSession.builder \
    .appName("SparkLocal") \
    .master("local[1]") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .config("spark.python.worker.memory", "512m") \
    .config("spark.local.dir", str(temp_dir)) \
    .config("spark.sql.codegen.wholeStage", False) \
    .config("spark.sql.execution.arrow.pyspark.enabled", True) \
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")

print("Spark session configured.")

try:
    pdf = pd.DataFrame({
        'name': ['test1', 'test2'],
        'value': [1, 2]
    })
    
    test_data = spark.createDataFrame(pdf)
    
    output_dir = base_dir / "output"
    output_dir.mkdir(exist_ok=True)
    output_path = str(output_dir / "test_data")
    
    pdf.to_csv(output_path + ".csv", index=False)
    print(f"Successfully wrote CSV to {output_path}.csv")
    
    read_data = spark.read.csv(output_path + ".csv", header=True)
    print("\nTest Results:")
    read_data.show()

except Exception as e:
    print("\nError during operations:")
    print(str(e))
    import traceback
    traceback.print_exc()
    raise

finally:
    try:
        spark.stop()
        print("\nSpark session stopped")
    except Exception as e:
        print(f"Error stopping Spark: {e}")
