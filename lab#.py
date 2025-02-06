# SHOW DATASETS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, upper, substring, rand
import os

spark = SparkSession.builder.appName("Cereal").getOrCreate()

data_path = os.path.join(os.getcwd(), "cereal.csv") 
print(f"Reading CSV from: {data_path}")

df = spark.read.csv(data_path, header=True, inferSchema=True)

# Display the DataFrame to verify
df.show()

# PARTITIONING STRATEGY #1: RANGE PARTIONING

print("Partition 1: Range Partitioning")
range_partitioned_df = df.orderBy(col("rating")).repartitionByRange(4, col("rating"))
print(f"Number of partitions (Range): {range_partitioned_df.rdd.getNumPartitions()}")

for i in range(range_partitioned_df.rdd.getNumPartitions()):
    print(f"Partition {i}:")
    # Get data for the current partition as a DataFrame
    partition_data = range_partitioned_df.rdd.mapPartitionsWithIndex(lambda index, it: it if index == i else [])
    # Create a DataFrame from the partition data
    partition_df = spark.createDataFrame(partition_data, df.schema) 
    # TRANSFORMATION PIPELINE
    # Now you can use groupBy on the DataFrame
    partition_df.groupBy("mfr").max("sugars").show() 

# PARTITIONING STRATEGY #2: CUSTOM PARTITIONING

print("Partition 2: Custom Partitioning")
from pyspark.sql.functions import when

df = df.withColumn(
    "partition_category",
    when(rand() < 0.5, "Partition_1").otherwise("Partition_2")
)

# Repartition correctly
name_partitioned_df = df.repartition(2, col("partition_category"))

# Verify partition count
print(f"Number of partitions (name): {name_partitioned_df.rdd.getNumPartitions()}")

# Process partitions correctly
for partition_value in ["Partition_1", "Partition_2"]:
    print(f"\nPartition: {partition_value}")
    
    # Filter data for the current partition
    partition_df = name_partitioned_df.filter(col("partition_category") == partition_value)
    
    # TRANSFORMATION PIPELINE
    partition_df.groupBy("mfr").max("calories").show()
    partition_df.groupBy("mfr").avg("rating").show()