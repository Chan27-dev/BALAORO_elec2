
# SHOW DATASETS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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
    partition_data = range_partitioned_df.rdd.mapPartitionsWithIndex(lambda index, it: it if index == i else []).collect()
    # TRANSFORMATION PIPELINE
    partition_data.groupBy("mfr").avg("rating").show()

    # for row in partition_data:
    #     print(row) 
        
# PARTITIONING STRATEGY #2: CUSTOM PARTIONING

print("Partition 2: Custom Partitioning")
df = df.withColumn(
    "sugar_category",
    when(df["sugars"] < 5, "Low Sugar")
    .when((df["sugars"] >= 5) & (df["sugars"] < 10), "Medium Sugar")
    .otherwise("High Sugar")
)

sugar_partitioned_df = df.repartition(3, "sugar_category")

print(f"Number of partitions (Sugar Partitioning): {sugar_partitioned_df.rdd.getNumPartitions()}")
for i in range(sugar_partitioned_df.rdd.getNumPartitions()):
    print(f"Partition {i}:")
    partition_data = sugar_partitioned_df.rdd.mapPartitionsWithIndex(lambda index, it: it if index == i else []).collect()
    # TRANSFORMATION PIPELINE
    partition_data.groupBy("sugar_category").max("calories").show()
    partition_data.groupBy("sugar_category").avg("rating").show()





