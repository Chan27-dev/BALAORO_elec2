
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

range_partitioned_df = df.orderBy(col("rating")).repartitionByRange(4, col("rating"))
print(f"Number of partitions (Range): {range_partitioned_df.rdd.getNumPartitions()}")

for i in range(range_partitioned_df.rdd.getNumPartitions()):
    print(f"Partition {i}:")
    partition_data = range_partitioned_df.rdd.mapPartitionsWithIndex(lambda index, it: it if index == i else []).collect()
    for row in partition_data:
        print(row) 
        
# TRANSFORMATION PIPELINE
range_partitioned_df.groupBy("mfr").avg("rating").show()

# PARTITIONING STRATEGY #2: CUSTOM PARTIONING
df = df.withColumn(
    "sugar_category",
    when(df["sugars"] < 5, "Low Sugar")
    .when((df["sugars"] >= 5) & (df["sugars"] < 10), "Medium Sugar")
    .otherwise("High Sugar")
)

sugar_partitioned_df = df.repartition(3, "sugar_category")

print(f"Number of partitions (Sugar Partitioning): {sugar_partitioned_df.rdd.getNumPartitions()}")
sugar_partitioned_df.groupBy("sugar_category").count().show()

# TRANSFORMATION PIPELINE
sugar_partitioned_df.groupBy("sugar_category").max("calories").show()
sugar_partitioned_df.groupBy("sugar_category").avg("rating").show()


