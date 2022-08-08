from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import BooleanType
import random

def in_circle(x):
    x, y = random.random(), random.random()
    return x * x + y * y <= 1.0

spark = SparkSession.builder.appName('Pi').getOrCreate()
f = F.udf(in_circle, BooleanType())
spark.udf.register('inCircle', f)
spark.sparkContext.setLogLevel('WARN')

NUM_SAMPLES = 10_000_000
rdd = spark.sparkContext.parallelize(range(0, NUM_SAMPLES))
df = spark.createDataFrame(rdd, 'int')
df.printSchema()
df.createOrReplaceTempView('samples')
total = spark.sql('select * from samples where inCircle(value) == true').count()
# total = df.filter(f(df.value)).count()

print(f"Pi: {4.0 * float(total) / NUM_SAMPLES}")

spark.stop()