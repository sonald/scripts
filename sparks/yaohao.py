import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

# ss = SparkSession.builder.appName('yaohao').master('spark://127.0.0.1:7077').getOrCreate()
ss = SparkSession.builder.appName('yaohao')\
    .config('spark.executor.memory', '4g')\
    .config('spark.executor.cores', '1')\
    .config('spark.driver.memory', '8g')\
    .getOrCreate()

# data_path = "/home/sonald/yaohao/"
data_path = "/home/sonald/yaohao/"
if len(sys.argv) > 1:
    data_path = sys.argv[1]
print(f"data_path = {data_path}")
apply_df = ss.read.parquet(data_path + "/apply")
lucky_df = ss.read.parquet(data_path + 'lucky/')

filtered = lucky_df.filter(lucky_df.batchNum >= '201601').select(lucky_df.carNum)
# joint = apply_df.join(filtered, apply_df.carNum == filtered.carNum, "inner")
joint = apply_df.join(filtered, 'carNum', "inner")
counted = joint.groupBy(joint.batchNum, joint.carNum).count().alias('count')
# counted.show()

uniqued = counted.groupBy(counted.carNum).agg(F.max('count').alias('max_count'))
# uniqued.show()
totaled = uniqued.groupBy('max_count').agg(F.count('*').alias('total')).orderBy('max_count')
#totaled.cache()

total = totaled.collect()

print(f"total size: {len(total)}")
for i in total:
    print(total[i])

ss.stop()