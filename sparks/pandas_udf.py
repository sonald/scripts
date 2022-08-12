import os
import sys
print(os.environ['PYTHONPATH'])
print(os.getcwd())
for p in sys.path:
    print(p)

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from typing import Iterator

def run():
    import pandas as pd

    @F.pandas_udf("long")
    def pd_plus_one(v: pd.Series) -> pd.Series:
        return v+1

    spark = SparkSession.builder.appName('pandas udf').getOrCreate()
    # spark.sparkContext.addPyFile('/jobs/pyspark_env.zip')

    df = spark.range(1000)
    df.withColumn('plus_one', pd_plus_one('id')).show()

    spark.stop()

run()