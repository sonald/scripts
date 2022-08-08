import pyspark
from pyspark.sql import SparkSession, functions as F

ss = SparkSession.builder.appName('wc stream').getOrCreate()

df = ss.readStream.format('socket')\
    .option('host', '127.0.0.1')\
    .option('port', '9999')\
    .load()

# with event time
counts = df\
    .withColumn('time&words', F.split(df.value, ",")) \
    .withColumn('eventTime', F.element_at("time&words", 1).cast("timestamp")) \
    .withColumn('words', F.element_at("time&words", 2)) \
    .withColumn('word', F.explode(F.col('words'))) \
    .withWatermark('eventTime', '10 minutes') \
    .groupBy(F.window(F.col('eventTime'), '5 minutes'), F.col('word')) \
    .count()

query = counts.writeStream.format('console').option('truncate', False).outputMode('complete')\
    .trigger(processingTime='1 seconds') \
    .start()

query.explain()
