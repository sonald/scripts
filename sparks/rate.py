from pyspark.sql import SparkSession
import pyspark.sql.functions as F

ss = SparkSession.builder.appName('spark rate').getOrCreate()

df = ss.readStream.format('rate').option('rowsPerSecion', 2).load()

df = df.groupBy(F.window(F.col('timestamp'), '10 seconds')).count()

query = df.writeStream.format('console').option('truncate', False).outputMode('complete').start()

query.awaitTermination()

ss.stop()