from pyspark.sql import SparkSession
import pyspark.sql.functions as F


ss = SparkSession.builder.appName('spark rate').getOrCreate()
ss.sparkContext.setLogLevel('WARN')

stream1 = (ss.readStream.format('rate').option('rowsPerSecion', 2)
        .load()
        .withWatermark('timestamp', '10 seconds')
        .groupBy(F.window('timestamp', '5 seconds').alias('window'))
        .agg(F.sum('value').alias('total1')))
    
stream1.printSchema()

stream2 = (ss.readStream.format('rate').option('rowsPerSecion', 3)
        .load()
        .withWatermark('timestamp', '10 seconds')
        .groupBy(F.window('timestamp', '5 seconds').alias('window'))
        .agg(F.sum('value').alias('total2')))

#jointDF = (stream1
#    .join(stream2, 'window')
#    .withColumn('value', stream1.total1 + stream2.total2))
#
#query = (jointDF.writeStream
#    .format('console')
#    .outputMode('append')
#    .trigger(processingTime='5 seconds')
#    .option('truncate', False)
#    .start())

query2 = (stream1.writeStream
    .format('console')
    .outputMode('append')
    .trigger(processingTime='5 seconds')
    .option('truncate', False)
    .start())


query2.awaitTermination()