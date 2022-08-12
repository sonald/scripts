from pyspark.sql import SparkSession
import pyspark.sql.functions as F

class DumpStream:
    def open(self, partition_id, epoch_id):
        print(f"partition: {partition_id}, epoch: {epoch_id}")
        return True
    
    def process(self, row):
        print(row)

    def close(self, error):
        print("close stream")

spark = SparkSession.builder.appName('Kafka2').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

topic = (spark.readStream.format('kafka')
    #.option('kafka.bootstrap.servers', '172.28.0.9:9092')
    .option('kafka.bootstrap.servers', 'kafka:9092')
    .option('subscribe', 'events')
    .option('startingOffsets', 'earliest')
    .load())


topic.printSchema()
df = topic.selectExpr('cast(key as string)', 'cast(value as string)', 'offset', 'timestamp')

query = (df.writeStream
          .foreach(DumpStream())
          .format('kafka')
          .option('kafka.bootstrap.servers', 'kafka:9092')
          .option('checkpointLocation', '/tmp/spark-kafka/checkpoints')
          .option('topic', 'output')
          .start())

query.awaitTermination()

spark.stop()