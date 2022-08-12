from pyspark.sql import SparkSession
import pyspark.sql.functions as F
# import delta
# from delta.pip_utils import configure_spark_with_delta_pip

spark = (SparkSession.builder.appName('delta test')
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate())
spark.sparkContext.setLogLevel('WARN')

#spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()

delta_tbl_path = '/jobs/delta/lucky'
(spark.read.format('parquet').load('/jobs/yaohao/lucky')
        .write.format('delta').mode('ignore').save(delta_tbl_path))

df = spark.read.format('delta').load(delta_tbl_path)
df.createOrReplaceTempView('lucky')

df.printSchema()
spark.catalog.listDatabases()
spark.catalog.listTables()

# df.groupBy('carNum').count().show()
# spark.sql('select carNum, count(*) as times from lucky group by carNum').show()

spark.sql('select count(*) from lucky').show()

spark.stop()
