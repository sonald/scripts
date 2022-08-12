from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import random
random.seed(42)

spark = SparkSession.builder.appName('join').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', -1)

states = {0:'AZ', 1:'CO', 2:"CA", 3:"TX", 4:"NY", 5:"MI"}
items = {0:"SKU-0", 1:"SKU-1", 2:"SKU-2", 3:"SKU-3", 4:"SKU-4", 5:"SKU-5"}


usersDF = (spark.sparkContext.parallelize(range(1_000_000))
           .map(lambda x: (x, f"user_{x}", f"user_{x}@example.com", states[random.randint(0,5)]))
           .toDF(["uid", "login", "email", "user_state"]))
# usersDF.show(truncate=False)

ordersDF = (spark.sparkContext.parallelize(range(1_000_000))
            .map(lambda x: (x, random.randint(1,1000), random.randint(1, 10_000), 10 * x * 0.2, states[random.randint(0,5)], items[random.randint(0,5)]))
            .toDF(["transaction_id", "quantity", "user_id", "amount", "state", "items"]))
# ordersDF.show(truncate=False)

usersDF.createOrReplaceTempView('Users')
ordersDF.createOrReplaceTempView('Orders')
# usersDF.orderBy(F.asc("uid"))\
#     .write.format("parquet").bucketBy(8, "uid").mode("overwrite")\
#         .option('path', '/opt/bitnami/spark/spark-warehouse/users').saveAsTable("Users")
# ordersDF.orderBy(F.asc("user_id"))\
#     .write.format("parquet").bucketBy(8, "user_id").mode("overwrite")\
#         .option('path', '/opt/bitnami/spark/spark-warehouse/orders').saveAsTable("Orders")
spark.sql("cache table Users")
spark.sql("cache table Orders")

# usersOrdersDF = ordersDF.join(usersDF, usersDF["uid"] == ordersDF["user_id"])
# usersOrdersDF.show()
# usersOrdersDF.explain(True)

usersBucketDF = spark.table("Users")
# ordersBucketDF = spark.table("Orders")
print(f"orders :#{usersBucketDF.count()}")

# joined = ordersBucketDF.join(usersBucketDF, usersBucketDF["uid"] == ordersBucketDF["user_id"])
joined = spark.sql("""
    select /*+ MERGE(orders, users) */ *, users.* from orders join users on users.uid == orders.user_id
""")
print(f"count = {joined.count()}")
joined.show()
joined.explain()

spark.stop()
