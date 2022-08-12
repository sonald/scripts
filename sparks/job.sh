job=$1
PKGS=io.delta:delta-core_2.12:2.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0

# export PYSPARK_DRIVER_PYTHON=python
# export PYSPARK_PYTHON=./environment/bin/python
# spark-submit -v --packages $PKGS \
#     --archives file:/jobs/pyspark_env.zip#environment \
#     --master spark://spark-master:7077 /jobs/$job


# --archives only works with k8s deployment
# spark-submit -v --packages $PKGS \
#     --archives /jobs/pyspark_env.zip \
#     --master spark://spark-master:7077 /jobs/$job
 
 spark-submit --packages $PKGS --master spark://spark-master:7077 /jobs/$job