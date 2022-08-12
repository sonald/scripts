#!/usr/bin/env bash

if [[ ! $# -eq 1 ]]; then
    echo "need job"
    exit 1
fi

if [ ! -f pyspark_env.tar.gz ]; then
    conda create -y -n pyspark_env -c conda-forge pyarrow pandas conda-pack 
    conda activate pyspark_env 
    conda pack -f -o pyspark_env.tar.gz 
fi

job=$1
PKGS=io.delta:delta-core_2.12:2.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0
#docker-compose exec \
    #-e PYSPARK_DRIVER_PYTHON=python -e PYSPARK_PYTHON=./environment/bin/python \
    #spark-master spark-submit --packages $PKGS \
    #--archives /jobs/pyspark_env.tar.gz#environment \
    #--master spark://spark-master:7077 /jobs/$job

#docker-compose exec \
    #spark-master sh -c "export PYSPARK_DRIVER_PYTHON=python; export PYSPARK_PYTHON=./environment/bin/python; \
    #spark-submit --packages $PKGS \
    #--archives /jobs/pyspark_env.tar.gz#environment \
    #--conf spark.archives=/jobs/pyspark_env.tar.gz#environment \
    #--master spark://spark-master:7077 /jobs/$job"

docker-compose exec spark-master sh -x /jobs/job.sh $job

