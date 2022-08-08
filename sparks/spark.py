import random
import sys
import socket

import pyspark
from pyspark.sql import SparkSession


def top5(words_rdd):
    sorted_words = words_rdd.map(lambda w: (w, 1)) \
        .reduceByKey(lambda acc, x: acc + x) \
        .map(lambda x: (x[1], x[0])) \
        .sortByKey(False)
    t5 = sorted_words.take(5)
    print(f"top5 = {t5}")

    for i in range(len(t5)):
        print(f"{i}: {t5[i]}")


def group(words_rdd):
    def seq_fun(l, x):
        l.append(x)
        return l

    data = words_rdd.map(lambda x: (x, random.random()))\
        .aggregateByKey([], seq_fun, lambda l, x: l)\
        .take(6)

    for i in range(len(data)):
        print(f"{i} len:{len(data[i][1])} {data[i]}")


if __name__ == "__main__":
    print("submit wc")
    spark = SparkSession.builder.appName('wordCount').getOrCreate()
    filename = sys.argv[1] if len(sys.argv) > 1 else './wiki.txt'
    textfile = spark.sparkContext.textFile(filename)
    words = textfile.flatMap(lambda ln: ln.split(' ')).filter(lambda w: len(w) != 0)
    top5(words)
    group(words)

    spark.stop()
