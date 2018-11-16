from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import random
import operator

def partitions_iters(partitions):
    iters = 0
    for i in partitions:
        total = 0.0
        while total < 1:
            total += random.random()
            iters += 1
    return iters

def main(inputs):

    random.seed(10)
    rdd = sc.range(int(inputs),numSlices = 8).glom()
    iterations = rdd.map(partitions_iters)
    print(iterations.reduce(operator.add)/int(inputs))

if __name__ == '__main__':
    conf = SparkConf().setAppName('euler')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    main(inputs)

