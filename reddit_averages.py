from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json

def add_pairs(a,b):
        return (a[0]+b[0],a[1]+b[1])

def get_key(kv):
        return kv[0]

def output_format(avg_tuple):
    a, b = avg_tuple
    return '[%s %.15f]' % (a, b)

def main(inputs, output):
    data = sc.textFile(inputs)
    jsondata = data.map(lambda x: json.loads(x))
    kvrdd = jsondata.map(lambda x: (x['subreddit'],(1,x['score'])))
    summed = kvrdd.reduceByKey(add_pairs)
    average = summed.map(lambda x: (x[0],x[1][1]/x[1][0]))

    outdata = average.sortBy(get_key).map(output_format)
    outputdata = outdata.map(lambda x: json.dumps(x))
    outputdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
