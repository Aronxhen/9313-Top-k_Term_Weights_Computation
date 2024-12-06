 from pyspark import SparkContext, SparkConf
import sys
import math

class Project2:   
        
    def run(self, inputPath, outputPath, stopwords, k):
        conf = SparkConf().setAppName("project2_rdd").setMaster("local")
        sc = SparkContext(conf=conf)
        
        # get argument
        text = sc.textFile(inputPath)
        n = int(stopwords)
        k = int(k)

        # get orginal data
        data_1 = text.map(lambda x: x.split(","))
        data_2 = data_1.map(lambda x: (x[0][:4], (x[0][:4], x[1])))
        data_3 = data_2.map(lambda x: (x[0], (x[1][0], " ".join(set(x[1][1].split())))))
        
        # get N data (the number of headlines in y)
        B_result = data_2.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)

        # get ni data (the number of headlines in y having t)
        C_result = data_3.flatMap(lambda x:[(x[0] + "+" + word, 1) for word in x[1][1].split()]).reduceByKey(lambda a, b: a + b)

        # get tf data (the frequency of t in y)
        A_result = data_2.flatMap(lambda x: [(x[0] + "+" + word, (x[0], word, 1)) for word in x[1][1].split()]).reduceByKey(
            lambda a, b: (a[0], a[1], a[2] + b[2])
        )
        
        # join ni and tf data
        AC_result = A_result.join(C_result)
        AC_result = AC_result.map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][0][2], x[1][1])))

        # get stopwords (n)
        sorted_AC_result = AC_result.map(lambda x: (x[1][1], x[1][2])).reduceByKey(lambda a, b: a + b).sortBy(lambda x: (-x[1], x[0]))
        sw_result = sorted_AC_result.take(n) # stopwords
        sw_set = set()
        sw_set.update(word[0] for word in sw_result)

        # filter stopwords
        rm_sw_result = AC_result.filter(lambda x: x[1][1] not in sw_set)

        # compute weights
        AC_result_2 = rm_sw_result.map(lambda x: (x[1][0], (x[1][0], x[1][1], x[1][2], x[1][3])))
        ABC_result = AC_result_2.join(B_result)
        weights_result_1 = ABC_result.map(lambda x: (x[1][0][1], (math.log10(x[1][0][2]) *  math.log10(x[1][1] / x[1][0][3]), 1)))
        weights_result_2 = weights_result_1.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        weights_result_3 = weights_result_2.map(lambda x: (x[0],  x[1][0] / x[1][1]))

        # sorted weight(k)
        sorted_weights_result = weights_result_3.sortBy(lambda x: (-x[1], x[0])).zipWithIndex().filter(lambda x: x[1] < k).map(lambda x: x[0])
        
        #get result
        result = sorted_weights_result.map(lambda x: f"\"{x[0]}\"\t{x[1]}")
        result.coalesce(1).saveAsTextFile(outputPath)

        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])

