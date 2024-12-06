from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

class Project2:           
    def run(self, inputPath, outputPath, stopwords, k):
        spark = SparkSession.builder.master("local").appName("project2_df").getOrCreate()
        
        # get argument
        text_df = spark.read.csv(inputPath, header = False, inferSchema = True)
        text_df = text_df.withColumnRenamed("_c0", "year").withColumnRenamed("_c1", "terms")
        text_df = text_df.withColumn("year", substring("year", 1, 4))
        n = int(stopwords)
        k = int(k)

        # get N data (the number of headlines in y)
        B_df = text_df.groupBy("year").count().withColumnRenamed("count", "N")

        # get (year, term)
        split_df = text_df.withColumn("terms", split(text_df["terms"], " "))
        distinct_split_df = split_df.withColumn("terms", array_distinct(col("terms")))

        split_df_1 = split_df.withColumn("term", explode(split_df["terms"]))
        split_df_2 = distinct_split_df.withColumn("term", explode(distinct_split_df["terms"]))

        # get ni data (the number of headlines in y having t)
        C_df = split_df_2.groupBy("year", "term").agg(count("terms").alias("ni"))

        # get tf data (the frequency of t in y)
        A_df = split_df_1.groupBy("year", "term").agg(count("term").alias("tf"))

        # join ni and tf data
        AC_df = C_df.join(A_df, on=["year", "term"], how="inner")

        # get stopwords(n)
        sw_df = split_df_1.groupBy("term").agg(count("term").alias("count"))
        sw_df = sw_df.orderBy(["count", "term"], ascending=[0, 1])
        sw_df = sw_df.limit(n)

        # filter stopwords
        fl_df = AC_df.join(sw_df, on="term", how="left")
        fl_df = fl_df.filter(sw_df["count"].isNull())
        fl_AC_df = fl_df.select(AC_df["year"], AC_df["term"], AC_df["ni"], AC_df["tf"])

        # compute weight
        ABC_df = fl_AC_df.join(B_df, on="year", how="left")
        weight_df = ABC_df.select("year", "term", "ni", "tf", "N", (log10(ABC_df["tf"]) * log10(ABC_df["N"] / ABC_df["ni"])).alias("weight"))
        weight_df = weight_df.groupBy("term").agg(avg("weight").alias("avg_weight"))

        # sorted weight
        sorted_weight_df = weight_df.orderBy(["avg_weight", "term"], ascending=[0, 1])

        # take k (k)
        result_df = sorted_weight_df.limit(k)

        # submmit result
        result_df = result_df.withColumn("term", concat(lit('\"'), result_df["term"], lit('\"')))
        result = result_df.withColumn("result", concat_ws("\t",result_df["term"], result_df["avg_weight"]))
        result = result.select("result")
        result.coalesce(1).write.text(outputPath)

        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])

