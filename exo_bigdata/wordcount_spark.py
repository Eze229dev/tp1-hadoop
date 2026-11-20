from pyspark import SparkContext

sc = SparkContext("local[*]", "WordCountSpark")
sc.setLogLevel("ERROR")

counts = (sc.textFile("gros_fichier.txt")
            .flatMap(lambda line: line.split())
            .map(lambda word: (word, 1))
            .reduceByKey(lambda a, b: a + b))

counts.saveAsTextFile("output_spark")

for word, count in sorted(counts.collect()):
    print(f"{word}\t{count}")

sc.stop()
