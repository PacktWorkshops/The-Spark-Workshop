from utilities01_py.helper_python import *
from pyspark.rdd import RDD

if __name__ == "__main__":
    words = ["Settlements", "some", "centuries", "old", "and", "still", "no", "bigger", "than", "pinheads", "on", "the",
              "untouched", "expanse", "of", "their", "background"]
    session = create_session(2, "Python map closure")
    words_rdd: RDD = session.sparkContext.parallelize(words)  # Creation of Spark data structure
    wordLengths: RDD = words_rdd.map(lambda word: len(word))  # passing a closure to Spark's `map`
    print(wordLengths.collect())  # printing result to stdout
    # Result: [11, 4, 9, 3, 3, 5, 2, 6, 4, 8, 2, 3, 9, 7, 2, 5, 10] order might differ
