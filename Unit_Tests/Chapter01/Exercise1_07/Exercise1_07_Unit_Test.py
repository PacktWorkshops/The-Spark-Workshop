import unittest
from pyspark.sql.tests import ReusedSQLTestCase


class TreeReduceTestCase(ReusedSQLTestCase):
    def test_treereduce(self):
        def add(left, right):
            return left + right

        words = ["Settlements", "some", "centuries", "old", "and", "still", "no", "bigger", "than", "pinheads", "on",
                 "the", "untouched", "expanse", "of", "their", "background"]
        words_rdd = self.spark.sparkContext.parallelize(words)
        word_lengths = words_rdd.map(lambda word: len(word))
        self.assertEqual(word_lengths.treeReduce(add, 1), 93)
        self.assertEqual(word_lengths.treeReduce(add, 2), 93)
        self.assertEqual(word_lengths.treeReduce(add, 3), 93)
        self.assertEqual(word_lengths.treeReduce(add, 1), word_lengths.reduce(add))
        self.assertEqual(word_lengths.treeReduce(add, 2), word_lengths.reduce(add))
        self.assertEqual(word_lengths.treeReduce(add, 3), word_lengths.reduce(add))


if __name__ == '__main__':
    unittest.main()
