import unittest

from pyspark import RDD
from pyspark.sql.tests import ReusedSQLTestCase


class FiltersTestCase(ReusedSQLTestCase):

    def test_filters(self):
        # test code comes here
        def begins_with_o(word):
            if word.startswith("o"):
                return word,  # single element tuple
            else:
                return ()  # empty tuple

        words = ["Settlements", "some", "centuries", "old", "and", "still", "no", "bigger", "than", "pinheads", "on", "the",
                 "untouched", "expanse", "of", "their", "background"]
        words_rdd: RDD = self.spark.sparkContext.parallelize(words)
        o_words_filter: RDD = words_rdd.filter(lambda word: word.startswith("o"))
        o_words_flatmap: RDD = words_rdd.flatMap(lambda word: begins_with_o(word))

        self.assertEqual(o_words_filter.count(), o_words_flatmap.count())
        self.assertEqual(o_words_filter.count(), 3)
        self.assertEqual(o_words_filter.collect(), o_words_flatmap.collect())


if __name__ == '__main__':
    unittest.main()