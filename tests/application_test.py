import os
import datetime
import unittest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

from src.application import highest_values_per_year

class ApplicationTest(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("app").getOrCreate()
        
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        
    def test_highest_value(self):
        df = self.spark.createDataFrame([
            {"date": datetime.date.fromisoformat("2024-01-01"), "close":2.0, "open":1.0}
        ])
        expected= self.spark.createDataFrame([
           {"date": datetime.date.fromisoformat("2024-01-01"), "close":2.0, "open":1.0} 
        ])
        actual = highest_values_per_year(df)
        assertDataFrameEqual(actual, expected)

if __name__== "__main__":
    unittest.main()