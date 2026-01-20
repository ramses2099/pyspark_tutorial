import os
import datetime
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import year, col, row_number

# Set the environment variable
os.environ['ARROW_PANDAS_EXPECT_EXTENSION_ARRAY'] = '1'

def highest_values_per_year(df: DataFrame) -> DataFrame:
    window = Window.partitionBy(year(col('date'))).orderBy(col('close').desc())
    return df.withColumn('rank', row_number().over(window)).filter(col('rank')==1).drop('rank')
            
if __name__ == "__main__":
    spark: SparkSession = SparkSession.builder.appName("app").getOrCreate()
    df = spark.createDataFrame([
        {"date":datetime.date.fromisoformat("2024-01-01"),"close":2.0, "open":1.0}
    ])
    highest_values_per_year(df).show()