'''
=================================================

Program ini dibuat untuk melakukan extract data dari dataset.

=================================================
'''
from pyspark.sql import SparkSession


path = '/opt/airflow/dags/'

def extract(path):
    
    spark = SparkSession.builder.getOrCreate()
    data1 = spark.read.csv(f'{path}reddit_opinion_PSE_ISR.csv', header=True, inferSchema=True)
    
    data2 = spark.read.csv(f'{path}assault.csv', header=True, inferSchema=True)
    
    return data1, data2

if __name__ == '__main__':
    extract(path)
    