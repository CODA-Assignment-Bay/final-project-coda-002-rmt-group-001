from pyspark.sql import SparkSession
import kagglehub
import os
import pandas as pd


path = '/opt/airflow/dags/'
data_path = '/opt/airflow/data/'


def extract(path):
    
    spark = SparkSession.builder.getOrCreate()

    if os.path.exists(f'{path}reddit_opinion_PSE_ISR.csv'):
        data1 = spark.read.csv(f'{path}reddit_opinion_PSE_ISR.csv', header=True, inferSchema=True, quote='"', escape='"', multiLine=True, encoding="UTF-8")
    else:
        path_download = kagglehub.dataset_download("asaniczka/reddit-on-israel-palestine-daily-updated")
        print("Path to dataset files:", path_download)
        if not os.path_download.exists(path):
            os.makedirs(path)
        os.system("cp -r {}/* {}".format(path_download, path))
        print("Path to dataset files:", path_download)

    data2 = spark.read.csv(f'{path}assault.csv', header=True, inferSchema=True, quote='"', escape='"', multiLine=True, encoding="UTF-8")
    
    data1 = data1.sample(withReplacement=False, fraction=0.001, seed=42)

    data1.write.parquet(f'{data_path}data_reddit_raw')
    data2.write.parquet(f'{data_path}data_assault_raw')

    return data1, data2

if __name__ == '__main__':
    extract(path)