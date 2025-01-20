'''
=================================================

Milestone 3

Nama : Afif Makruf<br>
Batch : RMT-002

File ini dibuat untuk melakukan ekstrak data dari sumber yang telah ditentukan dan mengubahnya menjadi file dengan ketentuan yang telah ditetapkan.

=================================================
'''

from pyspark.sql import SparkSession

path = '/opt/airflow/dags/'
st_path = '/opt/airflow/logs/data_staging'


def extract(path):
    '''
    Fungsi ini ditujukan untuk mengambil data dari local file untuk selanjutnya dilakukan transform (data cleaning)
    parameter:
        path : lokasi file yang akan diekstrak
    return:
        data : dataframe spark
    contoh penggunaan:
        load_data("/Users/afifmakruf/Desktop/Hacktiv8/RMT-002/Repo/airflow-with-spark/dags/")
    '''

    # Membuat spark engine
    spark = SparkSession.builder.getOrCreate()

    # Membuat datafram menggunakan spark engine pada path yang telah ditentukan
    data = spark.read.csv(f'{path}/merch_sales.csv', header=True, inferSchema=True)

    # Mengubah nama file dataframe dengan path spesifik
    data.toPandas().to_csv(f'{st_path}/P2M3_afif_makruf_data_raw.csv', index=False)
    
    return data

if __name__ == '__main__':
    extract(path)