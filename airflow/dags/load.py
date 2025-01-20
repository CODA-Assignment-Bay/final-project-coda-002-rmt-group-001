'''
=================================================

Milestone 3

Nama : Afif Makruf<br>
Batch : RMT-002

File ini dibuat untuk membuat koneksi dan melakukan load data ke database MongoDB.

=================================================
'''

from pymongo.mongo_client import MongoClient
import pandas as pd

path = '/opt/airflow/logs/data_staging'

def load(data):
    '''
    Fungsi ini ditujukan untuk melakukan load data ke dalam database.
    Parameters:
        data : dataframe spark
    return:
        data_cleaned : dataframe hasil transform (cleaning) yang dikonversi menjadi pandas dataframe 
    '''
    # Membuat koneksi ke MongoDB
    mongodb_uri = 'mongodb+srv://afifmakruf:afifmakruf@afif-playground.cj4fn.mongodb.net/'
    client = MongoClient(mongodb_uri)
    db = client['p2m3']
    collection = db['etl_pipeline']

    # Konversi dataframe ke dalam bentuk list dictionaries
    data = data.to_dict(orient='records')

    # Memasukkan data ke dalam collection
    if data :
        collection.insert_many(data)
    else:
        print('No data to insert.')

    # Menutup koneksi
    client.close()

if __name__ == "__main__":
    df = pd.read_csv(f'{path}/P2M3_afif_makruf_data_cleaned.csv')
    load(df)