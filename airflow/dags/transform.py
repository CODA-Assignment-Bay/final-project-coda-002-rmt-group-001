'''
=================================================

Milestone 3

Nama : Afif Makruf<br>
Batch : RMT-002

File ini dibuat untuk melakukan transform (cleaning) data raw untuk selanjutnya dapat dimuat ke database.

=================================================
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, to_date
from pyspark.sql.types import IntegerType, FloatType

path = '/opt/airflow/logs/data_staging'
spark = SparkSession.builder.getOrCreate()
data = spark.read.csv(f'{path}/P2M3_afif_makruf_data_raw.csv', header=True, inferSchema=True)

# Fungsi untuk mencoba mengonversi string menjadi integer atau float
def try_convert(value):
    '''
    Fungsi ini ditujukan untuk melakukan pengubahan data pada kolom numeric apabila terdapat data selain numeric, maka akan diubah terlebih dahulu.

    Parameters:
        value : value dari kolom
    return:
        int : hasil pengubahan apabila berhasil diubah ke dalam integer
        float :  hasil pengubahan apabila berhasil diubah ke dalam float
        None : hasil pengubahan apabila tidak berhasil diubah ke dalam integer ataupun float, dijadikan None atau Null 
  '''
    try:
        # Coba konversi menjadi integer
        return int(value)
    except ValueError:
        try:
            # Coba konversi menjadi float
            return float(value)
        except ValueError:
            # Jika gagal, kembalikan None
            return None

# Mendaftarkan UDF
convert_udf = udf(try_convert)

def transform(data):
    '''
    Fungsi ini ditujukan untuk melakukan transform (cleaning) data untuk selanjutnya dilakukan load ke dalam database.

    Parameters:
        data : dataframe spark
    return:
        data_cleaned : dataframe hasil transform (cleaning) yang dikonversi menjadi pandas dataframe 
  '''
    # Mengubah nama kolom
    data_clean = data.withColumnRenamed('Order Id','order_id')\
            .withColumnRenamed('Order Date','order_date')\
            .withColumnRenamed('Product Id','product_id')\
            .withColumnRenamed('Product Category','product_category')\
            .withColumnRenamed('Buyer Gender','buyer_gender')\
            .withColumnRenamed('Buyer Age','buyer_age')\
            .withColumnRenamed('Order Location','order_location')\
            .withColumnRenamed('International Shipping','international_shipping')\
            .withColumnRenamed('Sales Price','sales_price')\
            .withColumnRenamed('Shipping Charges','shipping_charges')\
            .withColumnRenamed('Sales per Unit','sales_per_unit')\
            .withColumnRenamed('Quantity','quantity')\
            .withColumnRenamed('Total Sales','total_sales')\
            .withColumnRenamed('Rating','rating')\
            .withColumnRenamed('Review','review')
    
    # Apply UDF untuk kolom yang seharusnya numerik (contoh: 'buyer_age', 'sales_price', dll.)
    columns_to_convert = ['buyer_age', 'sales_price', 'shipping_charges', 'sales_per_unit', 'quantity', 'total_sales', 'rating']
    
    for column in columns_to_convert:
        data_clean = data_clean.withColumn(column, convert_udf(col(column)))

    # Format order_date ke dalam format %Y-%m-%d jika tidak sesuai
    data_clean = data_clean.withColumn(
        "order_date", 
        to_date(col("order_date"), "yyyy-MM-dd").alias("order_date")
    )

    # Konversi ke pandas dataframe
    data_cleaned = data_clean.toPandas()

    # Konversi dataframe ke file csv dengan path yang spesifik
    data_cleaned.to_csv(f'{path}/P2M3_afif_makruf_data_cleaned.csv', index=False)

    return data_cleaned
if __name__ == '__main__':
    transform(data)