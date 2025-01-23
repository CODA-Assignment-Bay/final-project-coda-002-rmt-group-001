from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col
from pyspark.sql.types import IntegerType, DateType, FloatType, DateType, StringType, BooleanType


if __name__ == '__main__':
    # Path ke folder output
    path = '/opt/airflow/data/'

    spark = SparkSession.builder \
        .appName("WriteToPostgres") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()
    spark

    fact_comment_table = spark.read.parquet(f'{path}fact_comment_table')
    fact_assault_table = spark.read.parquet(f'{path}fact_assault_table')
    dim_user_table = spark.read.parquet(f'{path}dim_user_table')
    dim_post_table = spark.read.parquet(f'{path}dim_post_table')
    dim_date_table = spark.read.parquet(f'{path}dim_date_table')

    dim_user_table_cleaned = dim_user_table.withColumn("user_account_created_time", to_timestamp("user_account_created_time"))\
    .withColumn("user_is_verified", col("user_is_verified").cast("boolean"))\
    .withColumn("user_awardee_karma", col("user_awardee_karma").cast("int"))\
    .withColumn("user_awarder_karma", col("user_awarder_karma").cast("int"))\
    .withColumn("user_link_karma", col("user_link_karma").cast("int"))\
    .withColumn("user_comment_karma", col("user_comment_karma").cast("int"))\
    .withColumn("user_total_karma", col("user_total_karma").cast("int"))

    dim_post_table_cleaned = dim_post_table.withColumn("post_score", col("post_score").cast("int"))\
    .withColumn("post_upvote_ratio", col("post_upvote_ratio").cast("float"))\
    .withColumn("post_thumbs_ups", col("post_thumbs_ups").cast("int"))\
    .withColumn("post_total_awards_received", col("post_total_awards_received").cast("int"))\
    .withColumn("post_created_time", to_timestamp("post_created_time"))

    dim_date_table_cleaned = dim_date_table.withColumn("month", col("month").cast("int"))\
    .withColumn("year", col("year").cast("int"))

    fact_comment_table_cleaned = fact_comment_table.withColumn("created_time", to_timestamp("created_time"))\
    .withColumn("score", col("score").cast("int"))\
    .withColumn("controversiality", col("controversiality").cast("float"))

    print(fact_comment_table_cleaned.dtypes)    
    print(fact_assault_table.dtypes)    
    print(dim_post_table_cleaned.dtypes)    
    print(dim_user_table_cleaned.dtypes)    
    print(dim_date_table_cleaned.dtypes)    

    # PostgreSQL JDBC Connection
    postgres_url = "jdbc:postgresql://host.docker.internal:5432/final_project"
    postgres_properties = {
        "user": "afifmakruf",
        "password": "afifmakruf",
        "driver": "org.postgresql.Driver"
    }

    # Write DataFrame to PostgreSQL
    dim_user_table_cleaned.write \
        .jdbc(url=postgres_url, table="dim_user", mode="append", properties=postgres_properties)
    fact_assault_table.write \
        .jdbc(url=postgres_url, table="fact_assault", mode="append", properties=postgres_properties)
    dim_post_table_cleaned.write \
        .jdbc(url=postgres_url, table="dim_post", mode="append", properties=postgres_properties)
    dim_date_table_cleaned.write \
        .jdbc(url=postgres_url, table="dim_date", mode="append", properties=postgres_properties)
    fact_comment_table_cleaned.write \
        .jdbc(url=postgres_url, table="fact_comment", mode="append", properties=postgres_properties)