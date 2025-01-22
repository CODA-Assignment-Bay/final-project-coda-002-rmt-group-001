from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, concat_ws, date_format, to_date, year, month, expr, when, udf
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType
import pyspark.sql.functions as F
from datetime import datetime, timedelta
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk

spark = SparkSession.builder.getOrCreate()


def add_sentiment_analysis(data1):
    # Initialize SentimentIntensityAnalyzer
    sia = SentimentIntensityAnalyzer()
    # Define a UDF for sentiment score
    def sentiment_score_udf(text):
        if text:
            return sia.polarity_scores(text)['compound']
        return 0.0
    # Define a UDF for sentiment label
    def sentiment_label_udf(score):
        if score > 0.05:
            return 'Positive'
        elif score < -0.05:
            return 'Negative'
        else:
            return 'Neutral'

    # Register UDFs
    sentiment_score = udf(sentiment_score_udf, FloatType())
    sentiment_label = udf(sentiment_label_udf, StringType())

    # Apply UDFs to calculate sentiment score and label
    data1 = data1.withColumn('sentiment_score', sentiment_score(data1['self_text']))
    data1 = data1.withColumn('sentiment_label', sentiment_label(data1['sentiment_score']))
    
    return data1



def add_month_year_column(data2):
    data2 = data2.withColumn(
        "month_num",
        when(col("Month") == "January", "01")
        .when(col("Month") == "February", "02")
        .when(col("Month") == "March", "03")
        .when(col("Month") == "April", "04")
        .when(col("Month") == "May", "05")
        .when(col("Month") == "June", "06")
        .when(col("Month") == "July", "07")
        .when(col("Month") == "August", "08")
        .when(col("Month") == "September", "09")
        .when(col("Month") == "October", "10")
        .when(col("Month") == "November", "11")
        .when(col("Month") == "December", "12")
    )

    data2 = data2.withColumn("month_year", concat_ws("-", col("month_num"), col("year")))
    
    return data2



def data_cleaning(data1, data2):
    #handling missing value in data1
    data1_cleaned = data1.na.drop(subset=['post_id'])
    data1_cleaned = data1_cleaned.na.drop(subset=['post_title'])
    data1_cleaned = data1_cleaned.na.drop(subset=['comment_id'])
    data1_cleaned = data1_cleaned.na.drop(subset=['self_text'])
    data1_cleaned = data1_cleaned.na.drop(subset=['subreddit'])
    data1_cleaned = data1_cleaned.na.drop(subset=['author_name'])

    # kalau user is verified null maka isi jadi False
    #
    
    #handling missing value in data2
    data2_cleaned = data2.na.drop(subset=['country'])
    data2_cleaned = data2_cleaned.na.drop(subset=['month_year'])
    
    #handling duplicates
    data1_cleaned.dropDuplicates()
    data2_cleaned.dropDuplicates()
    
    return data1_cleaned, data2_cleaned

def add_date_table():
    # Define start and end dates
    start_date = datetime(2023, 9, 2)
    end_date = datetime(2025, 1, 18)

    # Generate list of distinct dates
    date_list = [(start_date + timedelta(days=x)).strftime('%Y-%m-%d') 
                for x in range((end_date - start_date).days + 1)]

    # Create a PySpark DataFrame from the date list
    date_df = spark.createDataFrame([(d,) for d in date_list], ['date'])

    # Add month, year, and month_year columns with numeric month
    date_df = date_df.withColumn("date", col("date").cast(DateType())) \
        .withColumn("month", date_format(col("date"), "MM")) \
        .withColumn("year", date_format(col("date"), "yyyy"))

    return date_df





if __name__ == '__main__':

    #Creating variable data as spark dataframe for anrgument in tarnsform function
    path = '/opt/airflow/dags/'
    
    data1 = spark.read.csv(f'{path}reddit_opinion_PSE_ISR.csv', header=True, inferSchema=True)
    data2 = spark.read.csv(f'{path}assault.csv', header=True, inferSchema=True)

    # Download the required lexicon
    nltk.download('vader_lexicon')

    # Tambahkan hasil sentiment analysis ke data1
    data1 = add_sentiment_analysis(data1)

    # Tambahkan kolom month_year ke data2
    data2 = add_month_year_column(data2)

    date_df = add_date_table()

    fact_comment_columns = data1.select("comment_id", "self_text", "subreddit","created_time",
                                        "controversiality","score","author_name","post_id", "sentiment_score", "sentiment_label")
    fact_assault_columns = data2.select("country", "month_year", "events", "fatalities")
    dim_user_columns = data1.select("author_name", "user_is_verified", "user_account_created_time", "user_awardee_karma", 
                                    "user_awarder_karma", "user_link_karma", "user_comment_karma", "user_total_karma") #tolong benerin karena ini mengakibatkan table dim user tidak unik untuksetiap user
    dim_post_columns = data1.select("post_id", "post_score", "post_title", "post_self_text", "post_upvote_ratio", 
                                    "post_thumbs_ups", "post_total_awards_received", "post_created_time", "month_year") #idempoten with dim user problem
    dim_date_columns = date_df.select("date", "month", "year")
    # dim_date_with_month_year = fact_assault_columns.crossjoin(fact_assault_columns.select("month_year").distinct())

    # Select columns for each new DataFrame
    fact_comment_table = data1.select(*fact_comment_columns)
    fact_assault_table = data2.select(*fact_assault_columns)
    dim_user_table = data1.select(*dim_user_columns)
    dim_post_table = data1.select(*dim_post_columns)
    dim_date_table = date_df.select(*dim_date_columns)
    # fact_comment_table
    # fact_assault_table
    # dim_user_table
    # dim_post_table
    # dim_date_table