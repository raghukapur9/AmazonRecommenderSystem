import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import expr, col, length
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from timeit import default_timer as timer
from datetime import datetime, timedelta
import logging
import boto3
from botocore.exceptions import ClientError
import json
import mysql.connector as mysql
from tabulate import tabulate

import string
import random

# logger config
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)s: %(message)s')

# # Create SQS client
AWS_REGION = 'us-west-2'
sqs = boto3.client('sqs', region_name=AWS_REGION)
max_queue_messages = 10
QUEUE_NAME = 'review-data-queue'

# insert MySQL Database information here
HOST = 'localhost'
DATABASE = 'amazon_reviews'
USER = 'ars_user'
PASSWORD = '****'
TABLE_NAME = 'collected_review_data'

# INSERT INTO collected_review_data (asin, name, category, price, reviewDate, rating, reviewText) VALUES (673647, 'SomeProduct', 'Electronics', 65, '2022-11-23', 5.0, 'Good product')
reviewdata_schema = types.StructType([
    types.StructField('asin', types.LongType ()),
    types.StructField('name', types.StringType()),
    types.StructField('category', types.StringType()),
    types.StructField('price', types.FloatType()),
    types.StructField('reviewDate', types.DateType()),
    types.StructField('rating', types.FloatType()),
    types.StructField('reviewText', types.StringType()),
    types.StructField('sentiment', types.StringType())
])

review_analysis_schema = types.StructType([
        types.StructField('overall', types.FloatType()),
        types.StructField('reviewText', types.StringType()),
        types.StructField('summary', types.StringType())
    ])

def gen_random_string(): 
    # initializing size of string
    N = 7
    # generating random strings using random.choices()
    res = ''.join(random.choices(string.ascii_uppercase +
                                string.digits, k=N))
    return res

def get_queue_url():
    # Get the queue. This returns an SQS.Queue instance
    queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)
    return queue_url['QueueUrl']

def get_db():
    # connect to the database
    db_connection = mysql.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD)
    # get server information
    logger.info(f'MySQL DB Info: {db_connection.get_server_info()}')
    return db_connection

def get_db_cursor(db_connection):
    # get the db cursor
    cursor = db_connection.cursor()
    # get database information
    cursor.execute("SELECT DATABASE()")
    database_name = cursor.fetchone()
    logger.info(f'[+] Database connection established with database {database_name}')
    return cursor

def change_review_date(date, delta_days):
    date_formatted = datetime.strptime(date, '%Y-%m-%d')
    #print('Date formatted: ', date_formatted)
    day_minus_3 = date_formatted - timedelta(days=delta_days)
    #print('Date formatted - 3: ', str(day_minus_3)[:10])
    return str(day_minus_3)

# {'asin': 'B07SJTHHRB', 
# 'name': 'JBL GO2 Ultra Portable Waterproof Wireless Bluetooth Speaker with up to 5 Hours of Battery Life - Blue', 
# 'category': 'Electronics', 
# 'price': 49, 
# 'timestamp': '2022-11-23 00:00:00', 
# 'rating': 5, 
# 'review_content': 'le son est bon, le blueooth fonctionne très bien, je vais en commander une deuxième !'}
def insert_into_db(db_connection, db_cursor, review_data_list, predicted_sentiment_list):
    """
    Iterates over review-data and loads data into DB.

    DB Created with following schema:
    CREATE TABLE collected_review_data (
    reviewID int unsigned NOT NULL AUTO_INCREMENT,
    asin varchar(10) NOT NULL,
    name text NOT NULL,
    category varchar(150) NOT NULL,
    price float(10,2) NOT NULL,
    reviewDate date NOT NULL,
    rating float(2,1) NOT NULL,
    reviewText text NOT NULL,
    sentiment text,
    PRIMARY KEY (reviewID)
    )
    """
    try:
        for i in range(len(review_data_list)):
            uid = review_data_list[i].get('uid')
            asin = review_data_list[i].get('asin')
            name = review_data_list[i].get('name')
            category = review_data_list[i].get('category')
            price = review_data_list[i].get('price')
            #reviewDate = datetime.strptime(review_data.get('reviewDate'), '%Y-%m-%d')
            reviewDate = review_data_list[i].get('timestamp')[:10]
            #reviewDate = change_review_date(reviewDate, 3)[:10]
            #overall = float(review_data.get('overall'))
            rating = review_data_list[i].get('rating')
            reviewText = review_data_list[i].get('review_content')
            sentiment = predicted_sentiment_list[uid]

            # insert each book as a row in MySQL
            insert_cmd = f'INSERT INTO {TABLE_NAME} (asin, name, category, price, reviewDate, rating, reviewText, sentiment) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
            data = (asin, name, category, price, reviewDate, rating, reviewText, sentiment)
            db_cursor.execute(insert_cmd, data)
            db_connection.commit()
            logger.info(f'[+] Inserted review data to DB for product: {name}(ASIN: {asin})')
        return True
    except ClientError:
        logger.exception(
            f'Insertion of data to DB failed.')
        raise
    else:
        return False

def get_db_data(db_cursor):
    # fetch the database
    insert_cmd = f'SELECT * FROM {TABLE_NAME}'
    db_cursor.execute(insert_cmd)
    # get all selected rows
    rows = db_cursor.fetchall()
    # print all rows in a tabular format
    print(tabulate(rows, headers=db_cursor.column_names))

def list_queues():
    """
    Creates an iterable of all Queue resources in the collection.
    """
    try:
        sqs_queues = []
        for queue in sqs.queues.all():
            sqs_queues.append(queue)
    except ClientError:
        logger.exception('Could not list queues.')
        raise
    else:
        return sqs_queues

def receive_queue_message(queue_url):
    """
    Retrieves one or more messages (up to 10), from the specified queue.
    """
    try:
        response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=max_queue_messages, WaitTimeSeconds=10)
    except ClientError:
        logger.exception(
            f'Could not receive the message from the - {queue_url}.')
        raise
    else:
        return response

def delete_queue_message(queue_url, receipt_handle):
    """
    Deletes the specified message from the specified queue.
    """
    try:
        response = sqs.delete_message(QueueUrl=queue_url,
                                             ReceiptHandle=receipt_handle)
    except ClientError:
        logger.exception(
            f'Could not delete the meessage from the - {queue_url}.')
        raise
    else:
        return response

def create_analysis_df(reviewdata_list):
    """
    Create dataframe with review data
    """
    reviewdata_analysis_list = []
    for review_data in reviewdata_list:
        reviewdata_analysis_list.append((float(review_data.get('rating')), review_data.get('review_content'), review_data.get('uid')))
    
    reviewdata_analysis_df = spark.createDataFrame(reviewdata_analysis_list, review_analysis_schema)
    return reviewdata_analysis_df

def remove_non_ascii_chars(review):
    return review.encode('ascii', 'ignore').decode('ascii')

@functions.udf(returnType=types.StringType())
def preprocess_reviews(review):
    non_ascii_reviews = remove_non_ascii_chars(review)
    return non_ascii_reviews

def prediction_value_to_sentiment(prediction_val):
    if(prediction_val==0.0):
        return "Positive"
    elif(prediction_val==1.0):
        return "Neutral"
    elif(prediction_val==2.0):
        return "Negative"
    else:
        return "Negative"

def analyze_review_sentiment(model, reviewdata_list):
    ### Read inputs using command line arguments and create a df with the defined schema
    reviewdata_df = create_analysis_df(reviewdata_list)
    
    reviewdata_df = reviewdata_df.filter(reviewdata_df.reviewText.isNotNull())
    reviewdata_df = reviewdata_df.withColumn("reviewText", functions.lower(functions.col("reviewText")))

    reviewdata_df = reviewdata_df.withColumn(
        "processedReviewText",
        preprocess_reviews(reviewdata_df.reviewText)
    )

    reviewdata_df_with_sentiment = reviewdata_df.select(col('*'), expr("CASE WHEN overall > 4 THEN 'Positive' " + 
        "WHEN overall >3.0 AND overall <=4 THEN 'Neutral' WHEN overall <=3.0 THEN 'Negative'" +
        "ELSE overall END").alias("sentiment")
    )

    reviewdata_df_with_sentiment = reviewdata_df_with_sentiment.withColumn("length", length(reviewdata_df_with_sentiment.processedReviewText))

    # use the model to make predictions
    predictions = model.transform(reviewdata_df_with_sentiment)
    predicted_sentiment_list = {}
    for prediction in predictions.collect():
        predicted_review_uid = prediction[2]
        predicted_reviewdata = prediction[-1]
        sentiment = prediction_value_to_sentiment(predicted_reviewdata)
        logger.info(f'Predicted sentiment for the review is: {sentiment}')
        predicted_sentiment_list[predicted_review_uid] = sentiment
    return predicted_sentiment_list

def main(model):
    queue_url = get_queue_url()
    logger.info(f'Queue URL - {queue_url}')
    
    db_connection = get_db()
    db_cursor = get_db_cursor(db_connection)

    try:
        reviewdata_list = []
        receipt_handle_list = []
        messages = receive_queue_message(queue_url)
        while True:
            if "Messages" not in messages:
                break
            for msg in messages['Messages']:
                msg_body = msg['Body']
                review_data = json.loads(msg_body)
                receipt_handle = msg['ReceiptHandle']
                logger.info(f'Review data: {review_data}')
                logger.info(f'Receipt handle: {receipt_handle}')
                uid = gen_random_string()
                logger.info(f'Generated UID: {str(uid)}')
                review_data['uid'] = uid
                reviewdata_list.append(review_data)
                receipt_handle_list.append(receipt_handle)
            logger.info(f'Received message(s) from {queue_url}.')
            messages = receive_queue_message(queue_url)
    except ClientError:
        logger.exception(
            f'Could not process more messages from the - {queue_url}.')
        raise

    predicted_sentiment_list = analyze_review_sentiment(model, reviewdata_list)
    insert_into_db(db_connection, db_cursor, reviewdata_list, predicted_sentiment_list)
    for receipt_handle in receipt_handle_list:
        logger.info('Deleting message(s) from the queue...')
        delete_queue_message(queue_url, receipt_handle)
    logger.info('Message(s) deleted from queue')
    get_db_data(db_cursor)
    
    # close the cursor
    db_cursor.close()
    # close the DB connection
    db_connection.close()

if __name__=="__main__":
    model_file = sys.argv[1]
    
    spark = SparkSession.builder.appName('sentiment analyzer').getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    assert spark.version >= '3.0'
    sc = spark.sparkContext

    # load the model
    model = PipelineModel.load(model_file)

    start = timer()
    main(model)
    end = timer()
    print("Execution time: {}".format(end - start))