import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from timeit import default_timer as timer
from datetime import datetime
import logging
import boto3
from botocore.exceptions import ClientError
import json
import mysql.connector as mysql
from tabulate import tabulate

# logger config
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)s: %(message)s')

# # Create SQS client
AWS_REGION = "us-west-2"
sqs = boto3.client('sqs', region_name=AWS_REGION)
max_queue_messages = 10
'''
CREATE TABLE collected_review_data (
  reviewID int unsigned NOT NULL AUTO_INCREMENT,
  asin varchar(10) NOT NULL,
  name text NOT NULL,
  category varchar(150) NOT NULL,
  price float(4,2) NOT NULL,
  reviewDate date NOT NULL,
  rating float(2,1) NOT NULL,
  reviewText text NOT NULL,
  PRIMARY KEY (reviewID)
)
'''

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
])

def get_queue_url():
    # Get the queue. This returns an SQS.Queue instance
    queue_url = sqs.get_queue_url(QueueName='review-data-queue')
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

# {'asin': 'B07SJTHHRB', 
# 'name': 'JBL GO2 Ultra Portable Waterproof Wireless Bluetooth Speaker with up to 5 Hours of Battery Life - Blue', 
# 'category': 'Electronics', 
# 'price': 49, 
# 'timestamp': '2022-11-23 00:00:00', 
# 'rating': 5, 
# 'review_content': 'le son est bon, le blueooth fonctionne très bien, je vais en commander une deuxième !'}
def insert_into_db(db_cursor, review_data):
    asin = review_data.get('asin')
    name = review_data.get('name')
    category = review_data.get('category')
    price = review_data.get('price')
    #reviewDate = datetime.strptime(review_data.get('reviewDate'), '%Y-%m-%d')
    reviewDate = review_data.get('timestamp')[:10]
    #overall = float(review_data.get('overall'))
    rating = review_data.get('rating')
    reviewText = review_data.get('review_content')
    
    # INSERT INTO collected_review_data (asin, name, category, price, reviewDate, rating, reviewText) VALUES (673647, 'SomeProduct', 'Electronics', '65', '2022-11-23', 5.0, 'Good product')
    # insert each book as a row in MySQL
    insert_cmd = f'INSERT INTO {TABLE_NAME} (asin, name, category, price, reviewDate, rating, reviewText) VALUES (%s, %s, %s, %s, %s, %s, %s)'
    data = (asin, name, category, price, reviewDate, rating, reviewText)
    db_cursor.execute(insert_cmd, data)
    logger.info(f'[+] Inserted review data to DB for product: {name}(ASIN: {asin})')

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

def analyze_review_sentiment(msg):
    reviewdata_df = spark.read.json(msg, schema=reviewdata_schema)
    
    # use the model to make predictions
    predictions = model.transform(reviewdata_df)
    predictions.show()
    
    # evaluate the predictions
    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
            metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)
    logger.info(f'RMSE Evaluation value for sentiment prediction: {rmse}.')
    logger.info(f'Feature importance of current model: {model.stages[-1].featureImportances}')

def main():
    queue_url = get_queue_url()
    logger.info(f'Queue URL - {queue_url}')
    
    db_connection = get_db()
    db_cursor = get_db_cursor(db_connection)

    try:
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
                insert_into_db(db_cursor, review_data)
                db_connection.commit()
                logger.info('Deleting message from the queue...')
                #delete_queue_message(queue_url, receipt_handle)
            logger.info(f'Received and message(s) not deleted from {queue_url}.')
            messages = receive_queue_message(queue_url)
    except ClientError:
        logger.exception(
            f'Could not process more messages from the - {queue_url}.')
        raise

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
    main() #main(model)
    end = timer()
    print("Execution time: {}".format(end - start))