import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

#from pyspark.sql import SparkSession, functions
#from pyspark.ml import PipelineModel
from timeit import default_timer as timer
import logging
import boto3
from botocore.exceptions import ClientError
import json

# logger config
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)s: %(message)s')

# # Create SQS client
AWS_REGION = "us-west-2"
sqs = boto3.client('sqs', region_name=AWS_REGION)

def get_queue_url():
    # Get the queue. This returns an SQS.Queue instance
    queue_url = sqs.get_queue_url(QueueName='review-data-queue')
    return queue_url['QueueUrl']

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
        response = sqs.receive_message(QueueUrl=queue_url)
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


def main():
    queue_url = get_queue_url()
    logger.info(f'Queue URL - {queue_url}')
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
                logger.info('Deleting message from the queue...')
                #delete_queue_message(queue_url, receipt_handle)
            logger.info(f'Received and message(s) deleted from {queue_url}.')
            messages = receive_queue_message(queue_url)
    except ClientError:
        logger.exception(
            f'Could not process more messages from the - {queue_url}.')
        raise

if __name__=="__main__":
    start = timer()
    main()
    end = timer()
    print("Execution time: {}".format(end - start))