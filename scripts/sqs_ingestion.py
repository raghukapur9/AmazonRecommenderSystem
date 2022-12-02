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
    messages = receive_queue_message(queue_url)
    print(messages)
    '''
    {'Messages': [{'MessageId': '71eec6f2-d2e9-42c5-9c5b-0a38ae82a9d1', 'ReceiptHandle': 'AQEB/b/', 'MD5OfBody': '2079d7bcda3672e0f0951bcf774c0254', 'Body': 'Learn how to create, receive, delete and modify SQS queues and see the other functions available within the AWS.'}], 'ResponseMetadata': {'RequestId': '80bd5718-41a4-5a7f-9ac1-0b1a3f8109ed', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amzn-requestid': '80bd5718-41a4-5a7f-9ac1-0b1a3f8109ed', 'date': 'Thu, 01 Dec 2022 10:03:17 GMT', 'content-type': 'text/xml', 'content-length': '963'}, 'RetryAttempts': 0}}
    '''
    for msg in messages['Messages']:
        msg_body = msg['Body']
        receipt_handle = msg['ReceiptHandle']
        logger.info(f'The message body: {msg_body}')
        #logger.info('Deleting message from the queue...')
        #delete_queue_message(queue_url, receipt_handle)
    logger.info(f'Received and message(s) not deleted from {queue_url}.')

if __name__=="__main__":
    start = timer()
    main()
    end = timer()
    print("Execution time: {}".format(end - start))