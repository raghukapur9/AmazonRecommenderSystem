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
# sqs = boto3.client("sqs", region_name=AWS_REGION)

AWS_REGION = "us-west-2"
# Get the service resource
sqs = boto3.resource('sqs', region_name=AWS_REGION)

def get_queue():
    # Get the queue. This returns an SQS.Queue instance
    queue = sqs.get_queue_by_name(QueueName='review-data-queue')
    return queue

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

def send_queue_message(queue, msg_body):
    """
    Sends a message to the specified queue.
    """
    try:
        response = queue.send_message(QueueUrl=queue.url,
                                           MessageBody=msg_body)
    except ClientError:
        logger.exception(f'Could not send meessage to the - {queue.url}.')
        raise
    else:
        return response

def main():
    queue = get_queue()
    queue_url = queue.url
    logger.info(f'Queue URL - {queue_url}')
    #print(queue.attributes.get('DelaySeconds'))
    
    MSG_ATTRIBUTES = {
        'EntryName': {
            'DataType': 'String',
            'StringValue': 'Some speaker data'
        }
    }
    MSG_BODY_1 = {
        'asin': '5120053067', 
        'name': 'Anker Blue 4', 
        'category': 'Electronics', 
        'reviewDate': '2022-11-25', 
        'overall': '5', 
        'reviewText': 'Great product in affordable price range.',
    }
    MSG_BODY_2 = {
        'asin': '5120053084', 
        'name': 'JBL Flip 6', 
        'category': 'Electronics', 
        'reviewDate': '2022-11-26', 
        'overall': '4.5', 
        'reviewText': 'Good speaker having a long battery life! I enjoy using the product.',
    }

    msg_body_list = [MSG_BODY_1, MSG_BODY_2]
    for msg_body in msg_body_list:
        msg = send_queue_message(queue, json.dumps(msg_body))
        json_msg = json.dumps(msg, indent=4)

        logger.info(f'''
            Message sent to the queue {queue_url}.
            Message attributes: \n{json_msg}''')


if __name__ == '__main__':
    main()