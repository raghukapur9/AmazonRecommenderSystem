try:
    import boto3
    import os
    import sys
    import json
except Exception as e:
    print(e)
 
#Add AWS Creds here
AWS_ACCESS_KEY=""
AWS_SECRET_KEY=""
AWS_SQS_QUEUE_NAME = ""

class SQSQueue(object):
 
    def __init__(self, queueName=None):
        self.resource = boto3.resource('sqs', region_name='us-west-2',
                                       aws_access_key_id=AWS_ACCESS_KEY,
                                       aws_secret_access_key=AWS_SECRET_KEY)
        self.queue = self.resource.get_queue_by_name(QueueName=AWS_SQS_QUEUE_NAME)
        self.QueueName = queueName
 
    def send(self, Message={}):
        data = json.dumps(Message)
        response = self.queue.send_message(MessageBody=data)
        return response
 
    def receive(self):
        try:
            queue = self.resource.get_queue_by_name(QueueName=self.QueueName)
            for message in queue.receive_messages():
                data = message.body
                data = json.loads(data)
                message.delete()
        except Exception:
            print(e)
            return []
        return data