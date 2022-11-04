
import boto3
import mysql.connector
import socket
import json

receiver = 1

# Create SQS client
sqs = boto3.client('sqs',region_name=regionName)
regionName=['ap-southeast-1','us-east-2','ca-central-1','eu-central-1']
#queue_url = 'https://sqs.eu-central-1.amazonaws.com/130379169672/Qfrnk'

queue_url=['https://sqs.ap-southeast-1.amazonaws.com/130379169672/Qsng','https://sqs.us-east-2.amazonaws.com/130379169672/ds','https://sqs.ca-central-1.amazonaws.com/130379169672/Qca','https://sqs.eu-central-1.amazonaws.com/130379169672/Qfrnk']


# Receive message from SQS queue
response = sqs.receive_message(
    QueueUrl=queue_url,
    AttributeNames=[
        'SentTimestamp'
    ],
    MaxNumberOfMessages=1,
    MessageAttributeNames=[
        'All'
    ],
    VisibilityTimeout=0,
    WaitTimeSeconds=0

)

try:
        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']

        # Delete received message from queue
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        print('Received and deleted message: %s' % message)

        sender = message["MessageAttributes"]["node"]["StringValue"]
        print("Message received from: %s" % sender)

        connection = mysql.connector.connect(user='root', password='zxcvB!23', host='127.0.0.1', database='DS')
        cursor = connection.cursor()
        cursor.execute("UPDATE `log1` SET `n%i`=`n%i`+1 WHERE `node`='%s' LIMIT 1;" % (receiver, receiver, sender.upper()))

        cursor.execute("SELECT * FROM `log1`")
        table = {}
        for (node, n1, n2, n3, n4) in cursor:
                table[node] = {'n1': n1+1, 'n2': n2+2, 'n3': n3+4, 'n4': n4}
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("127.0.0.1", 59988))
        sock.send(json.dumps(table).encode('utf-8'))
        connection.commit()
        cursor.close()
        connection.close()
except Exception as e:
        print(e)
       # print("No message in the queue")