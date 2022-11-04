import boto3
import sys

def StoreLog(fileName):
        from botocore.client import Config
        ACCESS_KEY_ID = ' AKIAIEWHWCNEOTNQN5WQ'
        ACCESS_SECRET_KEY = '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'
        BUCKET_NAME = 'calendarlog'
     
        data = open(fileName, 'rb')

        s3 = boto3.resource(
            's3',
            aws_access_key_id=ACCESS_KEY_ID,
            aws_secret_access_key=ACCESS_SECRET_KEY,
            config=Config(signature_version='s3v4')
        )

        s3.Bucket(BUCKET_NAME).put_object(Key=fileName, Body=data)
        print ("Log uploaded to permanent storage")

StoreLog(sys.argv[1])