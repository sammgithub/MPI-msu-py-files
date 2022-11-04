import boto3
def sendMessage(region, url, matrix, op, time, node):
    # Create SQS client    
    sqs = boto3.client('sqs',region_name=region)
    response = sqs.send_message(
        QueueUrl=url,
        DelaySeconds=10,
        MessageAttributes={
            'operation': {
                'DataType': 'String',
                'StringValue': op
            },
            'Matrix': {
                'DataType': 'String',
                'StringValue': str(matrix)
            },
            'time': {
                'DataType': 'Number',
                'StringValue': str(time)
            },
            'node': {
                    'DataType': 'String',
                    'StringValue': str(node)
            }
        },
        MessageBody=(
            'Information about current NY Times'
        )
    )
#main
urlList=['https://sqs.ap-southeast-1.amazonaws.com/130379169672/Qsng','https://sqs.us-east-2.amazonaws.com/130379169672/ds','https://sqs.ca-central-1.amazonaws.com/130379169672/Qca','https://sqs.eu-central-1.amazonaws.com/130379169672/Qfrnk']
regionName=['ap-southeast-1','us-east-2','ca-central-1','eu-central-1']

node=0
matrix=[[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0]]
time=0
while(1):
    print("1. Appointment 2. Delete 3. Print 4. Exit")
    schedule=input()
    if(schedule==1):
        print("Appointment Date: ")
        date=input()
        print("Appointment Participants: ")
        participant=input()
        print("Appointment Start Time: ")
        start_time=input()
        print("Appointment End time: ")
        end_time=input()
        sendMessage(regionName[int(participant)],urlList[int(participant)],matrix,"Insert",1,node)
        time+=1        
    elif(schedule==2):
        a=[]
    elif(schedule==3):
        a=[]
    else:
        break

