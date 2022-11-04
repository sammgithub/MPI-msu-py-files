import time
import mysql.connector
import json
import boto3

receiver = 2

urlList=['https://sqs.us-east-2.amazonaws.com/130379169672/ds','https://sqs.ca-central-1.amazonaws.com/130379169672/Qca',
'https://sqs.eu-central-1.amazonaws.com/130379169672/Qfrnk','https://sqs.ap-southeast-1.amazonaws.com/130379169672/Qsng']



# s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# s.bind(("localhost", 59763))
# s.listen(1)

queue_url = urlList[receiver-1]


while True:
	time.sleep(2)
	# conn, addr = s.accept()
	
	# extract json data
	# data = conn.recv(1024)
	# data = json.loads(data.decode("utf-8")) 
	
	# print("data received")
	# print(data)
	# print("\n")
	
	sqs = boto3.client('sqs')

	try:
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
		
		message = response['Messages'][0]
		receipt_handle = message['ReceiptHandle']
		
		# Delete received message from queue
		sqs.delete_message(
			QueueUrl=queue_url,
			ReceiptHandle=receipt_handle
		)
		
		print('message received: %s' % message)
		
		connection = mysql.connector.connect(user='root', password='zxcvB!23', host='127.0.0.1', database='DS')
		
		# update appointment
		appointment = message["MessageAttributes"]["appointment"]["StringValue"]
		if appointment != "false":
			appointment = json.loads(appointment)
			cursor = connection.cursor()
			cursor.execute("INSERT INTO `appointment` (`id`,`title`,`day`,`start_time`,`end_time`,`participants`,`local_time`) VALUES ('%s','%s','%s','%s','%s','%s','%s');" % (appointment["appointment_id"], appointment["title"], appointment["day"], appointment["start_time"], appointment["end_time"], appointment["participants"], ""))
			connection.commit()
			cursor.close()
		
		# update log table
		table = json.loads(message["MessageAttributes"]["log_table"]["StringValue"])
		cursor_s = connection.cursor(buffered=True)
		# get latest mysql table
		cursor_s.execute("SELECT * FROM `log`;")

		for (node, n1, n2, n3, n4) in cursor_s:
			vals = [n1, n2, n3, n4]
			for i in range(4):
				# print(data.keys());print(node);
				if table[node]["n%i" % (i+1)] != vals[i]:
					try: 
						cursor_u = connection.cursor()
						cursor_u.execute("UPDATE `log` SET `n%i`='%i' WHERE `node`='%s';" % (i+1, max(table[node]["n%i" % (i+1)], vals[i]), node))
						connection.commit()
						cursor_u.close()
					except Exception as e:
						 print(e)
		cursor_s.close()
		connection.close()
		
	except Exception as e:
		print(e)
		
		
		