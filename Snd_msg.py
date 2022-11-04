import boto3
from zlib import crc32
from random import randint
import socket
import time
import mysql.connector
import json

sender = 1
urlList=['https://sqs.us-east-2.amazonaws.com/130379169672/ds','https://sqs.ca-central-1.amazonaws.com/130379169672/Qca',
'https://sqs.eu-central-1.amazonaws.com/130379169672/Qfrnk','https://sqs.ap-southeast-1.amazonaws.com/130379169672/Qsng']
server_ips = ["", "35.182.254.52", "", ""]
regionName=['us-east-2','ca-central-1','eu-central-1','ap-southeast-1']

connection = mysql.connector.connect(user='root', password='zxcvB!23', host='127.0.0.1', database='DS')


def sendMessage(region, url, action, appointment, table, receiver):
	# Create SQS client
	
	sqs = boto3.client('sqs', region_name=region)
		
	
	# Send message to SQS queue
	response = sqs.send_message(
		QueueUrl=url,
		DelaySeconds=10,
		MessageAttributes={
			# 'appointment_id': {
			# 	'DataType': 'String',
			# 	'StringValue': appointment.appointment_id
			# },
			# 'title': {
			# 	'DataType': 'String',
			# 	'StringValue': appointment.title
			# },
			# 'day': {
			# 	'DataType': 'String',
			# 	'StringValue': appointment.day
			# },
			# 'start_time': {
			# 	'DataType': 'String',
			# 	'StringValue': appointment.start_time
			# },
			# 'end_time': {
			# 	'DataType': 'String',
			# 	'StringValue': appointment.end_time
			# },
			# 'participants': {
			# 	'DataType': 'String',
			# 	'StringValue': appointment.participants
			# },
			# 'timestamp': {
			# 	'DataType': 'String',
			# 	'StringValue': "time"
			# }
			'appointment': {
				'DataType': 'String',
				'StringValue': json.dumps(appointment.json()) if str(receiver) in appointment.participants else "false"
			},
			'log_table': {
				'DataType': 'String',
				'StringValue': json.dumps(table)
			},
		},
		MessageBody=(
			'body'
		)
	)

	# send json table
	# sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	# sock.connect((server_ips[1], 59763))
	# sock.send(json.dumps(table).encode('utf-8'))

class Appointment():
	def __init__(self, appointment_id, title, day, start_time, end_time, participants):
		self.appointment_id = appointment_id
		self.title = title
		self.day = day
		self.start_time = start_time
		self.end_time = end_time
		self.participants = participants.split(" ")
	
	def json(self):
		json = {}
		json["appointment_id"] = self.appointment_id
		json["title"] = self.title
		json["day"] = self.day
		json["start_time"] = self.start_time
		json["end_time"] = self.end_time
		json["participants"] = " ".join(self.participants)
		return json
	
	def __str__(self):
		return "Id: %s\nTitle: %s\nDay: %s\nStart Time: %s\nEnd Time: %s\nParticipant(s): %s\n" % (
			self.appointment_id,
			self.title,
			self.day,
			self.start_time,
			self.end_time,
			" ".join(self.participants)
		)
		

while(1):
	print("Please choose any: 1. Appointment 2. Delete 3. Print 4. Exit")
	action = int(input())
	
	if(action==1):
		'''
		a=[]
		print("Take Appointment Name: ")
		name=input()
		'''
		
		# title=input("Take Appointment Name: ")
		# print("Take Appointment Date: ")
		# date=input()
		# print("Take Appointment Participant: ")
		# participants=input()
		# print("Take Appointment Start Time: ")
		# start_time=input()
		# print("Take Appointment End time: ")
		# end_time=input()
		
		title = input("Appointment title: ")
		day = input("Day: ")
		start_time = input("Start Time (HH:MM): ")
		end_time = input("End Time (HH:MM): ")
		participants = input("Participant(s): ")
		
		# generate an appointment id using CRC32
		appointment_id = hex(crc32(("%s %s %s %s" % (day, start_time, end_time, randint(111, 999))).encode('UTF-8'))).upper()[2:]
		appointment = Appointment(appointment_id, title, day, start_time, end_time, participants)
		
		# update mysql table for new appointment
		cursor = connection.cursor()
		cursor.execute("UPDATE `log` SET `n%i`=`n%i`+1 WHERE `node`='N%i' LIMIT 1;" % (sender, sender, sender))
		connection.commit()
		
		# get the latest mysql table
		cursor.execute("SELECT * FROM `log`")
		# build json from mysql
		table = {}
		for (node, n1, n2, n3, n4) in cursor:
			table[node] = {'n1': n1, 'n2': n2, 'n3': n3, 'n4': n4}
		
		for receiver in range(1,5):
			if receiver == sender:
				continue
			sendMessage(
				region = regionName[receiver-1],
				url = urlList[receiver-1],
				action = "Insert",
				appointment = appointment,
				table = table,
				receiver = receiver
			)
	elif(action==2):
		#delete
		a=[]
	elif(action==3):
		a=[]
	else:
		break

cursor.close()
connection.close()
