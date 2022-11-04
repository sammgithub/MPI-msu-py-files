import os
import json
import time
import boto3
from zlib import crc32
from threading import Thread


self_node = "node2"
self_name = "mike"
nodes=["node1","node2","node3","node4"]
node_names = {"seraj":"node1", "mike":"node2", "tom":"node3", "jerry":"node4"}

log_file = "%s.txt" % self_name

region={
	"node1":'us-east-1',
	"node2":'us-east-2',
	"node3":'us-west-1',
	"node4":'us-west-2'
}

queue_url = {
	"node1":'https://sqs.us-east-1.amazonaws.com/130379169672/nvg',
	"node2":'https://sqs.us-east-2.amazonaws.com/130379169672/oho',
	"node3":'https://sqs.us-west-1.amazonaws.com/130379169672/ncl',
	"node4":'https://sqs.us-west-2.amazonaws.com/130379169672/ore'
}

def send_message(message, recipient=None):
	global self_node, self_name, nodes, region, queue_url
	if recipient is None:
		for node in nodes:
			if node != self_node:
				sendMessage(message, node)
	else:
		# inject source name
		message["source"] = self_node
		message["source_name"] = self_name
		# Send message to SQS queue
		sqs = boto3.client('sqs', region_name=region[recipient])
		response = sqs.send_message(
			QueueUrl=queue_url[recipient],
			DelaySeconds=10,
			MessageAttributes={},
			MessageBody=(
				json.dumps(message)
			)
		)
	
def receive_message():
	global nodes, region, queue_url
	sqs = boto3.client('sqs', region_name=region[self_node])
	
	try:
		# Receive message to SQS queue
		response = sqs.receive_message(
			QueueUrl=queue_url[self_node],
			AttributeNames=[ 'SentTimestamp' ],
			MaxNumberOfMessages=1,
			MessageAttributeNames=[ 'All' ],
			VisibilityTimeout=0,
			WaitTimeSeconds=0
		)
	
		message = response['Messages'][0]
		receipt_handle = message['ReceiptHandle']
		# Delete received message from queue
		sqs.delete_message(
			QueueUrl=queue_url[self_node],
			ReceiptHandle=receipt_handle
		)
		return json.loads(message["Body"])
	except Exception as e:
		return None

class Log_Manager(Thread):
	def __init__(self):
		Thread.__init__(self)
		self.stop = False
		self.truncate_log = False
		self.cache = []
	
	def stop(self):
		self.stop = true
	
	def add(self, log):
		self.cache.append(log)
	
	def run(self):
		global log_file
		while(not self.stop):
			if len(self.cache) > 0:
				log = self.cache.pop(0)
				if log=="truncate":
					# delete everything from the log file
					file = open(log_file, "w")
					file.close()
				else:
					# append the log in the log file
					file = open(log_file, "a")
					file.write(log)
					file.close()
			time.sleep(1)

class Receiver(Thread):
	def __init__(self):
		Thread.__init__(self)
		self.stop = False
	
	def stop(self):
		self.stop = true
	
	def run(self):
		while(not self.stop):
			message = receive_message()
			if message == None:
				time.sleep(1)
				continue
			request = message["req"]
			global appointment_manager
			if request == "check time":
				appointment=json.loads(message["appointment"])
				appointment["source"]=message["source"]
				appointment_manager.check_time(appointment)
			elif request == "confirm time":
				appointment_manager.confirm_time(message)
			elif request == "add to calender":
				data = json.loads(message["appointment"])
				appointment = Appointment(data["title"], data["day"], data["start_time"], data["end_time"], data["participants"])
				appointment.data["id"] = data["id"]
				appointment_manager.appointments[data["id"]] = appointment
				print("event added to the calender successfully")
				print(appointment)
				log_manager.add(str(appointment))
			elif request == "delete appointment":
				del appointment_manager.appointments[message["appointment_id"]]
			else:
				print("system received message with unknown request")
				
			


class Appointment_Manager():
	def __init__(self):
		self.appointments = {}
	
	def add(self, appointment_id=None):
		if appointment_id == None:
			title = input("Title > ")
			participants = input("Participants > ").split(" ")
			participants.insert(0, self_name)
		while True:
			day = input("Day > ")
			start_time = input("Start_time > ")
			end_time = input("End_time > ")
			if self.time(end_time) - self.time(start_time) % 30!=0 and self.time(end_time) - self.time(start_time) < 30:
				print("meeting slot must be for 30 minutes")
				continue

			if self.check_time(Appointment("",day, start_time, end_time, ""), True):
				break
			print("self conflicting event")
		
		if appointment_id == None:
			appointment = Appointment(title, day, start_time, end_time, participants)
			appointment_id=appointment.get("id")
		else:
			appointment = self.appointments[appointment_id]
			appointment.day = day
			appointment.start_time = start_time
			appointment.end_time = end_time
		
		self.appointments[appointment_id] = appointment
		print("checking with participants")
		for participant in participants:
			if participant != self_name:
				send_message({"req":"check time", "appointment":appointment.json()}, node_names[participant])
	
	def time(self, time):
		hours, minutes = time.split(":")
		return (int(hours)*60)+int(minutes)
	
	def overlap(self, day1, start1, end1, day2, start2, end2):
		if day1 != day2:
			return False
		
		start1 = self.time(start1)
		end1 = self.time(end1)
		start2 = self.time(start2)
		end2 = self.time(end2)
		
		return	(start1 <= start2 and start2 < end1) or (start1 < end2 and end2 <= end1) or (start2 <= start1 and start1 < end2) or (start2 < end1 and end1 <= end2)
	
	def check_time(self, app, return_value=False):
		overlap = False
		for i in self.appointments.keys():
			old=self.appointments[i]
			if self.overlap(old.get("day"), old.get("start_time"), old.get("end_time"), app.get("day"), app.get("start_time"), app.get("end_time")):
				overlap = True
				break
		if return_value:
			return not overlap
		else:
			send_message({"req":"confirm time", "appointment_id":app["id"], "res":int(not overlap)}, app["source"])
	
	def confirm_time(self, message):
		appointment = self.appointments[message["appointment_id"]]
		if message["res"]==1: # 1 no overlapping
			appointment.data["attending"] += 1
			if appointment.get("attending") == len(appointment.data["participants"]):
				for participant in appointment.get("participants"):
					if participant != self_name:
						send_message({"req":"add to calender", "appointment":appointment.json()}, node_names[participant])
				print("event added to the calender successfully (%d)" % appointment.get("id"))
				log_manager.add(str(appointment))
				#os.system("python3 upstore.py")
				#og_manager.add("truncate")
		else:
			appointment.data["attending"] = 1
			print("%s can't participate" % message["source_name"])
			self.add(appointment.data["id"])
	
	def delete(self):
		global log_manager
		while True:
			uid = int(input("Appointment Id > "))
			if uid in self.appointments.keys():
				break
			print("appointment id not found")
		appointment = self.appointments[uid]
		for participant in appointment.get("participants"):
			if participant != self_name:
				send_message({"req":"delete appointment", "appointment_id":uid}, node_names[participant])
		del self.appointments[uid]
		log_manager.add("truncate")
		
class Appointment():
	def __init__(self, title, day, start_time, end_time, participants):
		self.data = {
			"id": crc32(str(time.time()).encode("utf-8")),
			"title": title,
			"day": day,
			"start_time": start_time,
			"end_time": end_time,
			"participants": participants,
			"attending": 1
		}
	
	def json(self):
		return json.dumps(self.data)
	
	def get(self, key):
		return self.data[key]
	
	def __str__(self):
		return "Event (%s):\n\tTitle: %s\n\tDay: %s\n\tStart Time: %s\n\tEnd Time: %s\n\tParticipant(s): %s\n" % (
			self.data["id"],
			self.data["title"],
			self.data["day"],
			self.data["start_time"],
			self.data["end_time"],
			", ".join(self.data["participants"])
		)

if __name__ == '__main__':
	log_manager = Log_Manager()
	log_manager.start()
	appointment_manager = Appointment_Manager()
	receiver = Receiver()
	receiver.start()
	
	while True:
		command = input("> ")
		if command== "add":
			appointment_manager.add()
		elif command=="del":
			appointment_manager.delete()
		elif command=="commit":
			os.system("python3 upstore.py %s.txt" % self_name)
			log_manager.add("truncate")
			
		# elif command=="print blockchain":
			
		# elif command=="try double spending block":
			
		elif command=="exit":
			break
		else:
			print("unknown command")

