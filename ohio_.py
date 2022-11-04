from hashlib import sha256
import threading
import json
import time
import boto3


blockGenProbability=0.5
ledger=100
hash_head = "0000"

verifying_reward_percentage = 0.05
signing_reward_percentage = 0.10

miner="m2"
verifier = "m4"

nodes=["m1","m2","m3","m4"]
#nodes=["m1","m4"]

region={"m1":'us-east-1',
		"m2":'us-east-2',
		"m3":'us-west-1',
		"m4":'us-west-2'}

queue_url = {
	"m1":'https://sqs.us-east-1.amazonaws.com/130379169672/nvg',
	"m2":'https://sqs.us-east-2.amazonaws.com/130379169672/oho',
	"m3":'https://sqs.us-west-1.amazonaws.com/130379169672/ncl',
	"m4":'https://sqs.us-west-2.amazonaws.com/130379169672/ore'}

def sendMessage(message, recipient=None):
	#return
	global nodes, region, queue_url


	if recipient is None:
		for each in nodes:
			if each !=miner:
				sendMessage(message, each)
		return
	# Send message to SQS queue
	sqs = boto3.client('sqs',region_name=region[recipient])
	response = sqs.send_message(
		QueueUrl=queue_url[recipient],
		DelaySeconds=10,
		MessageAttributes={
			},
		MessageBody=(
			"%s" % (message)
		)
	)    
	#print(response['MessageId'])
	#print("SENT TO %s: %s %s %s" % (recipient, miner, message,log[-1]))
	
def rcvMessage():
	global nodes, region, queue_url
	#print("Entered RCVMessage")
	sqs = boto3.client('sqs',region_name=region[miner])
	# Receive message to SQS queue
	response = sqs.receive_message(
		QueueUrl=queue_url[miner],
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
	#print ("Response %s" % response)
	try:
		message = response['Messages'][0]
		#print (message)
		receipt_handle = message['ReceiptHandle']
		# Delete received message from queue
		sqs.delete_message(
			QueueUrl=queue_url[miner],
			ReceiptHandle=receipt_handle
			)
		# print('Received and deleted message: %s' % message)
		#print(message["Body"])
		return message["Body"]
	except Exception as e:
		#print ("returning none")
		return None

class Consensus(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.kill=False

	def run(self):
		global miner, blockChain

		while True:
			if self.kill:
				break
			msg=rcvMessage()
			if msg is None:
				time.sleep(1)
			else:
				msg=json.loads(msg)
				if msg["req"] == "res":
					print(msg["message"])
				elif msg["req"]=="verify this block":
					block = Block(msg)
					
					# calculate total spend of the block
					transactions_total_amount = 0
					for transaction in block.transactions.transact:
						transactions_total_amount += transaction["amount"] * (1 if transaction["frm"]==block.owner else -1)
					
					validity=True
					error=""
					
					# check if spends exceeds available balance
					if transactions_total_amount > msg["ledger"]:
						# check double spending and
						# if there is no double spending and a supporter is available, inform miner
						supportingAmount = transactions_total_amount - msg["ledger"]
						supporter=verifier_c.proofOfStake(miner=block.owner, supportingAmount=supportingAmount)
						if supporter is not None:
							sendMessage(json.dumps({"req":"supporter", "supporterName": supporter, "amount":supportingAmount}))
						else:
							validity=False
							error="amount exceeds the actual balance"
					
					# prev hash is equal to final hash of previous block
					if block.prev_hash != blockChain.chain[-1].final_hash:
						doubleSpending = False
						for _block in blockChain.chain:
							if _block.final_hash == block.prev_hash:
								doubleSpending = True
						validity=False
						if doubleSpending:
							error="No double spending is allowed"
						else:
							error="previous hash does not match"
					
					if validity:
						message = {"req":"add to chain", "block.owner": block.owner, "block.prev_hash": block.prev_hash, "block.nonce": block.nonce, "block.transactions": block.transactions.asJson(), "block.final_hash": block.final_hash, "newBalance": msg["ledger"]-transactions_total_amount}
						sendMessage(json.dumps(message))
						sendMessage(json.dumps(message), miner)
						for node in nodes:
							if block.owner != node:
								sendMessage(json.dumps({"req":"reward", "type":"verifying", "amount":transactions_total_amount*verifying_reward_percentage}), node)
					else:
						sendMessage(json.dumps({"req":"res", "message": error}), block.owner)
				elif msg["req"]=="supporter":
					print("%s can support this transaction" % msg["supporterName"])
					sendMessage(json.dumps({"req":"reward", "type":"signing", "amount":msg["amount"]*signing_reward_percentage}), msg["supporterName"])
				elif msg["req"]=="supporter exceeds p":
					print("%s could support this transaction but exceeds probability" % msg["supporterName"])
				elif msg["req"]=="reward":
					print("%s reward received = $%f" % (msg["type"], msg["amount"]))
				elif msg["req"]=="add to chain":
					block = Block(msg)
					verifier_c.ledger[block.owner] = msg["newBalance"]
					block.commit()

				else:
					print("unknown request")

class BlockChain():
	def __init__(self):
		self.chain=[]
		self.chainOwnerStat={}

	def addBlock(self, block):
		self.chain.append(block)
		if block.owner not in self.chainOwnerStat.keys():
			self.chainOwnerStat[block.owner]=0
		self.chainOwnerStat[block.owner]+=1

class Block():
	"""docstring for genesis"""
	def __init__(self, json_data=None, owner=None):
		global blockChain
		if json_data is not None:
			# json_data = json.loads(json_data)
			self.owner = json_data["block.owner"]
			self.prev_hash = json_data["block.prev_hash"]
			self.nonce = json_data["block.nonce"]
			self.transactions = Transactions()
			transactions = json.loads(json_data["block.transactions"])
			for transaction in transactions:
				self.transactions.insert(transaction["frm"], transaction["to"], transaction["amount"])
			self.final_hash = json_data["block.final_hash"]
		else:
			global miner
			self.owner=miner if owner==None else owner
			self.prev_hash = blockChain.chain[-1].final_hash if len(blockChain.chain)>0 else 0
			self.nonce = 0
			self.transactions = Transactions()
			self.final_hash = 0
		
	def generateHash(self):
		message=str(self.transactions)
		while True:
			hash_var=sha256(("%s %d %s" % (message, self.nonce, self.prev_hash)).encode("utf-8")).hexdigest()
			if hash_var[0:len(hash_head)]==hash_head:
				break

			self.nonce+=1
			#print(hash_var)
		#print(self.nonce)

		self.final_hash=hash_var

	def __str__(self):
		return "Block begin\n\tTransactions: %s\n\tPrevious Hash: %s\n\tNonce: %d\n\tFinal hash: %s\nBlock end" %(str(self.transactions), self.prev_hash, self.nonce, self.final_hash)

	def commit(self):
		global blockChain
		blockChain.addBlock(self)
		print("### Block Added to Chain $$$$")
		print(self)


class Transactions():
	def __init__(self):
		self.transact = []

	def insert(self, frm, to, amount):
		global ledger
		self.transact.append({"frm":frm, "to":to,"amount":amount})

		# if miner==frm:
		# 	ledger=ledger-amount
		# else:
		# 	ledger=ledger+amount
	
	def asJson(self):
		# output = []
		# for transaction in self.transact:
		# 	output.append(json.dumps(transaction))
		return json.dumps(self.transact)
	
	def __str__(self):
		# output=""
		# for transaction in self.transact:
		# 	output+="from %s to %s $%d|" %(transaction["frm"], transaction["to"], transaction["amount"])
		output = []
		for transaction in self.transact:
			output.append("from %s to %s $%d" %(transaction["frm"], transaction["to"], transaction["amount"]))
		return ", ".join(output)

class Verifier():
	def __init__(self):
		self.ledger={"m1":100, "m2":100,"m3":50, "m4":50}
		self.reservedAmount={}

	def doubleSpending(self, miner, amount):
		global blockChain
		if self.ledger[miner]>=self.reservedAmount[miner]+amount:
			self.reservedAmount[miner]=amount
			return True
		elif self.reservedAmount[miner] == 0 and self.ledger[miner] > 0:
			supportingAmount = amount - self.ledger[miner]
			for supporter in self.ledger.keys():
				if supporter == miner:
					continue
				
				if self.ledger[supporter] - self.reservedAmount[supporter] >= supportingAmount and (0 if supporter not in blockChain.chainOwnerStat.keys() else blockChain.chainOwnerStat[supporter])/len(blockChain.chain) <= blockGenProbability:
					# return supporter
					sendMessage(json.dumps({"req":"supporter", "supporterName": supporter}))
			return False
		else:
			return "Error: Double spending"
			#return False
	
	def proofOfStake(self, miner, supportingAmount):
		for supporter in self.ledger.keys():
			if supporter != miner and self.ledger[supporter] >= supportingAmount:
				if miner in blockChain.chainOwnerStat.keys() and (blockChain.chainOwnerStat[supporter]+1)/len(blockChain.chain) > blockGenProbability:
					sendMessage(json.dumps({"req":"supporter exceeds p", "supporterName": supporter}))
				else:
					return supporter
		return None

	def prob_p_check(self, miner, amount):
		global blockChain
		if miner not in self.ledger.keys():
			self.ledger[miner]=100
			self.reservedAmount[miner]=0
		#Cehck participation rate
		if not (0 if miner not in blockChain.chainOwnerStat.keys() else blockChain.chainOwnerStat[miner])/len(blockChain.chain) <= blockGenProbability:
			return False
		#Check double spending
		return doubleSpending(miner, amount)


if __name__ == '__main__':
	verifier_c=Verifier()
	consensus = Consensus()
	consensus.start()

	blockChain=BlockChain()
	## generate genesis block
	newBlock = Block(owner="")
	newBlock.generateHash()
	newBlock.commit()
	print("Block Chian initialized with Genesis block.")
	#print(newBlock)

	while True:
		command = input("> ")
		if command== "generate block":
			if miner not in blockChain.chainOwnerStat.keys() or (blockChain.chainOwnerStat[miner]+1)/(len(blockChain.chain)) <= blockGenProbability:
				newBlock=Block()
				print("Generating new block within probability range. Please add your transactions:")
			else:
				print("Generating new block exceeds probability")
			
		elif command=="add transaction":
			frm = input("From: ")
			to = input("To: ")
			amount = int(input("Amount: "))
			frm = miner if frm == "self" else frm
			to = miner if to == "self" else to
			newBlock.transactions.insert(frm, to, amount)
		elif command=="add to chain":
			newBlock.generateHash()
			block_miner_msg = {"req":"verify this block", "block.owner": newBlock.owner, "block.prev_hash": newBlock.prev_hash, "block.nonce": newBlock.nonce, "block.transactions": newBlock.transactions.asJson(), "block.final_hash": newBlock.final_hash, "ledger":ledger}
			sendMessage(json.dumps(block_miner_msg), verifier)
			print("New block has been sent to other verifiers.")
			print(newBlock)
			# newBlock.commit()
			# print("block added to chain")
			# print(newBlock)
		elif command == "print block":
			print(newBlock)
		elif command == "print blockchain":
			# chain = []
			# for block in blockChain.chain:
			# 	chain.append(block.owner)
			# print(" | ".join(chain))
			print("Blockchain Begins:")
			for block in blockChain.chain:
				print("\t<%s %s>" % (block.owner, block.final_hash))
			print("Blockchain Ends:")
		elif command == "try double spending block":
			doubleSpendingBlock = Block()
			doubleSpendingBlock.prev_hash = blockChain.chain[-2].final_hash
			doubleSpendingBlock.transactions.insert("self", "mike", 10)
			print(doubleSpendingBlock)
			doubleSpendingBlock.generateHash()
			block_miner_msg = {"req":"verify this block", "block.owner": newBlock.owner, "block.prev_hash": newBlock.prev_hash, "block.nonce": newBlock.nonce, "block.transactions": newBlock.transactions.asJson(), "block.final_hash": newBlock.final_hash, "ledger":ledger}
			sendMessage(json.dumps(block_miner_msg), verifier)
			print("New block has been sent to other verifiers")
		elif command == "debug":
			# temp1 = newBlock.transactions.asJson()
			# print(temp1)
			# temp2 = json.loads(temp1)
			# print(temp2)
			# print(temp2[0]["frm"])
			
			# transactions_total_amount = 0
			# for transaction in newBlock.transactions.transact:
			# 	transactions_total_amount += transaction["amount"] * (1 if transaction["frm"]==newBlock.owner else -1)
			# print(transactions_total_amount)
			
			newBlock.generateHash()
			newBlock.commit()
		else:
			print("unknown command")