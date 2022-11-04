import socket
import time
import mysql.connector
import json

receiver = 1

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("localhost", 6088))
s.listen(1)

while True:
    time.sleep(1)
    conn, addr = s.accept()
    data = conn.recv(1024)
    # print(data)
    data = json.loads(data.decode("utf-8")) 
    conn.close()
    # print(type(data))
    connection = mysql.connector.connect(user='root', password='zxcvB!23', host='127.0.0.1', database='DS')
    cursor_s = connection.cursor(buffered=True)
    cursor_u = connection.cursor()
    cursor_s.execute("SELECT * FROM `log1`;")
    
    
    for (node, n1, n2, n3, n4) in cursor_s:
        vals = [n1, n2, n3, n4]
        for i in range(4):
            # print(data.keys());print(node);
            if data[node]["n%i" % (i+1)] != vals[i]:
               try: 
                   cursor_u.execute("UPDATE `log1` SET `n%i`='%i' WHERE `node`='%s';" % (i+1, max(data[node]["n%i" % (i+1)], vals[i]), node))
                   cursor_u.commit()
               except Exception as e:
                   print(e)
            
    
    cursor_s.close()
    cursor_u.close()
    connection.close()