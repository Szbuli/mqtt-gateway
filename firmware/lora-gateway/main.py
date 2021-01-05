import socket
import json

HOST = '127.0.0.1'  # The server's hostname or IP address
PORT = 5009

data = {
    "id": "test",
    "value": "testMessage"
}

json_string = json.dumps(data)
byte_json = str.encode(json_string + "\n")
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    s.send(byte_json)
    s.close()
