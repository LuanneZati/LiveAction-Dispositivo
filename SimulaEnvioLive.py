import time
from paho.mqtt import client as mqtt_client
import uuid
from random import randint
import datetime
from datetime import datetime
from my_secrets import PASSWD, HOST, PORT, TOPIC, CLIENT_ID, USER

print("Iniciando")

def connect_mqtt() -> mqtt_client:
  def on_connect(client, userdata, flags, rc):
    if rc == 0:
      print("Connected to MQTT Broker!")
    else:
      print("Failed to connect!\n")
  client = mqtt_client.Client(CLIENT_ID)
  client.username_pw_set(USER, PASSWD)
  client.on_connect = on_connect
  client.connect(HOST, PORT)
  return client

def publish(client):
  data = datetime.now()
  dh = data.strftime('%Y-%m-%d %H:%M:%S')
  obj = f'"nser" : "Teste", "status": "OK", "dh": "{dh}"'
  msg = '{' + obj + '}'
  result = client.publish(TOPIC, msg)
  status = result[0]
  if status == 0:
    print(f"Send `{msg}` to topic `{TOPIC}`")
  else:
    print(f"Failed to send message to topic {TOPIC}")  

def run():
  client = connect_mqtt()
  while True:
    publish(client)
    time.sleep(60)
  client.loop_forever()

if __name__ == '__main__':
  run()

'''
mqttbox:
{
  "nser": "12089/6",
  "rssi": "-41",
  "temp": "22",
  "loct": "-21.54875,42.85749"
}
'''
