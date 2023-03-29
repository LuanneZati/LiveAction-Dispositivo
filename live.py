from paho.mqtt import client as mqtt_client
import json
import psycopg2
import time
from datetime import datetime
import datetime
import threading
from my_secrets import PASSWD, HOST, PORT, TOPIC, CLIENT_ID, USER, DATABASE, USERDB, PASSDB, HOSTDB

FORMATO_DATA = '%Y-%m-%d %H:%M:%S'
segundos_mqtt = 0
nser = 'Teste'

print("Iniciando")
print("Connecting to broker with client ID '{}'...".format(CLIENT_ID))

def connect_mqtt() -> mqtt_client:
  def on_connect(client, userdata, flags, rc):
    if rc == 0:
      print("Connected to MQTT Broker!")
    else:
      print("Failed to connect!\n")
  client = mqtt_client.Client(CLIENT_ID, clean_session=False)
  client.username_pw_set(USER, PASSWD)
  client.on_connect = on_connect
  client.connect(HOST, PORT)
  return client

def subscribe(client: mqtt_client):
  def on_message(client, userdata, msg):
    global segundos_mqtt
    global nser
    #Pegando mensagem do tópico
    obj = json.loads(msg.payload.decode())
    dh = (obj['dh'])
    nser = (obj['nser'])
    #Passando de string para datetime
    dh = datetime.datetime.strptime(dh, FORMATO_DATA)
    segundos_mqtt = calculaSegundos(dh)
    print('Segundos mqtt dentro da on_message: ', segundos_mqtt)
  client.subscribe(TOPIC)
  client.on_message = on_message

#Chamada para passar hora minuto e segundo para segundos
def calculaSegundos(dh):
  horas = dh.hour * 3600
  minutos = dh.minute * 60
  segundos = dh.second + minutos + horas
  return segundos

def thread():
  contador = 0
  while True:
    time.sleep(60)
    segundos = calculaSegundos(datetime.datetime.now())
    print('Segundos agora: ', segundos, 'Segundos mqtt: ', segundos_mqtt, 'Total: ', segundos - segundos_mqtt)
    #verificação se foi enviado alguma mensagem no ultimo 1 minuto
    if(segundos - segundos_mqtt < 62):
      print("Enviou")
      contador = 0
    elif(segundos - segundos_mqtt > 62 and contador == 0):
      print("Não enviou")
      try:
        connection = psycopg2.connect(host=HOSTDB,
                                      database=DATABASE,
                                      user=USERDB,
                                      password=PASSDB)
        postgresql_insert_query = f"INSERT INTO motor (nser, status, dh) VALUES ('{nser}', 'OFFLINE', '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}')"
        cursor = connection.cursor()
        cursor.execute(postgresql_insert_query)
        connection.commit()
        print(cursor.rowcount, "Data entered successfully")
        cursor.close()
        connection.close()
        print("Closed connection")
      except (Exception, psycopg2.DatabaseError) as error:
        print("Database failure".format(error))
      finally:
        contador+=1
    else:
      print("Não enviou, não inserido no banco!")

def run():
  client = connect_mqtt()
  subscribe(client)
  client.loop_forever()

if __name__ == '__main__':
  t = threading.Thread(target=thread)
  t.daemon = True
  t.start()
  try:
    run()
  except KeyboardInterrupt:
    pass
