import paho.mqtt.client as mqtt
#from kafka import KafkaProducer
import datetime
from pymongo import MongoClient
import json
from threading import Thread
import os

path = os.path.dirname(os.path.abspath(__file__))
with open(path + "/dcuConfig.txt") as f:
    lines = f.readlines()

lines = [line.rstrip('\n') for line in lines]

for line in lines:
    line = line.split('=')
    if line[0] == "DCU_ID" :
        _tdcuid = line[1].replace(" ", "")
    elif line[0] == "MQTT_TOPIC" :
        _tmtopic = line[1].replace(" ", "")
    elif line[0] == "MQTT_IP" :
        _tmip = line[1].replace(" ", "")
    elif line[0] == "MQTT_PORT" :
        _tmport = line[1].replace(" ", "")
    elif line[0] == "DPIM_TOPIC":
        _tktopic = line[1].replace(" ", "")
    elif line[0] == "DPIM_IP":
        _tkip = line[1].replace(" ", "")
    elif line[0] == "DPIM_PORT":
        _tkport = line[1].replace(" ", "")
    elif line[0] == "UTIB_TOPIC":
        _uttopic = line[1].replace(" ", "")
    elif line[0] == "UTIB_IP":
        _utip = line[1].replace(" ", "")
    elif line[0] == "UTIB_PORT":
        _utport = line[1].replace(" ", "")
    elif line[0] == "UTIB_USER" : 
        _utuser = line[1].replace(" ", "")
    elif line[0] == "UTIB_PW":
        _utpw = line[1].replace(" ", "")    


print(_tdcuid + _tmtopic + _tmip + _tmport + _tktopic + _tkip + _tkport + _uttopic + _utip + _utport + _utuser + _utpw)

DCU_ID = _tdcuid

MQTT_TOPIC_NAME = _tmtopic
MQTT_SERVER_NAME = 'localhost'
MQTT_SERVER_PORT = int(_tmport)

DPIM_TOPIC_NAME = _tktopic
DPIM_SERVER_NAME = _tkip
DPIM_SERVER_PORT = int(_tkport)

UTIB_TOPIC_NAME = _uttopic
UTIB_SERVER_NAME = _utip
UTIB_SERVER_PORT = int(_utport)
UTIB_USER = _utuser
UTIB_PW = _utpw

def getTimeDate() :    
    nowTime = datetime.datetime.now().today()
    nowTime = nowTime.replace(microsecond=0)
    return str(nowTime)

#### kafka send ###
#def kafkaProducer(producer, topicName, data) :
#    try :
#        producer.send(topicName, value=data)
#        producer.flush()
#        print("[KAFKA :: " + getTimeDate() +"] PRODUCER Send Success - Topic Name : " + str(topicName))
#    except Exception as e :
#        print("[KAFKA :: " + getTimeDate() +"] PRODUCER Send Error - " + str(e))

#### mongoDB Save ###
def spuDataSave(connection, data) :
    try :
        db = connection[data["SPU_ID"]]
        for box in data["BOXES"] :
            collect = db[box["BOX_ID"]]
            collect.insert_one(box)
    except Exception as e :
        print("[DB :: " + getTimeDate() +"] SPU DATA SAVE ERROR - " + str(e))

def on_connect(client, userdata, flags, rc) :
    if rc == 0 :
        print("[MQTT :: " + getTimeDate() + "] Connected Broker")
    else :
        print("[MQTT :: " + getTimeDate() + "] Connected Failed : " + rc)

def on_disconnect(client, userdata, flags, rc=0) :
    print("[MQTT :: " + getTimeDate() + "] Disconnected - " + str(rc))

def on_subscribe(client, userdata, mid, granted_qos) :
    print("[MQTT :: " + getTimeDate() + "] TOPIC Subscribe " + str(mid) + " - " + str(granted_qos))

def on_message(client, userdata, msg) :
    message = str(msg.payload.decode("utf-8"))
    try :
        if message.find("re-Connected") == -1:
            message = message.replace("\'", "\"")
            message = message.replace(" ", "")
            message = message.replace('\n', ' ').replace('\r', '')
            post = json.loads(message)

            ### kafka data trans ###
#            kafkaData = dict()
#            kafkaData["duc_id"] = DCU_ID
#            kafkaData["duc_time"] = getTimeDate().replace(' ', '').replace('-', '').replace(':', '')
#            kafkaData["dcu_payload"] = post
#            Thread(target=kafkaProducer, args=(producer, KAFKA_TOPIC_NAME, kafkaData)).start()
            DPIMdata = dict()
            DPIMdata["DCU_ID"] = DCU_ID
            DPIMdata["DCU_TIME"] = getTimeDate().replace(' ', '').replace('-', '').replace(':', '')
            DPIMdata["DCU_PAYLOAD"] = post
            Thread(target=DpimPublish, args=(DPIM_TOPIC_NAME, DPIMdata)).start()
            Thread(target=spuDataSave, args=(dbCon, post)).start()
            print("[MQTT(DPIM) :: " + getTimeDate() +"] TOPIC (" + msg.topic + ") Recv Success" )
    except Exception as e :
        print("[MQTT(DPIM) :: " + getTimeDate() +"] TOPIC Recv Error - " + str(e))

def Dpim_disConnect(DPIM, userdata, flags, rc=0) :
    print("[MQTT(DPIM) :: " + getTimeDate() + "] Disconnected - " + str(rc))

def Dpim_Connect(DPIM, userdata, flags, rc) :
    if rc == 0 :
        print("[MQTT(DPIM) :: " + getTimeDate() + "] Connected Broker")
    else :
        print("[MQTT(DPIM) :: " + getTimeDate() + "] Connected Failed : " + rc)

def Dpim_publish(DPIM, userdata, mid) :
    print("[MQTT(DPIM) :: " + getTimeDate() + "] TOPIC Subscribe mid=" + str(mid))

def Utib_disConnect(UTIB, userdata, flags, rc=0) :
    print("[MQTT(UTIB) :: " + getTimeDate() + "] Disconnected - " + str(rc))

def Utib_Connect(UTIB, userdata, flags, rc) :
    if rc == 0 :
        print("[MQTT(UTIB) :: " + getTimeDate() + "] Connected Broker")
    else :
        print("[MQTT(UTIB) :: " + getTimeDate() + "] Connected Failed : " + rc)

def Utib_publish(UTIB, userdata, mid) :
    print("[MQTT(UTIB) :: " + getTimeDate() + "] TOPIC publish mid=" + str(mid))

def DpimPublish(topicName, data) :
    try :
        DPIM = mqtt.Client()
        DPIM.on_connect = Dpim_Connect
        DPIM.on_disconnect = Dpim_disConnect
        DPIM.on_publish = Dpim_publish
        DPIM.connect(DPIM_SERVER_NAME, DPIM_SERVER_PORT)
        DPIM.publish(topicName, json.dumps(data, default=str))
        DPIM.disconnect()
        print("[DPIM :: " + getTimeDate() +"] publish Send Success - Topic Name : " + str(topicName) + str(data))
#UTIB publish
        UTIB = mqtt.Client("DCU02")
        UTIB.on_connect = Utib_Connect
        UTIB.on_disconnect = Utib_disConnect
        UTIB.on_publish = Utib_publish    
        UTIB.username_pw_set(UTIB_USER, UTIB_PW)
        UTIB.connect(UTIB_SERVER_NAME, UTIB_SERVER_PORT)
        UTIB.publish(topicName, json.dumps(data, default=str))
        UTIB.disconnect()
        print("[UTIB :: " + getTimeDate() +"] publish Send Success - Topic Name : " + str(topicName))

    except Exception as e :
        print("[DPIM or UTIB :: " + getTimeDate() +"] publish Send Error - " + str(e))

### setup mongoDB ###
dbCon = MongoClient('localhost', 27017)

### setup kafka producer ###
#producer = KafkaProducer(
#    bootstrap_servers=[KAFKA_SERVER_NAME + ":" + str(KAFKA_SERVER_PORT)],
#    acks=0,
#    compression_type='gzip',
#    value_serializer=lambda x : json.dumps(x).encode('utf-8')
#)

### setup MQTT ###
#machine_id = "SPU01"
#client = mqtt.Client(machine_id, clean_session=False)
client = mqtt.Client()
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_subscribe = on_subscribe
client.on_message = on_message

#client.username_pw_set(mqtt_user, mqtt_pw)
client.connect(MQTT_SERVER_NAME, MQTT_SERVER_PORT)
client.subscribe(MQTT_TOPIC_NAME, 1)
client.loop_forever()

