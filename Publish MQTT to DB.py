import os
import atexit
import re
from datetime import datetime
import time

import sqlite3
import paho.mqtt.client as mqtt

DB_NAME = 'location_data.db'
TABLE_NAME = 'tag_data'

MQTT_TOPIC = '/yyy/+/+/+' #/yyy/Study/f2734bec8248/rssi
MQTT_REGEX = '/yyy/([^/]+)/([^/]+)/([^/]*)'
MQTT_CLIENT_ID = 'PIServer'

dbConnection = sqlite3.connect(DB_NAME); # Connect to the SQLite database
# Create the table if it doesn't already exist
dbConnection.execute('CREATE TABLE IF NOT EXISTS tag_data ' \
                     '(submitTime int, gateway varchar(25), tagMac varchar(12), rssi int(4), volt long(4), tmr int(4), xcnt int(4), BEA int(1))')

# Define a tag_data class which defines the values stored in the mqtt_buffer array
class tag_data:
    def __init__(self, gateway_name, tag_name, measure_name, value):
        self.submit_time = int(time.time())
        self.gateway_name = gateway_name
        self.tag_name = tag_name
        self.measure_name = measure_name
        self.value = value
        
    def get_submit_time(self):
        return self.submit_time
    
    def get_gateway(self):
        return self.gateway_name
    
    def get_tag(self):
        return self.tag_name
    
    def get_measure(self):
        return self.measure_name
    
    def get_value(self):
        return self.value
    
    def to_string(self):
        return self.get_gateway() + ", " + self.get_tag() + ", " + self.get_measure() + ", " + self.get_value()

def on_exit():
    if(connection.total_changes() > 0):
        dbConnection.commit()
    dbConnection.close()
    mqtt_client.stop()
                    
def on_connect(client, userdata, flags, rc):
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC)
  
def on_message(client, userdata, msg):
    #print(msg.topic + ' ' + str(msg.payload))
    
    sensor_data = _parse_mqtt_message(msg.topic, msg.payload.decode('utf-8'))
    if sensor_data is not None:
        _send_sensor_data_to_sqlite(sensor_data)
    
def _parse_mqtt_message(topic, payload):
    match = re.match(MQTT_REGEX, topic)
    
    if match:
        gateway_name = match.group(1)        
        tag_name = match.group(2)
        measure_name = match.group(3)
        value = payload.strip()
        
        if (measure_name == 'mac'):
            return None
        else:
            return tag_data(gateway_name, tag_name, measure_name, value)

    else:
        return None


def _send_sensor_data_to_sqlite(sensor_data):
    print(sensor_data.to_string())
    sql_query = 'INSERT INTO {} (submitTime, gateway, tagMac, measureName, value) \n' \
        ' VALUES ({}, {}, {}, {}, {})' \
        .format(TABLE_NAME, sensor_data.get_submit_time(), "'{}'".format(sensor_data.get_gateway()), "'{}'".format(sensor_data.get_tag()), "'{}'".format(sensor_data.get_measure()), sensor_data.get_value())
    
    print(sql_query)
    dbConnection.execute(sql_query)
    dbConnection.commit()
    print("Entered record into db")

def main():
    mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    
    mqtt_client.connect('localhost', 1883)
    mqtt_client.loop_forever()

if __name__=='__main__':
    print('Started MQTT to SQLite Storage')
    print('DB Location: ' + os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/" + DB_NAME)
    main()

atexit.register(on_exit)

