/*
Author:  Eric Tsai, 2017
License:  CC-BY-SA, https://creativecommons.org/licenses/by-sa/2.0/

Purpose:  Gateway for JSON serial data to MQTT via ESP8266 wifi link

Input:  Expected JSON format via uart
{"mac":"c2f154bb0af9","rssi":-55,"volt":3.05,"sensor":1, "key":value}
{"mac":"cow2","rssi":-109,"volt":3.61,"lat":45.055761, "long":-92.980961}

Output:
Iterates through the key:value pairs and publishes in this form:
/BLE/<loc>/<mac>/<key>/ <value>
/ble/livingroom/c2f154bb0af9/volt 3.05
/ble/livingroom/c2f154bb0af9/rssi/ -55

Changes:  ctrl-F "**CHANGE**"
1)  IP address of MQTT broker
2)  <loc> designation, persumably unique per gateway
3)  SSID and password

Notes:  To test by subscribing to all topics
mosquitto_sub -h localhost -v -t '#'
*/

/*
Modified By: Shaun Wilkinson, 2021
Changes -
Removed all references to existing temperature reporting and reed functionality.
Upgraded ArduinoJson from 5 to latest 6.
Completely rewritten localParseJson function to handle multiple JsonPair value types
 */

#include <ESP8266WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>

// Update these with values suitable for your network.

#define LED 16    //GPIO16 is LED on my ESP8266 module

//"**CHANGE**" wifi SSID and password
const char* ssid = "PLUSNET-RKH2";
const char* password = "fe5a954c8a";

//"**CHANGE**" MQTT server IP address
const char* mqtt_server = "192.168.1.75";  //mqtt server server 2.3

const char mqtt_name[]="TopLeft"; // Used as part of the topic
//const char mqtt_name[]="livingroom";

WiFiClient espClient;
PubSubClient client(espClient);
long lastMsg = 0;
char msg[50];
int value = 0;

IPAddress server(192, 168, 2, 53);
unsigned long heartbeat_millis=0;
int heartbeat_cnt=0;

//"**CHANGE**" debug output
#define MyDebug 1   //0=no debug, 1=deeebug

const int pinHandShake = 4; //handshake pin, esp8266 GPIO4/D2

//buffer for char[] used in mqtt publishes, size at least as big as topic + msg length
char mqtt_buf[120];  // e.g "/ble/esp1/d76dcd1b3720/volt" is 30 chars long

bool conn_status;
// Callback function header
void callback(char* topic, byte* payload, unsigned int length);

const int serialBufSize = 120;      //size for at least the size of incoming JSON string
static char serialbuffer[serialBufSize];  // {"mac":c2f154bb0af9,"rssi":-55,"volt":3.05,"mag":1}
static char mytopic[120];  //MQTT topic string size, must be less than size declared.
//todo: add error checking for mqtt msg lengths


void setup_wifi() {

  delay(10);
  // We start by connecting to a WiFi network
  Serial.println();
  Serial.print("Connecting to ");
  Serial.println(ssid);

  WiFi.mode(WIFI_STA); // Disable advertising it's own network
  WiFi.begin(ssid, password);

  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }

  randomSeed(micros());

  Serial.println("");
  Serial.println("WiFi connected");
  Serial.println("IP address: ");
  Serial.println(WiFi.localIP());
  digitalWrite(LED, HIGH);  //off
}

void callback(char* topic, byte* payload, unsigned int length) {
  Serial.print("Message arrived [");
  Serial.print(topic);
  Serial.print("] ");
  for (int i = 0; i < length; i++) {
    Serial.print((char)payload[i]);
  }
  Serial.println();

  char message_buff[60]; //payload string
  // create string for payload
  int i=0;
  for(i=0; i<length; i++)
  {
    message_buff[i] = payload[i];
  }
  message_buff[i] = '\0';  //terminate buffer string class eol
  String msgString = String(message_buff);
  

}

void reconnect() {
  // Loop until we're reconnected
  while (!client.connected()) {
    Serial.print("Attempting MQTT connection...");
    // Create a random client ID
    String clientId = "ESP8266Client-";
    clientId += String(random(0xffff), HEX);
    // Attempt to connect
    if (client.connect(clientId.c_str())) {
      Serial.println("connected");
      // Once connected, publish an announcement...
      client.publish("outTopic", "hello world");
      
    } else {
      Serial.print("failed, rc=");
      Serial.print(client.state());
      Serial.println(" try again in 5 seconds");
      // Wait 5 seconds before retrying
      delay(5000);
    }
  }
}

void setup() {
  pinMode(LED, OUTPUT);     //  BUILTIN_LED pin as an output, actually pin D0 (GPIO16)
  digitalWrite(LED, LOW);  //LED on
  Serial.begin(9600);   //ESP8266 default serial on UART0 is GPIO1 (TX) and GPIO3 (RX)
  while (!Serial) {
    ; // wait for serial port to connect. Needed for Leonardo only
  }
  pinMode(pinHandShake, OUTPUT);
  setup_wifi();
  client.setServer(mqtt_server, 1883);
  client.setCallback(callback);
}


//stealing this code from
//https://hackingmajenkoblog.wordpress.com/2016/02/01/reading-serial-on-the-arduino/
//non-blocking serial readline routine, very nice.  Allows mqtt loop to run.
int readline(int readch, char *buffer, int len)
{
  static int pos = 0;
  int rpos;

  if (readch > 0) {
    switch (readch) {
      case '\n': // Ignore new-lines
        break;
      case '\r': // Return on CR
        rpos = pos;
        pos = 0;  // Reset position index ready for next time
        return rpos;
      default:
        if (pos < len-1) {
          buffer[pos++] = readch;   //first buffer[pos]=readch; then pos++;
          buffer[pos] = 0;
        }
    }
  }
  // No end of line has been found, so return -1.
  return -1;
}

//json iterator that parses char* and publish each key-value pair as mqtt data
void localParseJson(char *mybuf)
{

  //JSON parsing
  StaticJsonDocument<200> doc;
  auto error = deserializeJson(doc, mybuf);
  
  /*
    Incoming uart:
    mac:c2f154bb0af9,rssi:-55,volt:3.05,mag:1
    
    Iterate and publish as
    /ble/livingroom/c2f154bb0af9/rssi -55
    /ble/livingroom/c2f154bb0af9/volt 3
    /ble/livingroom/c2f154bb0af9/mag 1

  1)  count # fields in after MAC
  2)  concat "ble"/"MAC"/+iterate field
  3)  capture data to publish
   */
  if (!error)
  {
    // Parsing success, valid json

    JsonObject root = doc.as<JsonObject>();
      
    int topic_char_index=0;
  
    //construct the first part of the topic name, which is just /ble/mac
    mytopic[0]='/'; topic_char_index++;
    mytopic[1]='y'; topic_char_index++;
    mytopic[2]='y'; topic_char_index++;
    mytopic[3]='y'; topic_char_index++;
    mytopic[4]='/'; topic_char_index++;  //topic_char_index is 5;

    //add gateway location name, /ble/LIVINGROOM
    int mqtt_name_len = sizeof(mqtt_name);  //returns num of chars + 1 for \0 character
    
    #if MyDebug
    Serial.print("topic_char_index before mqtt_name:  ");
    Serial.println(topic_char_index);
    Serial.print("size of mqtt_name_len should be 16:  ");
    Serial.println(mqtt_name_len);
    #endif

    for (int i=0; i<(mqtt_name_len-1); i++)
    {
      #if MyDebug
      Serial.println(mqtt_name[i]);
      #endif
      mytopic[topic_char_index] = mqtt_name[i];
      topic_char_index++;
    }
    mytopic[topic_char_index] = '/';  topic_char_index++;
    
    #if MyDebug
    Serial.print("topic_char_index after mqtt_name:  ");
    Serial.println(topic_char_index);
    #endif
    
    //set string pointer to JSON Value containing the key "mac"
    // so it's pointing to "mac":c2f154bb0af9
    const char* mac_name = doc["mac"];
    
    #if MyDebug
    Serial.print("mac name is ");
    Serial.print(mac_name);
    Serial.print("\r\n");
    #endif
    
    int sizeofff = strlen(mac_name);    //returns exactly number of characters in mac_name.  Doesn't add 1 like sizeof

    #if MyDebug
    Serial.print("size of macname: ");
    Serial.println(sizeofff);
    #endif

    // /ble/livingroom/c2f154bb0af9/
    //fill mac address into topic name string
    for (int i=0; i<sizeofff; i++)
    {
      mytopic[topic_char_index]= mac_name[i];
      topic_char_index++;
    }
    mytopic[topic_char_index] = '/';
    topic_char_index++;

    #if MyDebug
    Serial.println("mytopic after mac address add");
    Serial.println(mytopic);
    #endif
  
    /*
     iterate through sensor topics portions
        /ble/livingroom/c2f154bb0af9/????
        /ble/livingroom/c2f154bb0af9/rssi
        /ble/livingroom/c2f154bb0af9/volt
        /ble/livingroom/c2f154bb0af9/sensor
    */

    //as we iterate through the keys in json, need a marker for char[] index
    //up to the MAC address:  "/ble/livingroom/c2f154bb0af9/"  <---- here
    int topic_char_index_marker = topic_char_index;
    
    for (JsonPair kv : root)
    {
      topic_char_index = topic_char_index_marker; //reset index to just after MAC location
      /*
       /ble/livingroom/c2f154bb0af9/  <---- here
      */

      //copy string for key into the prepared topic string
      const char* key_name = kv.key().c_str();  //funny, key_name has to be const to compile in ESP8266. 
      int keysize = strlen(key_name);
      
      for (int j=0; j<keysize; j++)
      {
        mytopic[topic_char_index] = key_name[j];  topic_char_index++;
        
      }
      
      // note that topic_char_index is sitting at position after rss"i"
       //  /ble/c2f154bb0af9/rssi   <----- rssi, volt, etc...
       //

      mytopic[topic_char_index] = '\0';  //terminate string w/ null

      #if MyDebug
      Serial.print("topic_char_index of null char");
      Serial.println(topic_char_index);
      Serial.println("printing entire mytopic:");
      Serial.print(mytopic);
      #endif
      
      //mqtt publish relies a null terminated string for topic names
      JsonVariant value = kv.value();
      const char valueType = getType(value);
      
      // Get variant type then publish it
      // This is required as attempting to cast values as char* fails and produces a null value when the value isn't char*
      switch(valueType) {
        case 'c':
          client.publish(mytopic, value.as<char*>());
          break;
        case 'i': {
            char b[5];
            String str;
            str = String(value.as<int>());
            str.toCharArray(b, 5);
            client.publish(mytopic, b);
          }
          break;
        case 'f': {
            char result[5];
            dtostrf(value.as<float>(), 5, 2, result);
            client.publish(mytopic, result); 
          }
          break;
      }
      
    }//end iterator

  }//end if json parsed successful
  else
  {
    client.publish(mqtt_name, "bad json format");
    client.publish(mqtt_name, mybuf);
    Serial.println(mybuf);
  } //failed json parse

  digitalWrite(LED, HIGH);  //LED off
} //end routine localParseJson

// Simple function that returns a char which indicates the type of value being passed
char getType(JsonVariant jv) {
  if(jv.is<char*>())
    return 'c';
  if(jv.is<int>())
    return 'i';
  if(jv.is<float>())
    return 'f';
}


//*****************************
// main loop
//
//*****************************
void loop() {
  //client.loop();
  if (!client.connected()) {
    reconnect();
  }
  client.loop();
  //buffer for reading serial, must be writable
  
  if (Serial.available() > 0)
  {
    
    //received serial line of json
    if (readline(Serial.read(), serialbuffer, serialBufSize) > 0)
    {
      digitalWrite(pinHandShake, HIGH);
      digitalWrite(LED, LOW);  //LED on
      #if MyDebug
      Serial.print("You entered: >");
      Serial.print(serialbuffer);
      Serial.println("<");
      #endif
      
      localParseJson(serialbuffer); //call JSON iterator on this line from serial
      digitalWrite(LED, HIGH);  //LED off
      digitalWrite(pinHandShake, LOW);
    }

  }//end seria.available

  
  //every X seconds, publish heartbeat
  if ((millis() - heartbeat_millis > 20000) || ((millis() - heartbeat_millis) < 0))
  {
    heartbeat_millis = millis();
    heartbeat_cnt++;
    conn_status = client.connected();
    Serial.print("20 second heartbeat:  ");
    if (conn_status == 0)
    {
      Serial.print("mqtt dead, attempting to connect...");
      client.disconnect();
      delay(2000);
      if (client.connect(mqtt_name))
      {
        Serial.println("failed to reconnect MQTT"); 
      }
      else
        Serial.println("reconnected succesful");
    }
    else
    {
      Serial.println("mqtt alive");
    } 
    if (conn_status == 0)
    {
      client.publish(mqtt_name, "had to reconnect");
    }
    client.publish(mqtt_name,itoa(heartbeat_cnt, mqtt_buf, 10));
    
  } //end heartbeat
} //end loop
