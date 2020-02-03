import pulsar
import mysql.connector
import json

def receiver(topic, client, process=None,preprocess=None):
   '''
    subscribe the 'client' to 'topic', then wait for message.
    if process is given, will apply the process on the message received
    if preprocess is given, will apply preprocessing on the message, before acknowleging reception
   '''
   consumer = client.subscribe(topic, "test", consumer_type=pulsar.ConsumerType.Exclusive)
   while True:
     msg = consumer.receive()
     print("message received ",msg.message_id())
     if preprocess:
        preprocess(msg)
     consumer.acknowledge(msg)
     if process:    
        process(msg)

def message_to_db(msg):
    table = 'cloudcoverage'
    db = mysql.connector.connect(host='10.0.0.8', user='ubuntu', passwd='Azertyu1',database='zipcloud')
    
    fielddic = json.loads(msg.data()) #transform json message to dictionarry    
    insert_mysql(db,table,fielddic)

def insert_mysql(db,table,fielddic):
    cursor= db.cursor()
    fields= fielddic.keys()
    values = tuple(fielddic.values())
    
    sqlcommand = "INSERT INTO " + table + ' (' +','.join(fields) + ') VALUES (' +'%s,'*(len(fields)-1) + '%s)'
    print(sqlcommand)
    print(values)
    cursor.execute(sqlcommand,values)
    db.commit()
    

if __name__=='__main__':
    pulsar_broker_url = ('pulsar://10.0.0.5:6650')
    consumer_topic = 'LandSatMetaData'

    client = pulsar.Client(pulsar_broker_url)
    receiver(consumer_topic,client,message_to_db)

