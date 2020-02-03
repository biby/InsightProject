import boto3
import pandas as pd
import pulsar
from random import randint

def send_publication(producer,data):
    producer.send(data.encode('utf-8'))

def get_data(dataframe,index=None):
    """
        Return the 'index'-th row of 'dataframe' as a json formated string. 
        If 'index' is not set or out of bounds, and random row is extracted.
    """
    dflen = len(dataframe)
    if index==None or index <0 or index >= dflen:
        index = randint(0,dflen)
    return dataframe.iloc[index].to_json()
        

if __name__=='__main__':
    nb_messages = 10

    csvfile = 'scene_list_LandSat_Data.csv'
    df = pd.read_csv(csvfile)
    
    #Setting pulsar producer
    pulsar_broker = "pulsar://10.0.0.5:6650"
    client = pulsar.Client(pulsar_broker)
    publication_topic = "LandSatMetaData"
    producer = client.create_producer(publication_topic)

    for _ in range(10):
        data = get_data(df)
        send_publication(producer,data)
    
