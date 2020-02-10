import boto3
import pandas as pd
import pulsar
from random import randint
import datetime
import time

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
    nowString = datetime.datetime.now().strftime('%y-%m-%d %H:%M:%S.%f')
    row = dataframe.iloc[index]
    row['acquisitionDate']=nowString
    row['productId']= 'FAKE'+row['productId']
    row['entityId']= 'FAKE'+row['entityId']
    row['min_lat'] =-123.181725
    row['min_long'] = 36.452961
    row['max_lat'] = -121.181725
    row['max_long'] = 38.452961
    return row.to_json()
        

if __name__=='__main__':
    nb_messages = 10
    s3=boto3.client('s3')
    s3Bucket = 'ideanalytics'
    csvfile = 'scene_list_LandSat_Data.csv'
    imdownloader = s3.get_object(Bucket=s3Bucket,Key=csvfile)
    df = pd.read_csv(imdownloader["body"])
    
    #Setting pulsar producer
    pulsar_broker = "pulsar://10.0.0.5:6650"
    client = pulsar.Client(pulsar_broker)
    publication_topic = "LandSatMetaData"
    producer = client.create_producer(publication_topic)

    for _ in range(60):
        data = get_data(df)
        send_publication(producer,data)
        time.sleep(1)
