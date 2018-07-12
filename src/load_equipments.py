import requests
import datetime
import json
import boto3
import os
import dotenv
import sys
import argparse
import datetime
import pandas as pd
from sqlalchemy import create_engine, exc, MetaData, select
from sqlalchemy.engine.url import URL


project_dir = os.path.join(os.pardir)
sys.path.append(project_dir)
dotenv_path = os.path.join(project_dir,'.env')
dotenv.load_dotenv(dotenv_path)


#GLOBALS
yesterday = datetime.date.today() - datetime.timedelta(1)
session = requests.Session()
username = os.environ.get("USER_NAME")
password = os.environ.get("PASSWORD")
auth_url = os.environ.get("URL")
raw_bucket = os.environ.get("S3BUCKET_RAW")
equipment = os.environ.get("EQUIPAMENTOS")
url = os.environ.get("URL_ENDPOINT")


df = pd.read_json('equipamentos.json')


 #Store in Database - ??? Is necessery perist DB everytime to record tables? We can put this piece of code above, before the loop
DATABASE = {
    'drivername': os.environ.get("RADARS_DRIVERNAME"),
    'host': os.environ.get("RADARS_HOST"), 
    'port': os.environ.get("RADARS_PORT"),
    'username': os.environ.get("RADARS_USERNAME"),
    'password': os.environ.get("RADARS_PASSWORD"),
    'database': os.environ.get("RADARS_DATABASE"),
    }


#DATABASE CONNECTION ON SCHEMA radars
db_url = URL(**DATABASE)
engine = create_engine(db_url)
meta = MetaData()
meta.bind = engine
meta.reflect(schema="radars")


# Removed columns none used    
df = df.drop(['endereco','endereco_alterado','radar_2_sentidos'], axis=1)

             
'''
RENAME COLUMNS:
TABLE | JSON    

equipment = equipamento
pubdate, = now() # INSERTION TIME NOW()
latitude = latitude
longitude = longitude
bike_lane = ciclofaixa
bus_lane = corredor
parking_lane = estacionamento
number_lanes = n_faixa_carro_sentido
'''
df = df.rename(index=str, columns={"ciclofaixa":"bike_lane" , "corredor":"bus_lane" ,"equipamento":"equipment",  "estacionamento":"parking_lane", "n_faixa_carro_sentido":"number_lanes"})



# Added pubdate column with now() date
hoje = datetime.date.today()
df = df.assign(pubdate=hoje)

#CAST some columns
df['bike_lane'] = df['bike_lane'].apply(lambda x: True if x == 1 else x)
df['bike_lane'] = df['bike_lane'].apply(lambda x: False if x == 0 else x)
df['bus_lane'] = df['bus_lane'].apply(lambda x: True if x == 1 else x)
df['bus_lane'] = df['bus_lane'].apply(lambda x: False if x == 0 else x)
df['parking_lane'] = df['parking_lane'].apply(lambda x: True if x == 1 else x)
df['parking_lane'] = df['parking_lane'].apply(lambda x: False if x == 0 else x)


#INSERT on table equipments the json file
df.to_sql("equipments", schema="radars", con=meta.bind, if_exists="append", index=False)
