import requests
import datetime
import json
import boto3
import os
import dotenv
import sys
import pandas as pd
from io import BytesIO
import xlrd

from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine.url import URL

import clean_data

dir_path = os.path.dirname(os.path.realpath(__file__))
project_dir = os.path.join(dir_path, os.pardir)
sys.path.append(project_dir)
dotenv_path = os.path.join(project_dir,'.env')
dotenv.load_dotenv(dotenv_path)

#GLOBALS
yesterday = datetime.date.today() - datetime.timedelta(1)
session = requests.Session()
username = os.environ.get("USER_NAME")
password = os.environ.get("PASSWORD")
auth_url = os.environ.get("URL")
raw_bucket="test-monitran-incoming"
# raw_bucket = os.environ.get("S3BUCKET_RAW")
processed_bucket = os.environ.get("S3BUCKET_PROC")
equipment = project_dir + os.environ.get("EQUIPAMENTOS")
url = os.environ.get("URL_ENDPOINT")

#Connect to S3
s3 = boto3.client('s3')

#Database connection
DATABASE = {
    'drivername': os.environ.get("RADARS_DRIVERNAME"),
    'host': os.environ.get("RADARS_HOST"), 
    'port': os.environ.get("RADARS_PORT"),
    'username': os.environ.get("RADARS_USERNAME"),
    'password': os.environ.get("RADARS_PASSWORD"),
    'database': os.environ.get("RADARS_DATABASE"),
    }

db_url = URL(**DATABASE)
engine = create_engine(db_url)
meta = MetaData()
meta.bind = engine
meta.reflect(schema="radars")

#Monitran connection
session = requests.Session()  
auth_url = 'http://monitran.com.br/joinville/login'
auth = session.post(auth_url, data={'login': username, 'senha': password})

#Get equipment list
df_equipment_csv = pd.read_csv(equipment, usecols=['equipment'])
equip_list = df_equipment_csv.drop_duplicates(subset=['equipment']).equipment.tolist()

#Scope for download of reports  
day = str(yesterday.day)
month = str(yesterday.month)
year = str(yesterday.year)
querystring_date = day+"/"+month+"/"+year
params = {"dataStr": querystring_date,
            "horaInicio": '00',
            "horaFim": '23',
            "opcao": 'excel',
            "exibir": "on"
        }

#JSON LOG FILE
data = {}
data['S3RAWOBJECT'] = {}
data['S3RAWOBJECT']['date'] = querystring_date
data['S3RAWOBJECT']['equipment'] = []

# DOWNLOAD DO RELATORIO PARA TODOS OS EQUIPAMENTOS DISPONIVEIS NA LISTA equipamentos.csv
for equip in equip_list:
    try:
        #Download and save in S3 Incoming
        print("Downloading", equip, "and saving in S3 incoming bucket")
        params["equipamento"] = equip
        response = session.get(url, params=params, stream=True)
        key = equip + "/" + year + "-" + month.zfill(2) + "-" + day.zfill(2) + '.xlsx'
        s3.put_object(Body=response.content, Bucket=raw_bucket, Key=key)
        data_execucao = datetime.datetime.now()
        data['S3RAWOBJECT']['equipment'].append({
            'name': equip,
            'dateTime': str(data_execucao),
            'status': 'downloaded'
            })
        #Process and save in S3 Processed
        print("Processing", equip, "saving in S3 processed bucket and saving in database.")
        raw_wb = xlrd.open_workbook(file_contents=response.content)
        clean_wb = clean_data.create_clean_wb(raw_wb)
        clean_data.process_clean_wb(clean_wb, s3, processed_bucket, meta)
        data['S3RAWOBJECT']['equipment'].append({
            'name': equip,
            'dateTime': str(data_execucao),
            'status': 'processed'
            })
    except Exception as e:
        data['S3RAWOBJECT']['equipment'].append({
            'name': equip,
            'dateTime': str(data_execucao),
            'status': 'fail',
            'error': str(e)
            })
    else:
        ''' 
        If we got here, the database has been populated and the clean document has been successfully stored.
        Only now should we proceed and delete the file from the incoming bucket
        ''' 
        print('deleting object from AWS S3 incoming')
        del_response = s3.delete_object(Bucket=raw_bucket, Key=key)


log_file_name = project_dir+'/log/log_monitran_'+year+'_'+month+'_'+day+'.json'
with open(log_file_name, 'w') as outfile:  
    json.dump(data, outfile)
