import requests
import datetime
import json
import boto3
import os
import dotenv
import sys
import pandas as pd



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
equipment = project_dir + os.environ.get("EQUIPAMENTOS")
url = os.environ.get("URL_ENDPOINT")


#Connect to S3 and check existing reports
s3 = boto3.client('s3')


def get_all_s3_keys(bucket):
    keys = []
    kwargs = {'Bucket': bucket}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            keys.append(obj['Key'])
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break
    return keys


session = requests.Session()  
auth_url = 'http://monitran.com.br/joinville/login'
auth = session.post(auth_url, data={'login': username, 'senha': password})

#Get equipment list
df_equipment_csv = pd.read_csv(equipment, usecols=['equipment'])
equip_list = df_equipment_csv.drop_duplicates(subset=['equipment']).equipment.tolist()

#Scope for download of reports  
day = str(yesterday.day) #int(os.environ.get("START_DAY"))
month = str(yesterday.month) #int(os.environ.get("START_MONTH"))
year = str(yesterday.year) #int(os.environ.get("START_YEAR"))
querystring_date = day+"/"+month+"/"+year
start_time = '00'
end_time = '23'


#JSON LOG FILE
data = {}
data['S3RAWOBJECT'] = {}
data['S3RAWOBJECT']['date'] = querystring_date
data['S3RAWOBJECT']['equipment'] = []
log_file_name = project_dir+'/log/log_monitran_'+year+'_'+month+'_'+day+'.json'
with open(log_file_name, 'w') as outfile:  

  # DOWNLOAD DO RELATORIO PARA TODOS OS EQUIPAMENTOS DISPONIVEIS NA LISTA equipamentos.json
  for equip in equip_list:
      try:
        params = {"equipamento": equip,
                "dataStr": querystring_date,
                "horaInicio": start_time,
                "horaFim": end_time,
                "opcao": 'excel',
                "exibir": "on"
                }
        req = requests.Request("GET", url, params=params)
        response = session.get(url, params=params, stream=True)
        # import pdb
        # pdb.set_trace()
        key = equip + "/" + year + "-" + month.zfill(2) + "-" + day.zfill(2) + '.xlsx'
        s3.put_object(Body=response.content, Bucket=raw_bucket, Key=key)
        data_execucao = datetime.datetime.now()
        data['S3RAWOBJECT']['equipment'].append({
            'name': equip,
            'dateTime': str(data_execucao),
            'status': 'downloaded'
            })        
      except Exception as e:
        data['S3RAWOBJECT']['equipment'].append({
            'name': equip,
            'dateTime': str(data_execucao),
            'status': 'fail',
            'error': e
            })        
  json.dump(data, outfile)
