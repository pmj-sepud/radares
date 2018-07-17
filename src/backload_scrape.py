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
from sqlalchemy import create_engine, exc, MetaData, select,and_
from sqlalchemy.engine.url import URL


project_dir = os.path.join(os.pardir)
sys.path.append(project_dir)
dotenv_path = os.path.join(project_dir,'.env')
dotenv.load_dotenv(dotenv_path)

#GLOBALS
yesterday = datetime.date.today() - datetime.timedelta(1)
username = os.environ.get("USER_NAME")
password = os.environ.get("PASSWORD")
auth_url = os.environ.get("URL")
raw_bucket = os.environ.get("S3BUCKET_RAW")
equipment = project_dir + os.environ.get("EQUIPAMENTOS")
url = os.environ.get("URL_ENDPOINT")

#CONEXAO WITH MONITRAN PORTAL
session = requests.Session()
auth = session.post(auth_url, data={'login': username, 'senha': password})

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

def validate_date(date_arg):

    # validade informations pass through arguments
    date_format = '%d/%m/%Y'
    try:     
        datetime_obj = datetime.datetime.strptime(date_arg, date_format).date()
    except ValueError:
        print ('Data inicial está em formato inválido, deveria ser dia/mês/ano! Data enviada:', date_arg)
        return None
    
    return datetime_obj

 
initial_date = None
final_date = None
while not initial_date:
    initial_date_arg = input("Data Inicial (DD/MM/YYYY): ")
    initial_date = validate_date(initial_date_arg)
while not final_date:
    final_date_arg = input("Data Final (DD/MM/YYYY): ")
    final_date = validate_date(final_date_arg)

# Print confirmation on terminal
print("-----------------------------------")
print("Baixando relatórios:", str(initial_date), "até", str(final_date))
print("-----------------------------------")

#Get date list
range_days = final_date - initial_date
num_days = range_days.days+1
start_time = '00' #PADRAO 24H DA CONSULTA
end_time = '23' #PADRAO 24H DA CONSULTA
date_range = [initial_date + i*datetime.timedelta(days=1) for i in range(0, num_days)]

#Get equipment list
df_equipment_csv = pd.read_csv(equipment, usecols=['equipment'])
equip_list = df_equipment_csv.drop_duplicates(subset=['equipment']).equipment.tolist()

date_range_dict = {date.strftime("%Y-%m-%d"):equip_list.copy() for date in date_range}

#Find on equipment table and check existing reports on processed bucket
tbl_equipment_files = meta.tables['radars.equipment_files']
query_equipment_files = (tbl_equipment_files.select()
                                           .where(tbl_equipment_files.c.equipment.in_(equip_list))
                                           .where(and_(
                                                    tbl_equipment_files.c.pubdate >= initial_date,
                                                    tbl_equipment_files.c.pubdate <= final_date
                                                )
                                            )
                        )
df_equipment_files = pd.read_sql(query_equipment_files, con=meta.bind)
df_equipment_files_list =  df_equipment_files.loc[:,'file_name'].drop_duplicates().tolist()


for file in df_equipment_files_list:
    file_info = file.split("/")
    file_equip = file_info[0]
    file_date = file_info[1].split(".")[0]
    date_range_dict[file_date].remove(file_equip)

#DOWNLOAD LIST OF FILES FOR EACH EQUIPMENT
s3 = boto3.client('s3')
for date in date_range_dict.keys():
    for equip in date_range_dict[date]:
        date_parts = date.split("-")
        year =  date_parts[0]
        month =  date_parts[1]
        day = date_parts[2]
        querystring_date =  day+"/"+month+"/"+year
        
        params = {"equipamento": equip,
                  "dataStr": querystring_date,
                  "horaInicio": start_time,
                  "horaFim": end_time,
                  "opcao": 'excel',
                  "exibir": "on",
        }
        req = requests.Request("GET", url, params=params)
        response = session.get(url, params=params, stream=True)
        key = equip + "/" + year + "-" + month + "-" + day  + '.xlsx'

        s3.put_object(Body=response.content, Bucket=raw_bucket, Key=key)

        print(equip)
        print(date)
