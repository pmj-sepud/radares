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
equipment = os.environ.get("EQUIPAMENTOS")
url = os.environ.get("URL_ENDPOINT")

#CONEXAO WITH MONITRAN PORTAL
session = requests.Session()
auth = session.post(auth_url, data={'login': username, 'senha': password})


def get_args():
    """Parse the passed arguments. If arguments are passed assign them to the file
       name variables. If not prompt the user to enter the file names."""
    # Define the description
    parser = argparse.ArgumentParser(
        description='Script para backload de equipamentos por periodo!')
    # Define the first argument
    parser.add_argument(
        '-equip', '--equipamento', type=str, help='Nome dos Equipamentos (SEPARADO POR VIRGULA)', required=False)
    # Define the second argument
    parser.add_argument(
        '-dt_ini', '--initial_date', type=str, help='Data Inicial (DD/MM/YYYY)', required=False)
    parser.add_argument(
        '-dt_fin', '--final_date', type=str, help='Data Final (DD/MM/YYYY)', required=False)    
    args = parser.parse_args()
    if args.equipamento:
        arg1 = args.equipamento
        arg2 = args.initial_date
        arg3 = args.final_date
    else:
        arg1 = input("Nome do Equipamento: ")
        arg2 = input("Data Inicial (DD/MM/YYYY): ")
        arg3 = input("Data Final (DD/MM/YYYY): ")
    # Return the file name variables
    return arg1, arg2, arg3


equipment,initial_date,final_date = get_args()



# validade informations pass through arguments
date_format = '%d/%m/%Y'
try:     
    date_obj = datetime.datetime.strptime(initial_date, date_format)
except ValueError:
    print ('Data inicial está em formato inválido, deveria ser dia/mês/ano! Data enviada:', initial_date)
    sys.exit(1)
try:
    date_obj = datetime.datetime.strptime(final_date, date_format)
except ValueError:
    print ('Data final está em formato inválido,deveria ser dia/mês/ano! Data enviada:', final_date)
    sys.exit(1)

try:
    #Get equipment list on table equipments. Count number of equipments to fetch    
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

    equipment_list =  equipment.split(',')
    equipment_list.sort()

    tbl_equipment = meta.tables['radars.equipments']
    equipments_query = tbl_equipment.select(tbl_equipment.c.equipment.in_(equipment_list))
    df_equipments = pd.read_sql(equipments_query, con=meta.bind)
    df_equipments_list =  df_equipments.loc[:,'equipment'].tolist()
    
    #VALIDATing lists to continue the file download 
    if not equipment_list == df_equipments_list:
        equipments_not_found = [item for item in equipment_list if item not in df_equipments_list]      
        #FORCE ValueError to quit the program
        raise ValueError(equipments_not_found)            
except ValueError:
    print ('Equipamentos não encontrados na base de dados:', equipments_not_found)
    sys.exit(1)


# Print confirmation on terminal
print("-----------------------------------")
print("Baixando relatórios: " + equipment, " > " + initial_date, " até " +final_date, sep='\n')
print("-----------------------------------")

#Scope for download of reports with args. 
initial_datetime_obj = datetime.datetime.strptime(initial_date, '%d/%m/%Y')
final_datetime_obj = datetime.datetime.strptime(final_date, '%d/%m/%Y')
range_days = final_datetime_obj - initial_datetime_obj
start_date = initial_datetime_obj
num_days = range_days.days+1
step = datetime.timedelta(days=1)
start_time = '00' #PADRAO 24H DA CONSULTA
end_time = '23' #PADRAO 24H DA CONSULTA
date_range = [start_date + day*step for day in range(0, num_days)]
date_range_dict = {date.strftime("%Y-%m-%d"):equipment_list.copy() for date in date_range}



#NEW METHOD - Find on equipment table and check existing reports on processed bucket
tbl_equipment_files = meta.tables['radars.equipment_files']
query_equipment_files = tbl_equipment_files.select(tbl_equipment_files).where(
                                            tbl_equipment_files.c.equipment.in_(equipment_list)
                                            ).where(
                                                and_(
                                                    tbl_equipment_files.c.pubdate >= initial_datetime_obj,
                                                    tbl_equipment_files.c.pubdate <= final_datetime_obj
                                                )
                                            )
df_equipment_files = pd.read_sql(query_equipment_files, con=meta.bind)
df_equipment_files_list =  df_equipment_files.loc[:,'file_name'].tolist()


for file in df_equipment_files_list:
    file_info = file.split("/")
    file_equip = file_info[0]
    file_date = file_info[1].split(".")[0]
    date_range_dict[file_date].remove(file_equip)




# OLD METHOD was search on raw bucket to seek for same files. But now will be ignored and downloaded again replaccing the old file
# s3 = boto3.client('s3')
# all_files = [c["Key"] for c in s3.list_objects_v2(Bucket=raw_bucket)["Contents"]]
# for file in all_files:
#     file_info = file.split("/")
#     file_equip = file_info[1]
#     file_date = file_info[2].split(".")[0]
#     date_range_dict[file_date].remove(file_equip)


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
