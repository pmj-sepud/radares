import requests
import datetime
import json
import boto3
import os
import dotenv
import sys
import argparse
import datetime


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

#CONEXAO COM O PORTAL MONITRAN
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
        '-equip', '--equipamento', type=str, help='Nome do Equipamento', required=False)
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


# Print confirmation on terminal
print("-----------------------------------")
print("Baixando relatorios: " + equipment, " > " + initial_date, " - " +final_date, sep='\n')
print("-----------------------------------")


#Get equipment list
with open('equipamentos.json') as json_data:
    equipamentos = json.load(json_data)
equip_set = set([equipamento["equipamento"] for equipamento in equipamentos])
equip_list = list(equip_set)
equip_list.sort()

#Scope for download of reports
start_day=1
start_month=9
start_year=2017
start_date = datetime.date(day=start_day, month=start_month, year=start_year)
num_days = 120
step = datetime.timedelta(days=1)
start_time = '00' #PADRAO 24H DA CONSULTA
end_time = '23' #PADRAO 24H DA CONSULTA
date_range = [start_date + day*step for day in range(0, num_days)]
date_range_dict = {date.strftime("%Y-%m-%d"):equip_list.copy() for date in date_range}

#Connect to S3 and check existing reports
s3 = boto3.client('s3')
all_files = [c["Key"] for c in s3.list_objects_v2(Bucket="monit-data")["Contents"]]


for file in all_files:
    file_info = file.split("/")
    file_equip = file_info[1]
    file_date = file_info[2].split(".")[0]
    date_range_dict[file_date].remove(file_equip)

for date in date_range_dict.keys():
    for equip in date_range_dict[date]:
        date_parts = date.split("-")
        year =  date_parts[0]
        month =  date_parts[1]
        day = date_parts[2]
        querystring_date =  day+"/"+month+"/"+year

        url = "http://monitran.com.br/joinville/relatorios/fluxoVelocidadePorMinuto/gerar"
        params = {"equipamento": equip,
                  "dataStr": querystring_date,
                  "horaInicio": start_time,
                  "horaFim": end_time,
                  "opcao": 'excel',
                  "exibir": "on",
        }
        req = requests.Request("GET", url, params=params)
        response = session.get(url, params=params, stream=True)
        key = "raw/" + equip + "/" + year + "-" + month + "-" + day  + '.xlsx'

        s3.put_object(Body=response.content, Bucket='monit-data', Key=key)

        print(equip)
        print(date)
