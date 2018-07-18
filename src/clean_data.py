#!/usr/bin/env python
import os
import sys
import xlrd
import json
import boto3
import xlwt
from io import BytesIO, StringIO
import time
import datetime
import dotenv
import pandas as pd

from sqlalchemy import create_engine, exc, MetaData, select
from sqlalchemy.engine.url import URL

project_dir = os.path.join(os.pardir)
sys.path.append(project_dir)
dotenv_path = os.path.join(project_dir,'.env')
dotenv.load_dotenv(dotenv_path)

def create_empty_wb():
    empty_wb = xlwt.Workbook(encoding='utf-8')
    tab = empty_wb.add_sheet('tab1')
    tab.write(0, 0, 'pubdate')
    tab.write(0, 1, 'equipment')
    tab.write(0, 2, 'direction')
    tab.write(0, 3, 'time_range')
    tab.write(0, 4, 'speed_00_10')
    tab.write(0, 5, 'speed_11_20')
    tab.write(0, 6, 'speed_21_30')
    tab.write(0, 7, 'speed_31_40')
    tab.write(0, 8, 'speed_41_50')
    tab.write(0, 9, 'speed_51_60')
    tab.write(0, 10, 'speed_61_70')
    tab.write(0, 11, 'speed_71_80')
    tab.write(0, 12, 'speed_81_90')
    tab.write(0, 13, 'speed_91_100')
    tab.write(0, 14, 'speed_100_up')
    tab.write(0, 15, 'total')

    return empty_wb

def clean_direction(df):
    df.direction = df.direction.str.split(pat="/", n=1).str.get(1)
    df.direction = df.direction.replace({"^N$": "Norte",
                                         "^S$": "Sul",
                                         "^L$": "Leste",
                                         "^O$": "Oeste"}, regex=True)
    return df

def create_clean_wb(raw_wb):
    sheet = raw_wb.sheets()[0]
    len_data_block = 96

    #get date and equip from inside the file
    date_parts = sheet.cell(2,1).value.split("\n")[0].split(" ")[1].replace("/", "-").split("-")
    file_date = date_parts[2] + "-" + date_parts[1].zfill(2) + "-" + date_parts[0].zfill(2) #%Y-%m-%d
    equip = sheet.cell(5,1).value.split("-")[0]

    #Create clean file
    clean_wb = create_empty_wb()
    tab = clean_wb.get_sheet("tab1")

    #Define template type
    if (sheet.nrows==109) and (sheet.cell(105,1).value.strip() == "Total Geral"):
        template = 1
    elif (sheet.nrows==210) and (sheet.cell(206,1).value.strip() == "Total Geral"):
        template = 2
    elif (sheet.nrows==205) and (sheet.cell(201,1).value.strip() == "Total Geral"):
        template = 3
    else:
        raise Exception("No template was found for " + equip + file_date)

    if template == 1:
        len_data_block = 96
        block1_begin = 8
        direction = sheet.cell(5,15).value       
        blocks_list = [(0, block1_begin, direction)]

    #Template 2 para relatórios que possuem dois sentidos
    if template == 2:
        len_data_block = 96
        block1_begin = 8
        block2_begin = 109
        block1_direction = sheet.cell(5,15).value
        block2_direction = sheet.cell(106,15).value
        blocks_list = [(0, block1_begin, block1_direction), (len_data_block, block2_begin, block2_direction)]

    if template == 3:
        len_data_block = 192
        block1_begin = 8
        direction = sheet.cell(5,15).value
        blocks_list = [(0, block1_begin, direction)]

    #Get and write data
    for a, block_begin, direction in blocks_list: #First block, than second block
        for i in range(0, len_data_block): #row by row, in each block
            #Read data
            read_row = block_begin + i

            time_slot = sheet.cell(read_row,1).value 
            flow00 = sheet.cell(read_row,5).value 
            flow11 = sheet.cell(read_row,7).value
            flow21 = sheet.cell(read_row,9).value
            flow31 = sheet.cell(read_row,10).value
            flow41 = sheet.cell(read_row,12).value
            flow51 = sheet.cell(read_row,13).value
            flow61 = sheet.cell(read_row,14).value
            flow71 = sheet.cell(read_row,15).value
            flow81 = sheet.cell(read_row,17).value
            flow91 = sheet.cell(read_row,18).value
            flow100 = sheet.cell(read_row,20).value
            flowTotal = sheet.cell(read_row,21).value

            #Write data to excel file
            write_row = a + i + 1

            tab.write(write_row, 0, file_date)
            tab.write(write_row, 1, equip)
            tab.write(write_row, 2, direction)
            tab.write(write_row, 3, time_slot)
            tab.write(write_row, 4, flow00)
            tab.write(write_row, 5, flow11)
            tab.write(write_row, 6, flow21)
            tab.write(write_row, 7, flow31)
            tab.write(write_row, 8, flow41)
            tab.write(write_row, 9, flow51)
            tab.write(write_row, 10, flow61)
            tab.write(write_row, 11, flow71)
            tab.write(write_row, 12, flow81)
            tab.write(write_row, 13, flow91)
            tab.write(write_row, 14, flow100)
            tab.write(write_row, 15, flowTotal)

    return clean_wb

def process_clean_wb(clean_wb, s3_client, processed_bucket, meta):
    start = time.time()
    stream = BytesIO()
    clean_wb.save(stream)
    stream.seek(0)
    sheet = xlrd.open_workbook(file_contents=stream.read()).sheets()[0]
    file_date = sheet.cell(1,0).value 
    equip = sheet.cell(1,1).value


    #Create pandas DataFrame
    stream.seek(0)
    df = (pd.read_excel(stream)
          .assign(pubdate = lambda df: pd.to_datetime(df.pubdate))
          .pipe(clean_direction)
         )

    #Save to s3 object
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    write_key = equip + "/" + file_date + '.csv'
    response = s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=processed_bucket, Key=write_key)
    
    date_created_value = response.get('ResponseMetadata').get('HTTPHeaders').get('date')
    date_created_timestamp = time.mktime(datetime.datetime.strptime(date_created_value, '%a, %d %b %Y %H:%M:%S GMT').timetuple())

    #FIRST STEP: VERIFY if info doesn't exist and INSERT a new file on table equipment_files       
    d_equipment = {'file_name': write_key ,'pubdate': file_date,'equipment': equip,'date_created': date_created_value}
    df_equipment = pd.DataFrame(data=d_equipment,index=[0])

    query_df_equipment_fnd = '''
        SELECT id FROM radars.equipment_files
            WHERE file_name = %(file_name)s
            AND pubdate = %(pubdate)s
            AND equipment = %(equipment)s            
    '''  
    
    df_equipment_fnd_slct = pd.read_sql(query_df_equipment_fnd,meta.bind,params=d_equipment,index_col=['id'])        
    if not df_equipment_fnd_slct.index.empty:
        print("Equipamento já existe no banco de dados!")
    else:
        print('insert data in equipment_files table')
        df_equipment.to_sql("equipment_files", schema="radars", con=meta.bind, if_exists="append", index=False)

        
        #SECOND STEP: GET ID of equipment_files BY read_sql with 3 main keys: file_name,pubdate,equipment and date_created in aws
        query_df_equipment = '''
            SELECT id FROM radars.equipment_files
                WHERE file_name = %(file_name)s
                AND pubdate = %(pubdate)s
                AND equipment = %(equipment)s
                AND date_created = %(date_created)s
        '''    
        
        #THIRD STEP: PUT the new equipment_files_id in a new column and INSERT the flows of it
        df_equipment_slct = pd.read_sql(query_df_equipment,meta.bind,params=d_equipment,index_col=['id'])
        df_equipment_idx = df_equipment_slct.index.values[0]
        df_flows_equipment = df.assign(equipment_files_id=df_equipment_idx)

        # REMOVED COLUMNS pubdate, equipment, direction, 
        df_flows_equipment = df_flows_equipment.drop(['pubdate', 'equipment'], axis=1)

        
        print('insert data in flows table')
        df_flows_equipment.to_sql("flows", schema="radars", con=meta.bind, if_exists="append", index=False)
       
        end = time.time()
        duration = str(round(end - start))
        print("Successfully stored equip " + equip + ", on date " + file_date + ", in " + duration + " s.")


if __name__ == '__main__':

    s3 = boto3.client('s3')
    raw_bucket="test-monitran-incoming"
    # raw_bucket = os.environ.get("S3BUCKET_RAW")
    processed_bucket = os.environ.get("S3BUCKET_PROC")

    print("Iterate over all s3 incoming objects")
    all_incoming_objects = []
    paginator = s3.get_paginator('list_objects')
    page_iterator = paginator.paginate(Bucket=raw_bucket)
    for page in page_iterator:
        all_incoming_objects += [c["Key"] for c in page["Contents"] if "xlsx" in c["Key"]]

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

    #Create cleaned workbook
    for file in all_incoming_objects:
        start = time.time()
        print("Begin processing file:", file)
        #Read raw file
        equip, date = file.split("/")
        title_date = date.split(".")[0]
        key = file
        obj = s3.get_object(Bucket=raw_bucket, Key=key)
        wb = xlrd.open_workbook(file_contents=obj['Body'].read())
        
        clean_wb = create_clean_wb(wb)

        process_clean_wb(clean_wb, s3, processed_bucket, meta)

        ''' 
        If we got here, the database has been populated and the clean document has been successfully stored.
        Only now should we proceed and delete the file from the incoming bucket, whether got some problems, the incoming will be 
        cleanning next time
        ''' 
        print('deleting object from AWS S3 incoming')
        del_response = s3.delete_object(Bucket=raw_bucket, Key=key)

                  
        
