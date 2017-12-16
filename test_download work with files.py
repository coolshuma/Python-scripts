import psycopg2
import pandas as pd
import csv
import json

DBNAME = 'rawdata'  # ! Необходимо задать соответствующие данные
USERNAME = 'postgres' 
HOSTNAME = 'localhost'
PASSWORD = 'PASSWORD'

def create_table_for_raw_data():  # Создаем таблицу для сылых данных
    try:
        cur.execute("CREATE TABLE raw_data (api_source varchar, api_method varchar, " + \
        "api_date timestamptz, result text, api_param varchar, insert_ts timestamptz DEFAULT now());")
    except:
        pass

try:
    conn = psycopg2.connect(f"dbname={DBNAME} user={USERNAME} host={HOSTNAME} password={PASSWORD}")
    conn.autocommit = True  	# Соединяемся с базой данных
except:
    print("Connect to the database error")

cur = conn.cursor()

create_table_for_raw_data()

def load_csv_to_raw_data_table():  # Загружаем сырые данные из csv-файла в raw_data
    API_METHOD = "get_csv"
    API_SOURCE = "cur_api_source"  # Задаются в соответствии с обращением к серверу
    API_PARAM = "param_of_api"

    with open("reward-2017-02-01.csv", 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        is_names_row = True
        columns_name = ''
        for row in reader:
            if is_names_row:
                columns_name = ','.join(row)  # Запоминаем названия колонок для дальнейшего парсинга
                is_names_row = False
                continue
            cur_result = ','.join(row)
            cur_result = columns_name + ',' + cur_result  # Дополняем названиями csv-колонок
            SQL = "INSERT INTO raw_data (api_source, api_method, api_date, result, api_param, insert_ts)" + \
            "VALUES ((%s), (%s), now(), (%s), (%s), now());"
            data = (API_SOURCE, API_METHOD, cur_result, API_PARAM,)
            try:
                cur.execute(SQL, data)
            except:
                print("Can't insert data into table")

def load_json_to_raw_data_table():   # Загружаем сырые данные из json-файла в raw_data
    API_METHOD = "get_json"
    API_SOURCE = "cur_api_source"   # Задаются в соответствии с обращением к серверу
    API_PARAM = "param_of_api"

    with open("input-2017-02-01.json") as json_file:
        text = json_file.readlines()
        for row in text:
            cur_result = row
            SQL = "INSERT INTO raw_data (api_source, api_method, api_date, result, api_param, insert_ts)" + \
            "VALUES ((%s), (%s), now(), (%s), (%s), now());"
            data_for_sql = (API_SOURCE, API_METHOD, cur_result, API_PARAM,)
            try:
                cur.execute(SQL, data_for_sql)
            except:
                print("Can't insert data into table")

def fill_raw_data_table():  # Заполняем сырые данные в соотвествующую таблицу 
    load_csv_to_raw_data_table()
    load_json_to_raw_data_table()
    
fill_raw_data_table()