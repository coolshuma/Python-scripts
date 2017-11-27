import psycopg2
import pandas as pd
import csv
import json
import urllib
import os

DBNAME = 'rawdata'  # ! Необходимо задать соответствующие данные
USERNAME = 'postgres' 
HOSTNAME = 'localhost'
PASSWORD = '5300expres'

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

def load_csv_to_raw_data_table(FILE_NAME = "reward-2017-02-01.csv"):  # Загружаем сырые данные из csv-файла в raw_data
    API_METHOD = "get_csv"
    API_SOURCE = "cur_api_source"  # Задаются в соответствии с обращением к серверу
    API_PARAM = "param_of_api"

    with open(FILE_NAME, 'r') as csvfile:
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

def load_json_to_raw_data_table(FILE_NAME = "reward-2017-02-01.csv"):   # Загружаем сырые данные из json-файла в raw_data
    API_METHOD = "get_json"
    API_SOURCE = "cur_api_source"   # Задаются в соответствии с обращением к серверу
    API_PARAM = "param_of_api"

    with open(FILE_NAME) as json_file:
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
                

def get_file_from_server():  # Загрузка файла с сервера
    import urllib
    import mimetypes
    import os

    download_link = 'https://d3c33hcgiwev3.cloudfront.net/_3afd3252820d8a4e1c5c9148bb0ec3a5_bike_sharing_demand.csv' + \
    '?Expires=1511827200&Signature=Y-LnAd6EJ6pr~YZeESIFngvID9vIsivzhWmibHJVJRenZNkYqjdJrlJgvyJbz~Oi9FubfboAbpth4E7CI3ll' + \
    'irXjQZBNMBdyt9d3WgQ355MjjQIDKu8yRT4NW5YbAIhV~HZUKLlmeRp1t2qET4bDb2yASN02OyZNt~aplhi3O44_&Key-Pair-Id=APKAJLTNE6QMUY6HBC5A'
    source = urllib.request.urlopen(download_link)  # Ссылка на файл для примера, файл с Coursera 
    file_extension = mimetypes.guess_extension(source.info()['Content-Type'])  # Узнаем расширение

    current_directory = os.getcwd()
    file_path = current_directory + '/file' + file_extension  # Формирумем путь для файла
    urllib.request.urlretrieve(download_link, file_path)  # Сохраняем в текущую директорию с соответствующим расширением

    if file_extension == 'csv':
        load_csv_to_raw_data_table(file_path)  # В зависимости от расширения вызываем обработчик
    else:
        load_json_to_raw_data_table(file_path)
        
get_file_from_server()