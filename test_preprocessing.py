import psycopg2
import pandas as pd
import csv
import json

DBNAME = 'rawdata'  # ! Необходимо задать соответствующие данные
USERNAME = 'postgres'
HOSTNAME = 'localhost'
PASSWORD = '5300expres'

try:
    conn = psycopg2.connect(f"dbname={DBNAME} user={USERNAME} host={HOSTNAME} password={PASSWORD}")
    conn.autocommit = True  # Соединяемся с базой данных
except:
    print("I am unable to connect to the database")

cur = conn.cursor()


def create_table_for_json():  # Создаем таблицу для обработанного json
    try:
        cur.execute("CREATE TABLE data_method_input (\"user\" int, ts timestamptz, " + \
        "context jsonb, ip inet);")
    except:
        pass

def create_table_for_csv():  # Созадем таблицы для обработанного csv 
    try:
        cur.execute("CREATE TABLE data_method_reward_1 (\"user\" int, ts timestamptz, " + \
        "reward_id int, reward_money int);")
    except:
        pass
        
    try:
        cur.execute("CREATE TABLE data_method_reward_2 (\"user\" int, ts timestamptz, " + \
        "reward_id int, PRIMARY KEY(\"user\", reward_id));")
    except:
        pass

def create_error_table():  # Создаем таблицу для данных с ошибкой
    try:
        cur.execute("CREATE TABLE data_error (api_source varchar, api_method varchar, " + \
        "api_date timestamptz, result text, error_text varchar);")
    except:
        pass

def error_insert(data, err):  # Вставляем ошибочные данные и описание ошибки в таблицу для ошибочных данных
    SQL = "INSERT INTO data_error (api_source, api_method, api_date, result, error_text)" + \
        "VALUES ((%s), (%s), (%s), (%s), (%s));"
    data_for_sql = (data['api_source'], data['api_method'], data['api_date'], data['result'], err,)
    cur.execute(SQL, data_for_sql)
    return

def json_load_to_base(data):  # Загружаем конкретную строку данных в формате json в соответствующую таблицу
    err = ""
    try:
        parsed_result = json.loads(data['result'])
    except: 
        err = "JSON parser error" # Отлавливаем ошибку в случае парсинга json
        
    if err != "":
        error_insert(data, err)
        return

    SQL = "INSERT INTO data_method_input (\"user\", ts, context, ip)" + \
        "VALUES ((%s), to_timestamp(%s), (%s), (%s));"
    data_for_sql = (parsed_result['user'], parsed_result['ts'], 
                    json.dumps(parsed_result['context']), parsed_result['ip'],)
    try:
        cur.execute(SQL, data_for_sql) 
    except:
        error_insert(data, "Bad data error")  # Отлавливаем ошибку несоответствия данных формату таблицы 
        return

def csv_parser(data):  # Парсер csv в соответствии с тем, что в начале каждой записи запомненные колонки csv-файла
    split_result = data['result'].split(',')
    arr_size = len(split_result)
    parsed_result = dict()
    if arr_size % 2 != 0:
        raise NameError('BadData')
        
    for i in range(arr_size // 2):
        parsed_result[split_result[i]] = split_result[arr_size // 2 + i]
    
    return parsed_result

def csv_load_to_base(data):  # Загружаем конкретную строку данных в формате csv в соответствующие таблицы
    try:
        parsed_result = csv_parser(data)
    except:
        error_insert(data, "CSV parser error")  # Отлавливаем ошибку несоответвия данных формату csv из парсера
        return
    
    SQL = "INSERT INTO data_method_reward_1 (\"user\", ts, reward_id, reward_money)" + \
        "VALUES ((%s), to_timestamp(%s), (%s), (%s));"
    data_for_sql = (parsed_result['user'], parsed_result['ts'], 
                    parsed_result['reward_id'], parsed_result['reward_money'],)

    try:
        cur.execute(SQL, data_for_sql)
    except:
        error_insert(data, "Bad data error")   # Отлавливаем ошибку несоответствия данных формату таблицы 
        return
    
    SQL = "INSERT INTO data_method_reward_2 (\"user\", ts, reward_id)" + \
        "VALUES ((%s), to_timestamp(%s), (%s));"
    data_for_sql = (parsed_result['user'], parsed_result['ts'], 
                    parsed_result['reward_id'],)
    try:
        cur.execute(SQL, data_for_sql)
    except psycopg2.IntegrityError:
        error_insert(data, "Integrity error")  # Отлавливаем ошибку нарушения индексирования 
    except:
        error_insert(data, "Bad data error")   # Отлавливаем ошибку несоответствия данных формату таблицы 

def create_tables_for_processing():  # Создаем необходимые таблицы для обработки сырых данных
    create_error_table()
    create_table_for_json()
    create_table_for_csv()

def processing_raw_data_table():  # Построчно обрабатываем таблицу raw_data
    create_tables_for_processing()
    my_table = pd.read_sql("select * from raw_data", conn)
    for id, data in my_table.iterrows():
        if data["api_method"] == "get_json":  # В зависимости от типа данных вызываем соответствую функцию
            json_load_to_base(data)
        else:
            csv_load_to_base(data)

def check_parced_csv():  #  Функция для проверки вставленных csv данных
    my_table = pd.read_sql("select * from data_method_reward_1", conn)
    print(my_table.shape)

processing_raw_data_table()
check_parced_csv()