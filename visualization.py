import oauth2 as oauth
import json
import requests
import time
import plotly
import plotly.plotly as py
import plotly.tools as tls
import plotly.graph_objs as go

CONSUMER_KEY = "vuTttJnVDKh3Pwx2QY8Xm8Npq"
CONSUMER_SECRET = "FSkvbMs71bZp8Xu5fpuJWvCqexxmTPLrMgUh6BJOELm9Xkkd1C"
ACCESS_KEY = "2600303065-7GWZTmS2721pTxbBdqh9jPK39kjVw01693d3Erv"
ACCESS_SECRET = "3gMC0HyAfPA3TpJSXXZ6h2dZFnaddIMvFaKmRiQQqFuCP"

try:
    consumer = oauth.Consumer(key=CONSUMER_KEY, secret=CONSUMER_SECRET)  # Логинимся в twitter api с помощью OAuth
    access_token = oauth.Token(key=ACCESS_KEY, secret=ACCESS_SECRET)
    client = oauth.Client(consumer, access_token)
except:
    print("OAuth error")

plotly.tools.set_credentials_file(username='coolshuma', api_key='zqejELjKGdYv1MmqkUmF')
token = "khf9jt6htc"  # Настраиваем stream в plotly
stream_id = dict(token=token, maxpoints=3)
try:
    s = py.Stream(stream_id=token)
except:
    print("Plotly connect error")

def show_coordinates(coord, text):  # Функция для отображения координат на карте в dashboard
    try:
        s.open()
    except:
        print("Plotly stream error")
        return

    s.write(dict(
        lon=[coord[0]],
        lat=[coord[1]], 
        type='scattergeo',
        marker={'size': [10], 'sizemode': 'area',  # Отображаем текущую координату и текст твитта с ней
        'color': ["red"],
        'symbol': ["x-open"]},
        text=[text])
        )
    s.heartbeat()
    time.sleep(1)  # Ждем секунду
    s.write(dict(
        lon=[],
        lat=[], 
        type='scattergeo',
        marker={'size': [10], 'sizemode': 'area',  # Убираем координату
        'color': ["red"],
        'symbol': ["x-open"]},
        text=[])
        )
    s.heartbeat()
    s.close()

def get_coordinates_and_texts(tweets, already_showed):  # Получаем из данных координаты и тексты твиттов с геолокацией
    coordinates_to_show = []                            # При этом учитываем, не были ли они уже отображены 
    ids = []                                            # А также записывем полученные в отображенные, далее будем их отображать
    texts = []
    for tweet in tweets['statuses']:
        if ((tweet['coordinates'] != None) or (tweet['place'] != None) or (tweet['geo'] != None)):
            coordinates = tweet['coordinates']
            if (tweet['id'] in already_showed):
                continue
            already_showed.add(tweet['id'])
            
            texts.append(tweet['text'])
            
            if coordinates != None:
                coordinates_to_show.append(coordinates['coordinates'])
                continue
                
            place = tweet['place']
            if place != None:
                coordinates_to_show.append(place['bounding_box']['coordinates'][0][0])
                continue
                
            geo = tweet['geo']
            if geo != None:
                coordinates_to_show.append(geo['coordinates'])
                continue
                
    return coordinates_to_show, texts

def add_already_exist_tweets(already_showed):  # Добавляем твиты, которые существовали уже до начала работы скрипта в set,
    try:                                       # в который записываем уже отображенные твиты
        timeline_endpoint = "https://api.twitter.com/1.1/search/tweets.json?q=%23" + \
        "trip&count=100"
        response, data = client.request(timeline_endpoint)
    except:
        print("Request to API error")

    try:
        tweets = json.loads(data) 
    except:
        print("JSON parser error")
    
    coordinates, texts = get_coordinates_and_texts(tweets, already_showed)
    return


already_showed = set()
add_already_exist_tweets(already_showed)

while True:
    try:
        timeline_endpoint = "https://api.twitter.com/1.1/search/tweets.json?q=%23" + \
        "gameinsight&count=60"  # Делаем очередной запрос
        response, data = client.request(timeline_endpoint)
    except:
        print("Request to API error")

    try:
        tweets = json.loads(data)
    except:
        print("JSON parser error")
    
    coordinates, texts = get_coordinates_and_texts(tweets, already_showed)  # Передаем полученные данные в функцию для
    if coordinates:                                                         # получения координат и текстов
        for coord, text in zip(coordinates, texts):
            show_coordinates(coord, text)          # Отображаем полученные координаты, если они есть
    time.sleep(30)  # Ждем 30 секунд до следующего запроса