from kafka import KafkaProducer

from time import sleep
import json
from json import dumps
import random

#Generar valores random
def generate_values():
    directions=['N','NW','W','SW','S','SE','E','NE']

    wind = random.choice(directions)

    temp = round(random.uniform(0,100.00),2)

    hum = int(random.uniform(0,100))

    data = {'temperatura': temp, 'humedad': hum, 'direccion_viento': wind}

    message = (json.dumps(data))
    
    return(message)


#Inicializar producer
producer = KafkaProducer(bootstrap_servers=['20.120.14.159:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))

#Ciclo envio de mensajes
while(True):
    data = generate_values()
    producer.send('18040', value=data)
    sleep(15)
