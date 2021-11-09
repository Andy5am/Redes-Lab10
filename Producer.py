'''
            Laboratorio 10
        Estacion Meteorologica

Creado por:

    Andy Castillo           18040
    Juan Fernando De Leon   17822

'''


from kafka import KafkaProducer

from time import sleep
import json
from json import dumps
import random

def generate_values(restrictions):
    '''Generar valores random'''

    directions=['N','NW','W','SW','S','SE','E','NE']

    if not restrictions:

        wind = random.choice(directions)

        temp = round(random.uniform(0,100.00),2)

        hum = int(random.uniform(0,100))

        data = {'temperatura': temp, 'humedad': hum, 'direccion_viento': wind}

        message = (json.dumps(data))
    else:

        #Rand Temp
        temp = round(random.uniform(0,100.00),2)

        # Humedad
        hum = int(random.uniform(0,100))

        # Wind Direction
        wind_direction = random.choice(directions)
        wind_direction = directions.index(wind_direction)

        print(type(temp))
        print(type(hum))
        print(type(wind_direction))
        message = '{}{}{}'.format(chr(int(temp)), chr(hum), chr(wind_direction))

        message = message.encode('ascii')

        print(message)
    
    return(message)



#Inicializar producer
# producer = KafkaProducer(bootstrap_servers=['20.120.14.159:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
producer = KafkaProducer(bootstrap_servers=['20.120.14.159:9092'])

#Ciclo envio de mensajes
while(True):
    data = generate_values(restrictions=True)
    producer.send('18040', value=data)
    sleep(15)
