from kafka import KafkaConsumer

import json
from json import loads
from datetime import datetime

import matplotlib.pyplot as plt



def get_values(message):
    values = message.value
    
    return(values)


consumer = KafkaConsumer('18040', bootstrap_servers=['20.120.14.159:9092'], auto_offset_reset='earliest', value_deserializer=lambda x: loads(x.decode('utf-8')))

temperaturas = []
humedades = []
direcciones = []
times = []

for message in consumer:
    values = get_values(message)

    data = json.loads(values)

    temperaturas.append(data['temperatura'])
    humedades.append(data['humedad'])
    direcciones.append(data['direccion_viento'])
    times.append(datetime.now().strftime('%H:%M:%S')+data['direccion_viento'])

    print(times)

    fig,ax=plt.subplots(nrows=1,ncols=2,figsize=(20,5)) 

    #manipulating the first Axes 
    ax[0].plot(times,humedades) 
    # ax[0].xticks(default_x_ticks, times)
    ax[0].set_xlabel('Tiempo') 
    ax[0].set_ylabel('Humedad') 
    ax[0].set_title('Humedad con el tiempo') 

    #manipulating the second Axes 
    ax[1].plot(times,temperaturas) 
    # ax[1].xticks(default_x_ticks, times)
    ax[1].set_xlabel('Tiempo') 
    ax[1].set_ylabel('Temperatura') 
    ax[1].set_title('Temperatura con el tiempo')

    plt.pause(1)


# plt.show()


