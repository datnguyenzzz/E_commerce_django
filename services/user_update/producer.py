import pika, json

from APIKEY import CLOUD_AMQP_KEY

params = pika.URLParameters(CLOUD_AMQP_KEY)
conn = pika.BlockingConnection(params)
channel = conn.channel()

#fanout
#channel.exchange_declare(exchange='products', exchange_type='fanout')

#topic 
channel.exchange_declare(exchange='products', exchange_type='topic')

def default_publish(method, body):
    pass 

def topic_publish(method, body, routing_key):
    properties = pika.BasicProperties(method)
    channel.basic_publish(exchange='products', routing_key=routing_key, body=json.dumps(body), properties=properties)

def fanout_publish(method, body):
    properties = pika.BasicProperties(method) 
    channel.basic_publish(exchange='products', routing_key='', body=json.dumps(body), properties=properties) 

