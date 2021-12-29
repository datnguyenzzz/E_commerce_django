import pika, json

from APIKEY import CLOUD_AMQP_KEY

params = pika.URLParameters(CLOUD_AMQP_KEY)
conn = pika.BlockingConnection(params)
channel = conn.channel()

#DEFINE 
USER_PRODUCT_FANOUT_EXCHANGE = 'user.products.fanout'
USER_PRODUCT_TOPIC_EXCHANGE = 'user.products.topic'

#fanout
channel.exchange_declare(exchange=USER_PRODUCT_FANOUT_EXCHANGE, exchange_type='fanout')

#topic 
channel.exchange_declare(exchange=USER_PRODUCT_TOPIC_EXCHANGE, exchange_type='topic')

def default_publish(method, body):
    pass 

def topic_publish(method, body, routing_key):
    properties = pika.BasicProperties(method)
    channel.basic_publish(exchange=USER_PRODUCT_TOPIC_EXCHANGE, routing_key=routing_key, body=json.dumps(body), properties=properties)

def fanout_publish(method, body):
    properties = pika.BasicProperties(method) 
    channel.basic_publish(exchange=USER_PRODUCT_FANOUT_EXCHANGE, routing_key='', body=json.dumps(body), properties=properties) 

