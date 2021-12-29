import pika, json, os, django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "my_admin.settings")
django.setup()

from APIKEY import CLOUD_AMQP_KEY

params = pika.URLParameters(CLOUD_AMQP_KEY)
conn = pika.BlockingConnection(params)
channel = conn.channel()

#DEFINE 
ADMIN_PRODUCT_FANOUT_EXCHANGE = 'admin.products.fanout'
ADMIN_PRODUCT_TOPIC_EXCHANGE = 'admin.products.topic'

#fanout
channel.exchange_declare(exchange=ADMIN_PRODUCT_FANOUT_EXCHANGE, exchange_type='fanout')

#topic 
channel.exchange_declare(exchange=ADMIN_PRODUCT_TOPIC_EXCHANGE, exchange_type='topic')

def default_publish(method, body):
    pass 

def topic_publish(method, body, routing_key):
    properties = pika.BasicProperties(method)
    channel.basic_publish(exchange=ADMIN_PRODUCT_TOPIC_EXCHANGE, routing_key=routing_key, body=json.dumps(body), properties=properties)

def fanout_publish(method, body):
    properties = pika.BasicProperties(method) 
    channel.basic_publish(exchange=ADMIN_PRODUCT_FANOUT_EXCHANGE, routing_key='', body=json.dumps(body), properties=properties) 

