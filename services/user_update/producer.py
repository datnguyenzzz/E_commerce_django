import pika, json

from APIKEY import CLOUD_AMQP_KEY

params = pika.URLParameters(CLOUD_AMQP_KEY)
conn = pika.BlockingConnection(params)
channel = conn.channel()

#topic exchange
channel.exchange_declare(exchange='topic_products', exchange_type='topic')
#fanout exchange 
# exchange_type = 'fanout'

def default_publish(method, body):
    pass 

def topic_publish(method, body):
    properties = pika.BasicProperties(method) 
    routing_key = "products.*"
    channel.basic_publish(exchange='topic_products', routing_key=routing_key
                        , body=json.dump(body), properties=properties)

def fanout_publish(method, body):
    pass 

