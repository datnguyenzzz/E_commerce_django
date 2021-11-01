import pika, os

from APIKEY import CLOUD_AMQP_KEY

params = pika.URLParameters(CLOUD_AMQP_KEY)
conn = pika.BlockingConnection(params)
channel = conn.channel()

user_update_product_queue = "user_update_product_queue"
channel.queue_declare(queue=user_update_product_queue) 

#DEFINE 
ADMIN_PRODUCT_FANOUT_EXCHANGE = 'admin.products.fanout'
ADMIN_PRODUCT_TOPIC_EXCHANGE = 'admin.products.topic'

#fanout
channel.exchange_declare(exchange=ADMIN_PRODUCT_FANOUT_EXCHANGE, exchange_type='fanout')
channel.queue_bind(exchange=ADMIN_PRODUCT_FANOUT_EXCHANGE, queue=user_update_product_queue)

#topic 
channel.exchange_declare(exchange=ADMIN_PRODUCT_TOPIC_EXCHANGE, exchange_type='topic')
binding_key = 'admin.product.#' 
channel.queue_bind(exchange=ADMIN_PRODUCT_TOPIC_EXCHANGE, queue=user_update_product_queue, routing_key=binding_key)

def callback(ch, method, properties, body):
    print('received in user update')  
    if properties.content_type == 'product_create':
        pass 

    if properties.content_type == 'product_update':
        pass

    if properties.content_type == 'product_delete':
        pass

channel.basic_consume(queue=user_update_product_queue, on_message_callback=callback, auto_ack=True)

print('started consuming')
channel.start_consuming()
channel.close() 

