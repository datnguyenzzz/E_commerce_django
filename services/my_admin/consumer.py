import pika, os, django

from APIKEY import CLOUD_AMQP_KEY

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "my_admin.settings")
django.setup()

from products.models import Products

params = pika.URLParameters(CLOUD_AMQP_KEY)
conn = pika.BlockingConnection(params)
channel = conn.channel()

admin_product_queue = "admin_product_queue"
channel.queue_declare(queue=admin_product_queue) 

#DEFINE 
USER_PRODUCT_FANOUT_EXCHANGE = 'user.products.fanout'
USER_PRODUCT_TOPIC_EXCHANGE = 'user.products.topic'

#fanout
channel.exchange_declare(exchange=USER_PRODUCT_FANOUT_EXCHANGE, exchange_type='fanout')
channel.queue_bind(exchange=USER_PRODUCT_FANOUT_EXCHANGE, queue=admin_product_queue)

#topic 
channel.exchange_declare(exchange=USER_PRODUCT_TOPIC_EXCHANGE, exchange_type='topic')
binding_key = 'users.#' 
channel.queue_bind(exchange=USER_PRODUCT_TOPIC_EXCHANGE, queue=admin_product_queue, routing_key=binding_key)

def callback(ch, method, properties, body):
    print('received in my admin')  
    if properties.content_type == 'product_create':
        pass 

    if properties.content_type == 'product_update':
        pass

    if properties.content_type == 'product_delete':
        pass

channel.basic_consume(queue=admin_product_queue, on_message_callback=callback, auto_ack=True)

print('started consuming')
channel.start_consuming()
channel.close() 

