import pika, os, django

from APIKEY import CLOUD_AMQP_KEY

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "user_view.settings")
django.setup()

from products.models import Products

params = pika.URLParameters(CLOUD_AMQP_KEY)
conn = pika.BlockingConnection(params)
channel = conn.channel()

#DEFINE 
USER_PRODUCT_FANOUT_EXCHANGE = 'user.products.fanout'
USER_PRODUCT_TOPIC_EXCHANGE = 'user.products.topic'
ADMIN_PRODUCT_FANOUT_EXCHANGE = 'admin.products.fanout'
ADMIN_PRODUCT_TOPIC_EXCHANGE = 'admin.products.topic'

user_view_product_queue = "user_view_product_queue"
channel.queue_declare(queue=user_view_product_queue) 

#fanout
channel.exchange_declare(exchange=USER_PRODUCT_FANOUT_EXCHANGE, exchange_type='fanout')
channel.queue_bind(exchange=USER_PRODUCT_FANOUT_EXCHANGE, queue=user_view_product_queue)
channel.exchange_declare(exchange=ADMIN_PRODUCT_FANOUT_EXCHANGE, exchange_type='fanout')
channel.queue_bind(exchange=ADMIN_PRODUCT_FANOUT_EXCHANGE, queue=user_view_product_queue)

#topic 
channel.exchange_declare(exchange=USER_PRODUCT_TOPIC_EXCHANGE, exchange_type='topic')
binding_key = '*.product.#' 
channel.queue_bind(exchange=USER_PRODUCT_TOPIC_EXCHANGE, queue=user_view_product_queue, routing_key=binding_key)

def callback(ch, method, properties, body):
    print('received in my user_view')  
    print(body)
    print(properties)
    print(method.routing_key)

channel.basic_consume(queue=user_view_product_queue, on_message_callback=callback, auto_ack=True)

print('started consuming')
channel.start_consuming()
channel.close() 

