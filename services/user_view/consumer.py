import pika, os, django, json, io

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "user_view.settings")
django.setup()

from rest_framework.parsers import JSONParser

from APIKEY import CLOUD_AMQP_KEY

from products.models import Products
from products.serializers import ProductSerializer

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
    print(properties.content_type)
    if properties.content_type == 'product_create':
        data_parse = json.loads(body)
        #Products.objects.create(**data_parse)
        serializer = ProductSerializer(data=data_parse) 
        serializer.is_valid(raise_exception=True) 
        serializer.save() 
        print('product created')

    if properties.content_type == 'product_update':
        data_parse = json.loads(body)
        id = int(data_parse.pop('id'))

        product = Products.products.get(id=id) 
        serializer = ProductSerializer(instance=product, data=data_parse)
        serializer.is_valid(raise_exception=True) 
        serializer.save() 

        print('product updated')

    if properties.content_type == 'product_delete':
        id = int(json.loads(body))
        product = Products.products.get(id=id) 
        product.delete() 

        print('product deleted')

channel.basic_consume(queue=user_view_product_queue, on_message_callback=callback, auto_ack=True)

print('started consuming')
channel.start_consuming()
channel.close() 

