import pika, os, django

from APIKEY import CLOUD_AMQP_KEY

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "my_admin.settings")
django.setup()

from products.models import Products

params = pika.URLParameters(CLOUD_AMQP_KEY)
conn = pika.BlockingConnection(params)
channel = conn.channel()

channel.exchange_declare(exchange='products', exchange_type='fanout')

admin_product_queue = "admin_product_queue"
channel.queue_declare(queue=admin_product_queue) 

channel.queue_bind(exchange='products', queue=admin_product_queue)


def callback(ch, method, properties, body):
    print('received in my admin')  
    print(body)
    print(properties)
    print(method.routing_key)

channel.basic_consume(queue=admin_product_queue, on_message_callback=callback, auto_ack=True)

print('started consuming')
channel.start_consuming()
channel.close() 

