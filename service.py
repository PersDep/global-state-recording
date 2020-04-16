import pika
import time


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.basic_publish(exchange='', routing_key='queue3', body='transaction 7')
channel.basic_publish(exchange='', routing_key='queue7', body='transaction -3')
channel.basic_publish(exchange='', routing_key='queue9', body='snapshot')
time.sleep(1)
channel.basic_publish(exchange='', routing_key='queue1', body='transaction 5')
channel.basic_publish(exchange='', routing_key='queue6', body='transaction 1')
channel.basic_publish(exchange='', routing_key='queue0', body='transaction -2')
channel.basic_publish(exchange='', routing_key='queue5', body='snapshot')
time.sleep(1)
channel.basic_publish(exchange='', routing_key='queue2', body='transaction -4')
channel.basic_publish(exchange='', routing_key='queue8', body='transaction 6')
channel.basic_publish(exchange='', routing_key='queue4', body='snapshot')
time.sleep(1)

for i in range(10):
    channel.basic_publish(exchange='', routing_key='queue' + str(i), body='quit')
connection.close()