import pika


def transaction(value):
    channel.basic_publish(exchange='', routing_key='queue2', body='transaction ' + str(value))
    channel.basic_publish(exchange='', routing_key='queue4', body='snapshot')


def end():
    for i in range(10):
        channel.basic_publish(exchange='', routing_key='queue' + str(i), body='quit')
    connection.close()


def callback(ch, method, _, body):
    print(body.decode('utf-8'))
    global flag
    flag += 1
    ch.basic_ack(delivery_tag=method.delivery_tag)
    if flag == 10:
        print('\nReal current state: 8')
        transaction(-2)
        channel.basic_publish(exchange='', routing_key='queue8', body='transaction 5')
        channel.basic_publish(exchange='', routing_key='queue6', body='transaction 1')
    if flag == 20:
        print('\nReal current state: 10')
        transaction(-4)
        channel.basic_publish(exchange='', routing_key='queue5', body='transaction 1')
        channel.basic_publish(exchange='', routing_key='queue1', body='transaction 5')
    if flag == 30:
        print('\nReal current state: 7')
        channel.basic_publish(exchange='', routing_key='queue0', body='transaction -3')
        channel.basic_publish(exchange='', routing_key='queue0', body='snapshot')
    if flag == 40:
        end()


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='queueservice', auto_delete=True)
channel.confirm_delivery()
channel.basic_consume(queue='queueservice', on_message_callback=callback)

flag = 0
channel.basic_publish(exchange='', routing_key='queue3', body='transaction 7')
print('Real current state: 4')
channel.basic_publish(exchange='', routing_key='queue7', body='transaction -3')
channel.basic_publish(exchange='', routing_key='queue9', body='snapshot')
channel.start_consuming()
