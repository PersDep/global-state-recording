import argparse
import pika
import sys
import time


class process:
    def __init__(self, args):
        self.id = args.id
        self.snapped = 0
        self.recorded = False
        self.amount = args.amount
        self.data = 0
        self.pending = 0
        self.channel = pika.BlockingConnection(pika.ConnectionParameters(host='localhost')).channel()
        self.channel.queue_declare(queue='queue'+str(self.id), auto_delete=True)
        self.channel.confirm_delivery()
        print('Node', self.id, 'started')
        self.channel.basic_consume(queue='queue'+str(self.id), on_message_callback=self.callback)
        self.channel.start_consuming()

    def send(self, msg, target):
        self.channel.basic_publish(exchange='', routing_key='queue' + str(target), body=msg, mandatory=True)

    def send_all(self, msg):
        for i in range(self.amount):
            if i != self.id:
                self.send(msg, i)

    def callback(self, ch, method, _, body):
        print('Node', self.id, 'received', body.decode('utf-8'))
        getattr(self, body.split()[0].decode('utf-8'))(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def update(self, msg):
        message = msg.decode('utf-8').split(' ')
        self.data += int(message[1])

    def pupdate(self, msg):
        message = msg.decode('utf-8').split(' ')
        self.pending += int(message[1])

    def transaction(self, msg):
        message = msg.decode('utf-8').split(' ')
        if not self.recorded:
            self.data += int(message[1])
            self.send_all('update ' + message[1])
        else:
            self.pending += int(message[1])
            self.send_all('pupdate ' + message[1])

    def snapshot(self, _):
        while self.recorded:
            time.sleep(0.01)
        self.recorded = True
        self.send_all('snap')

    def snap(self, msg):
        self.snapped += 1
        if not self.recorded:
            self.recorded = True
            self.send_all(msg)
        if self.snapped == self.amount - 1:
            temp = self.data
            self.data += self.pending
            self.pending = 0
            self.snapped = 0
            self.recorded = False
            print('Node', self.id, 'recorded state', temp)
            self.send('Recorded state ' + str(temp) + ' on node ' + str(self.id), 'service')

    def quit(self, _):
        print('Node', self.id, 'finished service!')
        sys.exit()


parser = argparse.ArgumentParser()
parser.add_argument('--id', default=0, type=int, help='node id')
parser.add_argument('--amount', default=1, type=int, help='nodes amount')
arguments = parser.parse_args()

process(arguments)
