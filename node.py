import argparse
import pika
import sys
import time


class process:
    def __init__(self, args):
        self.id = args.id
        self.snapcnt = 0
        self.recorded = False
        self.amount = args.amount
        self.states = [[] for _ in range(self.amount)]
        self.records = [-1] * self.amount
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
        # print('Node', self.id, 'received', body.decode('utf-8'))
        getattr(self, body.split()[0].decode('utf-8'))(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def transaction(self, msg):
        message = msg.decode('utf-8').split(' ')
        if len(message) == 2:
            self.states[self.id].append(int(message[1]))
            self.send_all(msg + (' ' + str(self.id)).encode('utf-8'))
        if len(message) == 3:
            self.states[int(message[2])].append(int(message[1]))

    def snapshot(self, _):
        while self.recorded:
            time.sleep(0.01)
        self.recorded = True
        self.records[self.id] = len(self.states[self.id]) - 1
        self.send_all('snap ' + str(self.id))

    def snap(self, msg):
        channel = int(msg.decode('utf-8').split(' ')[1])
        self.snapcnt += 1
        if not self.recorded:
            self.recorded = True
            self.records[self.id] = len(self.states[self.id])
            self.records[channel] = len(self.states[channel])
            self.send_all(msg)
        if self.snapcnt == self.amount - 1:
            self.snapcnt = 0
            self.recorded = False
            print('Node', self.id, 'recorded local state', self.states[self.id][:self.records[self.id]])
            overall = 0
            for val in self.states[self.id][:self.records[self.id]]:
                overall += val
            for i in range(self.amount):
                if i != self.id:
                    print('Node', self.id, 'recorded channel', i, 'state', self.states[i][self.records[i]:])
                    for val in self.states[i][self.records[i]:]:
                        overall += val
            self.records = [-1] * self.amount
            print('Node', self.id, 'overall numeric state is', overall)

    def quit(self, _):
        print('Node', self.id, 'finished service!')
        sys.exit()


parser = argparse.ArgumentParser()
parser.add_argument('--id', default=0, type=int, help='node id')
parser.add_argument('--amount', default=1, type=int, help='nodes amount')
arguments = parser.parse_args()

process(arguments)
