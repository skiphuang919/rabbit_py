#!/usr/bin/env python
import pika
import random


class Producer(object):
    """
    obj to produce msg
    """
    def __init__(self):
        # create a localhost connection obj that is pass to connection adapter
        self.con_obj = pika.ConnectionParameters(host='localhost')

        # create a instance of connection object
        self.connection = pika.BlockingConnection(self.con_obj)

        # create a new channel
        # we can treat a channel as a light connection which sharing the same one TCP connection
        self.channel = self.connection.channel()

    def p1(self):
        """
        simply send messages to a named queue producer.
        """

        # declare a 'hello' queue, create if needed
        self.channel.queue_declare(queue='hello')

        # publish msg to the channel with the default exchange
        # cause msg can never be sent directly to queue it always need go through an exchange
        # default exchange is a direct exchange with no name (empty str)
        # within the default exchange situation the routing_key is specified by the queue name
        # in order to tell the msg which queue it should go
        self.channel.basic_publish(exchange='',
                                   routing_key='hello',
                                   body='Hello World!')
        print(" [x] Sent 'Hello World!'")

        # disconnect from rabbit
        self.connection.close()

    def p2(self):
        """
        distribute time-consuming tasks among multiple workers.
        """
        # declare a 'task_queue' queue, create if need
        # `durable=True` make sure that RabbitMQ will never lose the queue even if RabbitMQ restarts
        self.channel.queue_declare(queue='task_queue', durable=True)

        # generate msg with random int between 0 to 100
        message = "Hello World! tag={}".format(random.randint(0, 100))

        # publish msg to the channel with the default exchange
        # mark msg as persistent by supplying a `delivery_mode` property with a value 2.
        # Rabbit doesn't do `fsync(2)` for every message
        # it may be just saved to cache and not really written to the disk
        # although it tells RabbitMQ to save the message to disk
        self.channel.basic_publish(exchange='',
                                   routing_key='task_queue',
                                   body=message,
                                   properties=pika.BasicProperties(delivery_mode=2,)  # make message persistent
                                   )
        print(" [x] Sent %r" % message)
        self.connection.close()

    def p3(self):
        """
        publish msg to multi subscriber
        """

        # creates an 'fanout' type exchange named 'logs' if it does not already exist
        # if the exchange exists, verifies that it is of the correct and expected
        # 'fanout' type exchange broadcast all the msg it receive to all the queue it knows
        self.channel.exchange_declare(exchange='logs',
                                      exchange_type='fanout')

        message = "Hello World! tag={}".format(random.randint(0, 100))

        # publish msg to the named exchange 'logs'
        self.channel.basic_publish(exchange='logs',
                                   routing_key='',
                                   body=message)
        print(" [x] Sent %r" % message)
        self.connection.close()

    def p4(self):
        """
        distribute msg through exchange routing_key
        """
        # declare an direct exchange, create if need
        self.channel.exchange_declare(exchange='direct_logs',
                                      exchange_type='direct')

        severity = random.choice(['debug', 'info', 'error', 'warning'])
        message = "<{}> Hello World! tag={}".format(severity, random.randint(0, 100))

        # publish msg to the named exchange 'direct_logs'
        # bind the severity to the routing_key
        self.channel.basic_publish(exchange='direct_logs',
                                   routing_key=severity,
                                   body=message)
        print(" [x] Sent %r" % message)
        self.connection.close()


if __name__ == '__main__':
    s = Producer()
    s.p4()
