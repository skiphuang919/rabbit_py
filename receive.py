#!/usr/bin/env python
import pika


class Consumer(object):
    def __init__(self):
        # create a localhost connection obj that is pass to connection adapter
        self.con_obj = pika.ConnectionParameters(host='localhost')

        # create a instance of connection object
        self.connection = pika.BlockingConnection(self.con_obj)

        # create a new channel
        # that is to say create a new connection
        self.channel = self.connection.channel()

    @staticmethod
    def c1_callback(ch, method, properties, body):
        # declare callback function
        # it is called when consume msg
        print(" [x] Received %r" % body)

    def c1(self):
        # declare a 'hello' queue, make sure it exists
        # only one 'hello' queue will be created even you declare many times
        self.channel.queue_declare(queue='hello')

        # tell Rabbit that this particular callback function should receive messages
        # from our 'hello' queue
        # `no_ack` means tell the broker not to expect a response
        self.channel.basic_consume(self.c1_callback,
                                   queue='hello',
                                   no_ack=True)
        print(' [*] Waiting for messages. To exit press CTRL+C')

        # enter a never-ending loop that waits for data
        # runs callbacks whenever necessary
        self.channel.start_consuming()

if __name__ == '__main__':
    c = Consumer()
    c.c1()
