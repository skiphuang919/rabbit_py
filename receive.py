#!/usr/bin/env python
import pika
import time
import random


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
        """
        simply consumer that receive messages from a named queue.
        """

        # declare a 'hello' queue, make sure it exists
        # only one 'hello' queue will be created even you declare many times
        self.channel.queue_declare(queue='hello')

        # tell Rabbit that this particular callback function should receive messages
        # from our 'hello' queue
        # `no_ack=True` means turn off the msg ack
        self.channel.basic_consume(self.c1_callback,
                                   queue='hello',
                                   no_ack=True)
        print(' [*] Waiting for messages. To exit press CTRL+C')

        # enter a never-ending loop that waits for data
        # runs callbacks whenever necessary
        self.channel.start_consuming()

    @staticmethod
    def c2_callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
        # mock a time-consuming tasks
        cost = random.randint(1, 10)
        print (" [x] it will cost {}s".format(cost))
        time.sleep(cost)
        print(" [x] Done")

        # send a proper acknowledgment once we done with the task
        # An ack is sent back from the consumer to tell RabbitMQ that a particular msg had been received and processed
        # and that RabbitMQ is free to delete it
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def c2(self):
        # declare a 'task_queue' queue, create if need
        # `durable=True` make sure that RabbitMQ will never lose the queue even if RabbitMQ restarts
        self.channel.queue_declare(queue='task_queue', durable=True)

        print(' [*] Waiting for messages. To exit press CTRL+C')

        # tell rabbit not to give more than one msg to a work at a time
        # that means don't dispatch a new msg to a work until it has processed and acknowledged the previous one
        # instead, it will dispatch it to the next work that is not still busy
        self.channel.basic_qos(prefetch_count=1)

        # subscribing the callback function to a queue
        self.channel.basic_consume(self.c2_callback,
                                   queue='task_queue')

        # start the loop
        self.channel.start_consuming()


if __name__ == '__main__':
    c = Consumer()
    c.c2()
