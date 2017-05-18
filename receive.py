#!/usr/bin/env python
import pika

# create a localhost connection obj that is pass to connection adapter
con_obj = pika.ConnectionParameters(host='localhost')

# create a instance of connection object
connection = pika.BlockingConnection(con_obj)

# create a new channel
# that is to say create a new connection
channel = connection.channel()

# declare a 'hello' queue, make sure it exists
# only one 'hello' queue will be created even you declare many times
channel.queue_declare(queue='hello')


# declare callback function
# it is called when receive msg
def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)

# tell Rabbit that this particular callback function should receive messages
# from our 'hello' queue
# `no_ack` means tell the broker not to expect a response
channel.basic_consume(callback,
                      queue='hello',
                      no_ack=True)
print(' [*] Waiting for messages. To exit press CTRL+C')

# enter a never-ending loop that waits for data
# runs callbacks whenever necessary
channel.start_consuming()
