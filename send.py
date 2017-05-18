#!/usr/bin/env python
import pika

# create a localhost connection obj that is pass to connection adapter
con_obj = pika.ConnectionParameters(host='localhost')

# create a instance of connection object
connection = pika.BlockingConnection(con_obj)

# create a new channel
# that is to say create a new connection
channel = connection.channel()

# declare a 'hello' queue, create if needed
channel.queue_declare(queue='hello')

# publish msg to the channel with the default exchange
# cause msg can never be sent directly to queue it always need go through an exchange
# default exchange is a direct exchange with no name (empty str)
# within the default exchange situation the routing_key is specified by the queue name
# in order to tell the msg which queue it should go
channel.basic_publish(exchange='',
                      routing_key='hello',
                      body='Hello World!')
print(" [x] Sent 'Hello World!'")

# disconnect from rabbit
connection.close()
