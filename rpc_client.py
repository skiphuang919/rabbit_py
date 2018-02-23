import pika
import uuid
import sys


class RpcClient(object):
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

        self.channel = self.connection.channel()

        # create a single callback queue per client
        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue

        self.response = None
        self.corr_id = ''

        # subscribe the callback queue for receiving RPC response
        self.channel.basic_consume(self.on_response,
                                   no_ack=True,
                                   queue=self.callback_queue)

    def on_response(self, ch, method, props, body):
        """
        executed on every response
        check the msg from callback queue if it's the response match the request
        based on BasicProperties of correlation_id
        """
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n):
        self.corr_id = str(uuid.uuid4())

        # publish the request message, with two properties: 'reply_to' and 'correlation_id'.
        # 'reply_to' specify queue used for response msg
        self.channel.basic_publish(exchange='',
                                   routing_key='rpc_queue',
                                   properties=pika.BasicProperties(reply_to=self.callback_queue,
                                                                   correlation_id=self.corr_id,),
                                   body=str(n))

        # block here wait until the proper response arrives
        while self.response is None:
            self.connection.process_data_events()

        return self.response

    def request(self):
        n = int(sys.argv[1])
        print(" [x] Requesting fib({})".format(n))
        response = self.call(n)
        print(" [.] Got %r" % response)


if __name__ == '__main__':
    fc = RpcClient()
    fc.request()
