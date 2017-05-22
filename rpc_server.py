#!/usr/bin/env python
import pika


class RpcServer(object):
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='rpc_queue')

    @staticmethod
    def fib(n):
        """
        generate the fibonacci list
        """
        if n == 0:
            return 0
        else:
            res = []
            a, b = 0, 1
            while a < n:
                res.append(a)
                a, b = b, a+b
            return res

    def _on_request(self, ch, method, props, body):
        """
        do the works when receive the remote call from client, and
        sent the response to the reply queue
        """
        num = int(body)
        print(" [.] fib(%s)" % num)
        response = self.fib(num)
        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id=props.correlation_id),
                         body=str(response))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def serve(self):
        """
        waiting for remote call
        """
        # load balance
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self._on_request, queue='rpc_queue')
        print(" [x] Awaiting RPC requests")
        self.channel.start_consuming()

if __name__ == '__main__':
    rs = RpcServer()
    rs.serve()

