import json
import sys

import pika

class Mapper:
    def __init__(self, num, keeper_num, all_num):
        self.self_num = num
        self.keeper_num = keeper_num
        self.count = all_num

        self.data = {}

        self.connection = None
        self.channel = None

    def wr(self, *mess):
        with open(f'mapper{self.self_num}_k{self.keeper_num}_log.txt', 'a') as f:
            print(*mess, file=f)

    def start(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=f'to_manager_from_k')
        # self.channel.queue_declare(queue=f'to_mapper{self.self_num}_from_m')
        # self.channel.basic_consume(queue=f'to_keeper{self.num}_queue', on_message_callback=self.resp, auto_ack=True)

        # try:
        #     self.channel.start_consuming()
        # except Exception as e:
        #     self.wr("Завершение... ", e)
        # finally:
        #     self.connection.close()
        # self.wr('EXIT')

    def end(self):
        self.connection.close()
        self.wr('EXIT')


    def write_data(self):
        with open(f'./storage_{self.keeper_num}/map_data{self.self_num}.json', 'w') as f:
            f.write(json.dumps(self.data))
        self.data = {}

    def processing(self):
        f = open(f'./storage_{self.keeper_num}/data_keeper{self.keeper_num}.json', 'r')
        local_data = json.load(f)
        f.close()
        for i in range(self.self_num, len(local_data), self.count):
            string, file_num = local_data[i]
            words = string.lower().split() # повторения смотреть или нет?.. наверное нет...
            for j in words:
                self.data[j] = (file_num, self.keeper_num)
        self.write_data()
        self.send_to('to_manager_from_k', {'mess': 'map_end'})

        self.wr('END')

    def send_to(self, queue, body, reply_to=None):
        properties = None
        if reply_to is not None:
            properties = pika.BasicProperties(reply_to=reply_to.encode())
        self.channel.basic_publish(exchange='', routing_key=queue.encode(),
                                   properties=properties,
                                   body=json.dumps(body).encode())

    # def resp(self, ch, method, properties, body):
        # self.wr("RESP", body.decode())
        # message = json.loads(body.decode())
        # mess = message['mess']

if __name__ == '__main__':
    m = Mapper(sys.argv[1], sys.argv[2], sys.argv[3])
    try:
        m.processing()
    except Exception as e:
        m.wr('ERROR', e)
    m.end()
