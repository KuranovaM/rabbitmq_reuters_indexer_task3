import json
import sys

import pika

def wr(num, *mess):
    with open(f'comn_{num}.txt', 'a') as f:
        print(*mess, file=f)

def send_to(channel, queue, body, reply_to=None):
    properties = None
    if reply_to is not None:
        properties = pika.BasicProperties(reply_to=reply_to.encode())
    channel.basic_publish(exchange='', routing_key=queue.encode(),
                               properties=properties,
                               body=json.dumps(body).encode())

def work(self_num, keeper_num, mapper_count):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=f'to_manager_from_k')

    data = {}
    st = set()
    for i in range(mapper_count):
        with open(f'./storage_{keeper_num}/map_data{i}.json') as f:
            loc_data = json.load(f)
            for j in loc_data:
                if j not in data:
                    data[j] = [[], []]
                    # сколько раз надо добавлять номер хранителя? если один, то
                    # data[j] = [[], [keeper_num]]
                if j == 'cat':
                    wr(self_num, 'cat!!!', loc_data[j])
                if (j, loc_data[j][0], keeper_num) not in st:
                    data[j][0].append(loc_data[j][0])
                    # если без повторений то следующее закоментировать!
                    data[j][1].append(loc_data[j][1])
                    st.add((j, loc_data[j][0], loc_data[j][1]))
        wr(self_num, 'end file', f'./storage_{keeper_num}/map_data{i}.json')
    with open(f'./manager/comb_{self_num}.json', 'w') as f:
        f.write(json.dumps(data))

    send_to(channel, 'to_manager_from_k', {'mess': 'comd_end'})
    connection.close()
    wr(self_num, 'END')


if __name__ == '__main__':
    try:
        work(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))
    except Exception as e:
        wr(int(sys.argv[1]), 'ERROR', e)
