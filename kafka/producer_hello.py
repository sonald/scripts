from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    producer = Producer(config)

    count = 0
    topic = "purchases"
    user_ids = ['eabara', 'jsmith', 'sgarcia', 'jbernard', 'htanaka', 'awalther']
    products = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']

    def delivery_callback(err, msg):
        if err:
            print('error {}'.format(err))
        else:
            print("Event: {}".format(msg))

    for _ in range(10):
        uid = choice(user_ids)
        prod = choice(products)
        producer.produce(topic, prod, uid, on_delivery=delivery_callback)
        count += 1

    producer.poll(10000)
    producer.flush()
