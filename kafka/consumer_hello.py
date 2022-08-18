from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    consumer = Consumer(config)

    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    topic = 'purchases'
    consumer.subscribe([topic], on_assign=reset_offset)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print('Waiting...')
            elif msg.error():
                print("ERROR %s".format(msg.error()))
            else:
                topic = msg.topic()
                key = msg.key()
                val = msg.value().decode('utf-8')
                print(f"Event from {topic}: key {key} val {val}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.stop()


