from kafka import KafkaConsumer
import logging
import argparse
import os


def run_job(broker):
    consumer = KafkaConsumer('example', bootstrap_servers=broker)
    for msg in consumer:
        print(str(msg.value, 'utf-8'))


def get_arg(env, default):
    return os.getenv(env) if os.getenv(env, '') is not '' else default


def parse_args(parser):
    args = parser.parse_args()
    args.brokers = get_arg('KAFKA_BROKERS', args.brokers)
    args.topic = get_arg('KAFKA_TOPIC', args.topic)
    args.conf = get_arg('PORT', args.port)
    return args


def main(args):
    logging.info('brokers={}'.format(args.brokers))
    logging.info('topic={}'.format(args.topic))
    logging.info('port={}'.format(args.port))
    run_job(args.brokers)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info('starting timeseries-mock consumer')
    parser = argparse.ArgumentParser(
        description='timeseries-mock consumer example for Kafka')
    parser.add_argument(
        '--brokers',
        help='The bootstrap servers, env variable KAFKA_BROKERS',
        default='localhost:9092')
    parser.add_argument(
        '--topic',
        help='Topic to publish to, env variable KAFKA_TOPIC',
        default='data')
    parser.add_argument(
        '--port',
        type=int,
        help='Web server port',
        default=8080)
    args = parse_args(parser)
    main(args)
    logging.info('exiting')
