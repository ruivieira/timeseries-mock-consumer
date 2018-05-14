import json

from flask import Flask, render_template
from flask_socketio import SocketIO
import threading
from kafka import KafkaConsumer
import logging
import argparse
import os

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)

UPDATE_EVENT = 'my event'
thread = None


def run_job():
    consumer = KafkaConsumer('example', bootstrap_servers='localhost:9092')
    for msg in consumer:
        print(str(msg.value, 'utf-8'))
        # socketio.emit(UPDATE_EVENT, json.loads(str(msg.value, 'utf-8')))


@app.route('/')
def hello_world():
    global thread
    if not thread:
        thread = threading.Thread(target=run_job)
        thread.start()

    return render_template('index.html')


@socketio.on(UPDATE_EVENT)
def handle_my_custom_event(json):
    # print('received json: ' + str(json))
    pass

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
    thread = threading.Thread(target=run_job)
    thread.start()

    socketio.run(app, host='0.0.0.0', port=args.port)


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
