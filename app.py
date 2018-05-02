from flask import Flask, render_template
from flask_socketio import SocketIO
import threading
from kafka import KafkaConsumer

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)

UPDATE_EVENT = 'my event'
thread = None


def run_job():
    consumer = KafkaConsumer('bones-brigade', bootstrap_servers='kafka:9092')
    for msg in consumer:
        print('Doing something imporant in the background')
        socketio.emit(UPDATE_EVENT, str(msg.value, 'utf-8'))


@app.route('/')
def hello_world():
    global thread
    if not thread:
        thread = threading.Thread(target=run_job)
        thread.start()

    return render_template('index.html')


@socketio.on(UPDATE_EVENT)
def handle_my_custom_event(json):
    print('received json: ' + str(json))


if __name__ == '__main__':
    RUNNING = False
    socketio.run(app)
