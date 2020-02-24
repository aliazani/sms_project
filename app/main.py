from flask import Flask

app = Flask(__name__)


@app.route('/')
def hello():
    return 'Hello World'


@app.route('/v1/get_sms')
def get_sms():
    pass


def send_sms():
    pass


def check_serial():
    pass


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
