from flask import Flask, jsonify, request
from pandas import read_excel
import requests
import kave_negar

app = Flask(__name__)


@app.route('/')
def hello():
    return 'Hello World'


def import_database_from_excel(file_path):
    """
    Get an excel file name and import lookup data (data and failure) from it.
    """
    data_frame = read_excel(file_path, 0)
    for index, (row, reference_number, description, start_serial, end_serial, date) in data_frame.iterrows():
        pass
    data_frame = read_excel(file_path, 1)  # This contains fail serial numbers.
    for index, failed_serial in data_frame.iterrows():
        fail_serial_row = failed_serial[0]


def send_sms(receptor, message):
    url = f'https://api.kavenegar.com/v1/{kave_negar.API_KEY}/sms/send.json'
    data = {'receptor': receptor,
            'message': message}
    response = requests.post(url, data)


@app.route('/v1/process', methods=['POST'])
def process():
    data = request.form
    sender = data['from']
    message = data['message']
    send_sms(sender, 'Hi' + message)
    return jsonify(data), 200


def check_serial():
    pass


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000, debug=True)
