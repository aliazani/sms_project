from flask import Flask, jsonify, request
from pandas import read_excel
import requests
import sqlite3
import kave_negar

app = Flask(__name__)


@app.route('/')
def hello():
    return 'Hello World'


def import_database_from_excel(file_path):
    """
    Get an excel file name and import lookup data (data and failure) from it.
    The first(0) sheet contains serials data like:
     Row    Reference Number    Description Start serial    End serial  Date
    And the 2nd(1) contains a column of invalid serials.
    This data will be written into the sqlite database located at kave_negar.DATA_BASE_FILE_PATH
    in two tables "serials" , "invalids".

    :param file_path: The path of excel file is in kave_negar.DATA_BASE_FILE_PATH
    :return: Two integers (number of serial rows, number of invalid rows)
    """
    conn = sqlite3.connect(kave_negar.DATA_BASE_FILE_PATH)
    cur = conn.cursor()
    # valid serials
    cur.execute('DROP TABLE IF EXISTS serials')

    cur.execute('''
    CREATE TABLE  IF EXISTS serials (
    id INTEGER PRIMARY KEY,
    reference TEXT ,
    description TEXT ,
    start_serial TEXT ,
    end_serial TEXT,
    date DATE);''')
    conn.commit()

    serials_counter = 0
    data_frame = read_excel(file_path, 0)

    for index, (row, reference_number, description, start_serial, end_serial, date) in data_frame.iterrows():
        if serials_counter % 10:
            query = f'INSERT INTO serials VALUES ' \
                    f'("{row}", "{reference_number}", "{description}", "{start_serial}", "{end_serial}", "{date}");'
            cur.execute(query)
            conn.commit()
        serials_counter += 1
    conn.commit()
    # Failure serials
    data_frame = read_excel(file_path, 1)  # This contains fail serial numbers.
    invalid_counter = 0

    cur.execute('DROP TABLE IF EXISTS invalids')

    cur.execute('''
    CREATE TABLE  IF EXISTS invalids (
        invalid_serial TEXT PRIMARY KEY);''')
    conn.commit()

    for index, failed_serial in data_frame.iterrows():
        fail_serial_row = failed_serial[0]
        if invalid_counter % 10:
            query = f'INSERT INTO invalids VALUES ("{fail_serial_row}");'
            cur.execute(query)
            conn.commit()
        invalid_counter += 1
    conn.commit()
    return serials_counter, invalid_counter


def send_sms(receptor, message):
    """
    This function will get a MSISDN and a message then uses KaveNegar to send sms.
    :param receptor:MSISDN or phone number
    :param message:The message you want to send
    :return:
    """
    url = f'https://api.kavenegar.com/v1/{kave_negar.API_KEY}/sms/send.json'
    data = {'receptor': receptor,
            'message': message}
    response = requests.post(url, data)


@app.route('/v1/process', methods=['POST'])
def process():
    """
    This is a call back from KaveNegar that will get sender and message
     and will check if it is valid , then answers back.
    :return:
    """
    data = request.form
    sender = data['from']
    message = data['message']
    send_sms(sender, 'Hi' + message)
    return jsonify(data), 200


def check_serial():
    pass


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000, debug=True)
