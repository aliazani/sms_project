from flask import Flask, jsonify, flash, request, Response, redirect, url_for, session, abort
from flask_login import LoginManager, UserMixin, login_required, login_user, logout_user
from werkzeug.utils import secure_filename
from pandas import read_excel
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import requests
import sqlite3
import re
import os
import kave_negar

app = Flask(__name__)

# Add Flask limiter
limiter = Limiter(app, key_func=get_remote_address)

# Upload file
upload_folder = kave_negar.UPLOAD_FOLDER
allowed_extension = kave_negar.ALLOWED_EXTENSION
app.config['UPLOAD_FOLDER'] = upload_folder


def allowed_file(filename):
    """ checks the extension of the passed filename to be in the allowed extensions
        :param filename: full file name with extension
        :return : extension of file
    """
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extension


# config
app.config.update(SECRET_KEY=kave_negar.SECRET_KEY)

# Flask Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'Login'


class User(UserMixin):
    def __init__(self, identifier):
        self.identifier = identifier

    def __repr__(self):
        return "%d" % self.identifier


# Create some user with id
user = User(0)


# some protected url
@app.route('/', methods=['GET', 'POST'])
@login_required
def home():
    """ creates database if method is post otherwise shows the homepage with some stats
    see import_database_from_excel() for more details on database creation"""
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part', 'danger')
            return redirect(request.url)
        file = request.files['file']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file', 'danger')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            rows, failures = import_database_from_excel(file_path)
            flash(f'Imported {rows} rows of serials and {failures} rows of failure', 'success')
            os.remove(file_path)
            return redirect('/')


# Somewhere to login
@app.route('/login', methods=['GET', 'POST'])
@Limiter.limit('5 per minute')
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if password == kave_negar.PASSWORD and username == kave_negar.USERNAME:
            login_user(user)
            return redirect(request.args.get('next'))
        else:
            return abort(401)
    else:
        return Response('''
        <form action="" method="post">
            <p><input type=text name=username>
            <p><input type=password name=password>
            <p><input type=submit value=Login>
        </form>
        ''')


# Somewhere to logout
@app.route('/logout')
@login_required
def logout():
    logout_user()
    return Response('<>Logged out</p>')


# Handle login failed
@app.errorhandler(401)
def page_not_found(error):
    return Response('<p>Login failed</p>')


# Callback to reload the user object
@login_manager.user_loader
def loader_user(user_id):
    return User(user_id)


@app.route('v1/ok')
def health_check():
    """
    This function is for saying every thing is fine.
    :return:The message that said every thing is fine
    """
    return_message = {'message': 'ok'}
    return jsonify(return_message)


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
    # Create Database and connect to it.
    conn = sqlite3.connect(kave_negar.DATA_BASE_FILE_PATH)
    cur = conn.cursor()

    # valid serials
    # Create serials table
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

    # Insert Data into serials table
    serials_counter = 0
    data_frame = read_excel(file_path, 0)

    for index, (row, reference_number, description, start_serial, end_serial, date) in data_frame.iterrows():
        start_serial = normalize_string(start_serial)
        end_serial = normalize_string(end_serial)
        if serials_counter % 10 == 0:
            query = f'INSERT INTO serials VALUES ' \
                    f'("{row}", "{reference_number}", "{description}", "{start_serial}", "{end_serial}", "{date}");'
            cur.execute(query)
            conn.commit()
        serials_counter += 1
    conn.commit()

    # Failure serials
    data_frame = read_excel(file_path, 1)  # This contains fail serial numbers.
    # Create invalid table
    invalid_counter = 0
    cur.execute('DROP TABLE IF EXISTS invalids')

    cur.execute('''
    CREATE TABLE  IF EXISTS invalids (
        invalid_serial TEXT PRIMARY KEY);''')
    conn.commit()
    # Insert Data into invalids table
    for index, (failed_serial,) in data_frame.iterrows():
        if invalid_counter % 10 == 0:
            query = f'INSERT INTO invalids VALUES ("{failed_serial}");'
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


def normalize_string(serial_number, fixed_length=30):
    """
    This function will change all the letters to the upper and convert persian digits to english digits.
    :param serial_number: The string that is also the serial number
    :param fixed_length : This will fix the length of serial number
    :return: converted serial number
    """
    # remove any non-alphanumeric character
    serial_number = re.sub(r'\W+', '', serial_number)
    serial_number = serial_number.upper()

    # replace persian and arabic numeric chars to standard format
    from_persian_char = '۱۲۳۴۵۶۷۸۹۰'
    from_arabic_char = '١٢٣٤٥٦٧٨٩٠'
    to_char = '1234567890'
    for i in range(len(to_char)):
        serial_number = serial_number.replace(from_persian_char[i], to_char[i])
        serial_number = serial_number.replace(from_arabic_char[i], to_char[i])

    # separate the alphabetic and numeric part of the serial number
    all_alpha = ''
    all_digit = ''
    for character in serial_number:
        if character.isalpha():
            all_alpha += character
        elif character.isdigit():
            all_digit += character

    # add zeros between alphabetic and numeric parts to standardize the length of the serial number
    missing_zeros = fixed_length - len(all_alpha) - len(all_digit)
    serial_number = all_alpha + '0' * missing_zeros + all_digit
    return serial_number


@app.route('/v1/process', methods=['POST'])
def process():
    """
    This is a call back from KaveNegar that will get sender and message
     and will check if it is valid , then answers back.
    :return:
    """
    data = request.form
    sender = data['from']
    message = normalize_string(data['message'])
    answer = check_serial(message)
    send_sms(sender, answer)
    return jsonify(data), 200


def check_serial(serial):
    """
    This function will get one serial number and return appropriate answer to that, after consulting the database.
    :param serial: The serial number we want to check the validity.
    :return:The text that say us if the serial is valid or not.
    """
    # Connect to database
    conn = sqlite3.connect(kave_negar.DATA_BASE_FILE_PATH)
    cur = conn.cursor()
    query = f"SELECT * FROM invalids WHERE invalid_serial == '{serial}'"
    results = cur.execute(query)
    if len(results.fetchall()) > 0:
        return 'This is not original product.'
    query = f"SELECT * FROM serials WHERE start_serial start_serial <='{serial}' AND end_serial <= '{serial}'"
    results = cur.execute(query)
    if len(results.fetchall()) == 1:
        return 'I found your serial'

    return 'It was not in the db'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000, debug=True)
