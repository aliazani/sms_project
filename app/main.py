import datetime
import requests
import re
import os
import time
import pymysql
import subprocess
from flask import Flask, jsonify, flash, request, Response, redirect, url_for, session, render_template, abort
from flask_login import LoginManager, UserMixin, login_required, login_user, logout_user, current_user
from werkzeug.utils import secure_filename
from pandas import read_excel
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from textwrap import dedent
import configs
import import_db

app = Flask(__name__)

MAX_FLASH = 10

# Add Flask limiter
limiter = Limiter(app, key_func=get_remote_address)

# Upload file
upload_folder = configs.UPLOAD_FOLDER
allowed_extension = configs.ALLOWED_EXTENSION
app.config['UPLOAD_FOLDER'] = upload_folder

# Flask Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.login_message_category = 'danger'


def allowed_file(filename):
    """ checks the extension of the passed filename to be in the allowed extensions
        :param filename: full file name with extension
        :return : extension of file
    """
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extension


# config
app.config.update(SECRET_KEY=configs.SECRET_KEY)


class User(UserMixin):
    def __init__(self, id):
        self.id = id

    def __repr__(self):
        return "%d" % self.id


# Create some user with id
user = User(0)


def get_database_connection():
    """connects to the MySQL database and returns the connection"""
    return pymysql.connect(host=configs.MYSQL_HOST,
                           user=configs.MYSQL_USERNAME,
                           password=configs.MYSQL_PASSWORD,
                           db=configs.MYSQL_DB,
                           charset='utf8')


# Database status
@app.route('/db_status/', methods=['GET'])
@login_required
def db_status():
    """ show some status about the DB """
    db = get_database_connection()
    cur = db.cursor()
    # collect some stats for the GUI
    try:
        cur.execute("SELECT count(*) FROM serials;")
        num_serials = cur.fetchone()[0]
    except Exception as e:
        num_serials = f'Can not query serials count => {e}'

    try:
        cur.execute("SELECT count(*) FROM invalids;")
        num_invalids = cur.fetchone()[0]
    except Exception as e:
        num_invalids = f'Can not query invalid count => {e}'

    try:
        cur.execute("SELECT log_value FROM logs WHERE log_name = 'import'")
        log_import = cur.fetchone()[0]
    except Exception as e:
        log_import = f'Can not read import log results... yet => {e}'

    try:
        cur.execute("SELECT log_value FROM logs WHERE log_name = 'db_filename'")
        log_filename = cur.fetchone()[0]
    except Exception as e:
        log_filename = f'can not read db filename from database => {e}'

    try:
        cur.execute("SELECT log_value FROM logs WHERE log_name = 'db_check'")
        log_db_check = cur.fetchone()[0]
    except Exception as e:
        log_db_check = f'Can not read db_check logs... yet => {e}'

    return render_template('db_status.html', data={'serials': num_serials, 'invalids': num_invalids,
                                                   'log_import': log_import, 'log_db_check': log_db_check,
                                                   'log_filename': log_filename})


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
            filename.replace(' ', '_')  # no space in file names! because we will call them as command line arguments
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            subprocess.Popen(["python", "import_db.py", file_path])
            flash('File uploaded. will be imported soon. follow from DB status Page', 'info')
            return redirect('/')

    db = get_database_connection()
    cur = db.cursor()

    # get last 5000 sms
    cur.execute("SELECT * FROM PROCESSED_SMS ORDER BY date DESC LIMIT 5000")
    all_smss = cur.fetchall()
    smss = []
    for sms in all_smss:
        status, sender, message, answer, date = sms
        smss.append({'status': status, 'sender': sender, 'message': message, 'answer': answer, 'date': date})

    # collect some stats for the GUI
    cur.execute("SELECT count(*) FROM PROCESSED_SMS WHERE status = 'OK'")
    num_ok = cur.fetchone()[0]

    cur.execute("SELECT count(*) FROM PROCESSED_SMS WHERE status = 'FAILURE'")
    num_failure = cur.fetchone()[0]

    cur.execute("SELECT count(*) FROM PROCESSED_SMS WHERE status = 'DOUBLE'")
    num_double = cur.fetchone()[0]

    cur.execute("SELECT count(*) FROM PROCESSED_SMS WHERE status = 'NOT-FOUND'")
    num_not_found = cur.fetchone()[0]

    return render_template('index.html', data={'smss': smss, 'ok': num_ok, 'failure': num_failure, 'double': num_double,
                                               'not_found': num_not_found})


# Somewhere to login
@app.route('/login', methods=['GET', 'POST'])
@limiter.limit("5 per minute")
def login():
    """ user login: only for admin user (system has no other user than admin)
    Note: there is a 10 tries per minute limitation to admin login to avoid minimize password factoring"""
    if current_user.is_authenticated:
        return redirect('/')
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if password == configs.PASSWORD and username == configs.USERNAME:
            login_user(user)
            return redirect('/')
        else:
            return abort(401)
    else:
        return render_template('login.html')


# Somewhere to logout
@app.route('/logout')
@login_required
def logout():
    """ logs out of the admin user"""
    logout_user()
    flash('Logged out', 'success')
    return redirect('/login')


# Handle login failed
@app.errorhandler(401)
def unauthorized(error):
    """ handling login failures"""
    flash('Login problem', 'danger')
    return redirect('/login')


@app.errorhandler(404)
def page_not_found(error):
    """ returns 404 page"""
    return render_template('404.html'), 404


def create_sms_table():
    """Creates PROCESSED_SMS table on database if it's not exists."""
    db = get_database_connection()
    cur = db.cursor()

    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS PROCESSED_SMS (
            status ENUM('OK', 'FAILURE', 'DOUBLE', 'NOT-FOUND'),
            sender CHAR(20),
            message VARCHAR(400),
            answer VARCHAR(400),
            date DATETIME, INDEX(date, status));""")
        db.commit()

    except Exception as error:
        flash(f'Error creating PROCESSED_SMS table; {error}', 'danger')

    db.close()


# Callback to reload the user object
@login_manager.user_loader
def loader_user(user_id):
    """To load a user for flask login"""
    return User(user_id)


@app.route('/ok')
def health_check():
    """
    This function is for saying every thing is fine.
    :return:The message that said every thing is fine
    """
    return_message = {'message': 'ok'}
    return jsonify(return_message)


def get_database_connection():
    """connects to the MySQL database and returns the connection"""
    return pymysql.connect(host=configs.MYSQL_HOST,
                           user=configs.MYSQL_USERNAME,
                           password=configs.MYSQL_PASSWORD,
                           db=configs.MYSQL_DB,
                           charset='utf8')


def send_sms(receptor, message):
    """
    This function will get a MSISDN and a message then uses KaveNegar to send sms.
    :param receptor:MSISDN or phone number
    :param message:The message you want to send
    :return:
    """

    url = f'https://api.kavenegar.com/{configs.API_KEY}/sms/send.json'
    data = {'receptor': receptor,
            'message': message}

    response = requests.post(url, data)


def translate_numbers(current, new, string):
    """This function will replace another languages numerals to english numerals.
    :param current : another languages numerals
    :param new : english numerals
    :param string : will be replaced to english
    """
    translation_table = str.maketrans(current, new)
    return string.translate(translation_table)


def remove_non_alphanum_char(string):
    """This function will remove non alpha numeric characters.
    """
    return re.sub(r'\W+', '', string)


def normalize_string(serial_number, fixed_length=30):
    """
    This function will change all the letters to the upper and convert persian digits to english digits.
    :param serial_number: The string that is also the serial number
    :param fixed_length : This will fix the length of serial number
    :return: converted serial number
    """
    # remove any non-alphanumeric character
    serial_number = remove_non_alphanum_char(serial_number)
    serial_number = serial_number.upper()

    # replace persian and arabic numeric chars to standard format
    persian_numerals = '۱۲۳۴۵۶۷۸۹۰'
    arabic_numerals = '١٢٣٤٥٦٧٨٩٠'
    english_numerals = '1234567890'

    serial_number = translate_numbers(persian_numerals, english_numerals, serial_number)
    serial_number = translate_numbers(arabic_numerals, english_numerals, serial_number)
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


@app.route(f'/{configs.CALL_BACK_TOKEN}/process', methods=['POST'])
def process():
    """
    This is a call back from KaveNegar that will get sender and message
    and will check if it is valid , then answers back.
    This is secured by 'CALL_BACK_TOKEN' in order to avoid mal-intended calls
    :return:
    """
    data = request.form
    sender = data['from']
    message = data['message']
    status, answer = check_serial(message)

    db = get_database_connection()
    cur = db.cursor()
    now = time.strftime('%Y-%m-%d %H:%M:%S')
    cur.execute("INSERT INTO PROCESSED_SMS (status, sender, message, answer, date) VALUES (%s, %s, %s, %s, %s)",
                (status, sender, message, answer, now))
    db.commit()
    db.close()

    send_sms(sender, answer)
    return jsonify(data), 200


@app.route('/check_one_serial', methods=['POST'])
@login_required
def check_one_serial():
    """ to check whether a serial number is valid or not using api
    caller should use something like /ABCD-SECRET/check_one_serial/AA10000
    answer back json which is status = DOUBLE, FAILURE, OK, NOT-FOUND
    """
    serial_to_check = request.form["serial"]
    status, answer = check_serial(serial_to_check)
    flash(f'{status} - {answer}', 'info')

    return redirect('/')


def check_serial(serial):
    """
    This function will get one serial number and return appropriate answer to that, after consulting the database.
    :param serial: The serial number we want to check the validity.
    :return:The text that say us if the serial is valid or not.
    """
    original_serial = serial
    serial = normalize_string(serial)
    # Connect to database
    db = get_database_connection()
    with db.cursor() as cur:
        query = "SELECT * FROM invalids WHERE invalid_serial == %s;"
        results = cur.execute(query, (serial,))

        if results > 0:
            answer = dedent(f'''This "{original_serial}" serial number is not original product.''')
            return 'FAILURE', answer

        query = "SELECT * FROM serials WHERE start_serial start_serial <= %s AND end_serial <= %s;"
        results = cur.execute(query, (serial, serial))

        if results > 1:
            answer = dedent(f'''This "{original_serial}" is valid for more details please contact us.''')

            return 'DOUBLE', answer

        elif results == 1:

            ret = cur.fetchone()
            description = ret[2]
            reference_number = ret[1]
            date = ret[5].date()
            answer = dedent(f'''{original_serial}
            {reference_number}
            {description}
            Hologram date: {date}
            Genuine product ''')
            return 'OK', answer

    answer = dedent(f'''This "{original_serial}" serial is not genuine.''')
    return 'NOT-FOUND', answer


@app.route(f"/{configs.REMOTE_CALL_API_KEY}/check_one_serial/<serial>", methods=["GET"])
def check_one_serial_api(serial):
    """ to check whether a serial number is valid or not using api
    caller should use something like /ABCDSECRET/cehck_one_serial/AA10000
    answer back json which is status = DOUBLE, FAILURE, OK, NOT-FOUND
    """
    status, answer = check_serial(serial)
    ret = {'status': status, 'answer': answer}
    return jsonify(ret), 200


if __name__ == '__main__':
    create_sms_table()
    app.run(host='0.0.0.0', port=3000, debug=True)
