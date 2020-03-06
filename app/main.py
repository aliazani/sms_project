import datetime
import requests
import re
import os
import time
from flask import Flask, jsonify, flash, request, Response, redirect, url_for, session, render_template, abort
from flask_login import LoginManager, UserMixin, login_required, login_user, logout_user, current_user
from werkzeug.utils import secure_filename
from pandas import read_excel
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import pymysql
import kave_negar

app = Flask(__name__)

MAX_FLASH = 10

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
login_manager.login_message_category = 'danger'


class User(UserMixin):
    def __init__(self, id):
        self.id = id

    def __repr__(self):
        return "%d" % self.id


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
        if password == kave_negar.PASSWORD and username == kave_negar.USERNAME:
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


@app.route('v1/ok')
def health_check():
    """
    This function is for saying every thing is fine.
    :return:The message that said every thing is fine
    """
    return_message = {'message': 'ok'}
    return jsonify(return_message)


def get_database_connection():
    """connects to the MySQL database and returns the connection"""
    return pymysql.connect(host=kave_negar.MYSQL_HOST,
                           user=kave_negar.MYSQL_USERNAME,
                           password=kave_negar.MYSQL_PASSWORD,
                           db=kave_negar.MYSQL_DB,
                           charset='utf8')


def import_database_from_excel(file_path):
    """
    Get an excel file name and import lookup data (data and failure) from it.
    The first(0) sheet contains serials data like:
     Row    Reference Number    Description Start serial    End serial  Date
    And the 2nd(1) contains a column of invalid serials.
    This data will be written into the Mysql_server database at Fandogh(Platform as service)
    in two tables "serials" , "invalids".

    :return: Two integers (number of serial rows, number of invalid rows)
    """
    # Create Database and connect to it.
    db = get_database_connection()
    cur = db.cursor()
    total_flashes = 0
    try:
        # valid serials
        # Create serials table
        cur.execute('DROP TABLE IF EXISTS serials;')

        cur.execute('''
        CREATE TABLE serials (
        id INTEGER PRIMARY KEY,
        reference VARCHAR(200) ,
        description  VARCHAR(200) ,
        start_serial  CHAR(30) ,
        end_serial  CHAR(30),
        date DATETIME, INDEX(start_serial, end_serial));''')
        db.commit()
    except Exception as error:
        flash(f'Error dropping and creating SERIALS table; {error}', 'danger')

    # Insert Data into serials table
    serials_counter = 1
    line_number = 1
    data_frame = read_excel(file_path, 0)
    for _, (row, reference_number, description, start_serial, end_serial, date) in data_frame.iterrows():
        line_number += 1
        try:
            start_serial = normalize_string(start_serial)
            end_serial = normalize_string(end_serial)
            query = 'INSERT INTO serials VALUES (%s, %s, %s, %s, %s, %s);'
            cur.execute(query, (row, reference_number, description, start_serial, end_serial, date))
            serials_counter += 1

        except Exception as error:
            total_flashes += 1
            if total_flashes < MAX_FLASH:
                flash(
                    f'Error inserting line {line_number} from serials sheet SERIALS, {error}',
                    'danger')
            elif total_flashes == MAX_FLASH:
                flash(f'Too many errors!', 'danger')

        if line_number % 1000 == 0:
            try:
                db.commit()
            except Exception as error:
                flash(f'problem committing serials into db around {line_number} (or previous 20 ones); {error}')

    db.commit()

    # now lets save the invalid serials.
    # remove the invalid table if exists, then create the new one

    try:
        cur.execute('DROP TABLE IF EXISTS invalids;')
        cur.execute('''
        CREATE TABLE invalids (
            invalid_serial CHAR(30), INDEX(invalid_serial));''')
        db.commit()
    except Exception as e:
        flash(f'Error dropping and creating INVALIDS table; {e}', 'danger')

    invalid_counter = 1
    line_number = 1
    data_frame = read_excel(file_path, 1)  # This contains fail serial numbers.
    # Insert Data into invalids table
    for _, (failed_serial,) in data_frame.iterrows():
        line_number += 1
        try:
            failed_serial = normalize_string(failed_serial)
            query = 'INSERT INTO invalids VALUES (%s);'
            cur.execute(query, (failed_serial,))
            invalid_counter += 1

        except Exception as e:
            total_flashes += 1
            if total_flashes < MAX_FLASH:
                flash(
                    f'Error inserting line {line_number} from serials sheet INVALIDS, {e}',
                    'danger')
            elif total_flashes == MAX_FLASH:
                flash(f'Too many errors!', 'danger')

        if line_number % 1000 == 0:
            try:
                db.commit()
            except Exception as e:
                flash(f'problem committing invalid serials into db around {line_number} (or previous 20 ones); {e}')

    db.commit()
    db.close()
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


@app.route(f'/v1/{kave_negar.CALL_BACK_TOKEN}/process', methods=['POST'])
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


@app.route('check_one_serial', methods=['POST'])
@login_required
def check_one_serial():
    """ to check whether a serial number is valid or not"""
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
            answer = f'''This "{original_serial}" serial number is not original product.'''
            return 'FAILURE', answer

        query = "SELECT * FROM serials WHERE start_serial start_serial <= %s AND end_serial <= %s;"
        results = cur.execute(query, (serial, serial))

        if results > 1:
            answer = f'''This "{original_serial}" is valid for more details please contact us.'''

            return 'DOUBLE', answer

        elif results == 1:

            ret = cur.fetchone()
            description = ret[2]
            reference_number = ret[1]
            date = ret[5].date()
            answer = f'''{original_serial}
            {reference_number}
            {description}
            Hologram date: {date}
            Genuine product '''
            return 'OK', answer

    answer = f'''This "{original_serial}" serial is not genuine.'''
    return 'NOT-FOUND', answer


if __name__ == '__main__':
    create_sms_table()
    app.run(host='0.0.0.0', port=3000, debug=True)
