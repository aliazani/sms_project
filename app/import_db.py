import os
import sys
import re
import pymysql
import configs
from pandas import read_excel

MAX_FLASH = 100


def get_database_connection():
    """connects to the MySQL database and returns the connection"""
    return pymysql.connect(host=configs.MYSQL_HOST,
                           user=configs.MYSQL_USERNAME,
                           password=configs.MYSQL_PASSWORD,
                           db=configs.MYSQL_DB,
                           charset='utf8')


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
    # Create Database and connect to it.
    db = get_database_connection()
    cur = db.cursor()
    output = []
    total_flashes = 0

    try:
        # Create logs table
        cur.execute('DROP TABLE IF EXISTS logs;')

        cur.execute('''
        CREATE TABLE logs(
        log_name CHAR(200),
        log_value MEDIUMTEXT);''')
        db.commit()
    except Exception as error:
        print('dropping logs.')
        output.append(f'Problem dropping and creating new table for logs in database; {error}')

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
        print("problem dropping serials")
        output.append(f'Problem dropping and creating new table serials in database; {error}')

    cur.execute("INSERT INTO logs VALUES ('db_filename', %s)", (file_path,))
    db.commit()

    # remove the invalid table if exists, then create the new one
    try:
        cur.execute('DROP TABLE IF EXISTS invalids;')
        cur.execute("""CREATE TABLE invalids (
            invalid_serial CHAR(30), INDEX(invalid_serial));""")
        db.commit()
    except Exception as error:
        output.append(f'Error dropping and creating INVALIDS table; {error}')

    # insert some place holder logs
    cur.execute("INSERT INTO logs VALUES ('import', %s)",
                ('Import started ... logs will appear when its done',))
    cur.execute("INSERT INTO logs VALUES ('db_check', %s)", ('DB check will be run after the insert is finished',))
    db.commit()

    # Insert Data into serials table
    data_frame = read_excel(file_path, 0)
    serials_counter = 1
    line_number = 1
    for _, (row, reference_number, description, start_serial, end_serial, date) in data_frame.iterrows():
        line_number += 1
        if not reference_number or (reference_number != reference_number):
            reference_number = ""
        if not description or (description != description):
            description = ""
        if not date or (date != date):
            date = "7/2/12"
        try:
            start_serial = normalize_string(start_serial)
            end_serial = normalize_string(end_serial)
            query = 'INSERT INTO serials VALUES (%s, %s, %s, %s, %s, %s);'
            cur.execute(query, (row, reference_number, description, start_serial, end_serial, date))
            serials_counter += 1

        except Exception as error:
            total_flashes += 1
            if total_flashes < MAX_FLASH:
                output.append(
                    f'Error inserting line {line_number} from serials sheet SERIALS, {error}')
            elif total_flashes == MAX_FLASH:
                output.append(f'Too many errors!')

        if line_number % 1000 == 0:
            try:
                db.commit()
            except Exception as error:
                output.append(f'problem committing serials into db around {line_number} (or previous 20 ones); {error}')

    db.commit()

    # now lets save the invalid serials.
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
                output.append(
                    f'Error inserting line {line_number} from serials sheet INVALIDS, {e}')
            elif total_flashes == MAX_FLASH:
                output.append(f'Too many errors!')

        if line_number % 1000 == 0:
            try:
                db.commit()
            except Exception as e:
                output.append(f'problem committing invalid serials into db around {line_number}'
                              f' (or previous 20 ones); {e}')

    # save the logs
    output.append(f'Inserted {serials_counter} serials and {invalid_counter} invalids')
    output.reverse()
    cur.execute("UPDATE logs SET log_value = %s WHERE log_name = 'import'", ('\n'.join(output), ))
    db.commit()

    db.close()

    return


def db_check():
    """ will do some sanity checks on the db and will flash the errors """

    db = get_database_connection()
    cur = db.cursor()
    cur.execute("INSERT INTO logs VALUES('db_check', %s)",
                ("Database Check started ... wait for results. It may take a while",))
    db.commit()

    def collision(start_1, end_1, start_2, end_2):
        if start_2 <= start_1 <= end_2:
            return True
        if start_2 <= end_1 <= end_2:
            return True
        if start_1 <= start_2 <= end_1:
            return True
        if start_1 <= end_2 <= end_1:
            return True
        return False

    def separate(input_string):
        """ gets AA0000000000000000000000000090 and returns AA, 90 """
        digit_part = ''
        alpha_part = ''
        for character in input_string:
            if character.isalpha():
                alpha_part += character
            elif character.isdigit():
                digit_part += character
        return alpha_part, int(digit_part)

    cur.execute("SELECT id, start_serial, end_serial FROM serials")

    raw_data = cur.fetchall()
    all_problems = []
    data = {}

    for row in raw_data:
        id_row, start_serial, end_serial = row
        start_serial_alpha, start_serial_digit = separate(start_serial)
        end_serial_alpha, end_serial_digit = separate(end_serial)
        if start_serial_alpha != end_serial_alpha:
            all_problems.append(f'Start serial and end serial of row {id_row} start with different letters')

        else:
            if start_serial_alpha not in data:
                data[start_serial_alpha] = []
            data[start_serial_alpha].append((id_row, start_serial_digit, end_serial_digit))

    for letters in data:
        for i in range(len(data[letters])):
            for j in range(i + 1, len(data[letters])):
                id_row_1, start_serial_1, end_serial_1 = data[letters][i]
                id_row_2, start_serial_2, end_serial_2 = data[letters][j]
                if collision(start_serial_1, end_serial_1, start_serial_2, end_serial_2):
                    all_problems.append(f'there is a collision between row ids {id_row_1} and {id_row_2}')

    all_problems.reverse()
    output = '\n'.join(all_problems)

    cur.execute("UPDATE logs SET log_value = %s WHERE log_name = 'db_check'", (output,))
    db.commit()

    db.close()


file_path = sys.argv[1]

import_database_from_excel(file_path)
db_check()

os.remove(file_path)
