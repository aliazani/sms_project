# This is a sample configs file. rename it to `CONFIGS.py` and edit accordingly

API_KEY = 'put your API key from KaveNegar here'

# Mysql configs
MYSQL_HOST = ''
MYSQL_USERNAME = ''
MYSQL_PASSWORD = ''
MYSQL_DB = ''

# call back url from KaveNegar will look like
# /v1/CALL_BACK_TOKEN/process
CALL_BACK_TOKEN = ''

# login credentials
USERNAME = ''
PASSWORD = ''

# generate one strong secret key for flask.
SECRET_KEY = 'random long string with alphanumeric + #()*&'

# Do not change below unless you know what you are doing.
UPLOAD_FOLDER = ''
ALLOWED_EXTENSION = {'xlsx'}

# remote systems can call this program like
# /{REMOTE_CALL_API_KEY}/check_one_serial/<serial> and check one serial, returns back json
REMOTE_CALL_API_KEY = 'set_unguessable_remote_api_key_lkjdfljerlj3247LKJ'