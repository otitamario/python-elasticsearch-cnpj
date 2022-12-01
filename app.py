from flask import Flask, render_template, url_for, request, session, redirect
import bcrypt
from dotenv import load_dotenv
import os
from pymongo import MongoClient


# Load env variables
load_dotenv()
#Database CSV config
dataset_csv = os.getenv("DATASET_CSV")
# Mongodb Config
mongo_username=os.getenv("MONGO_INITDB_ROOT_USERNAME")
mongo_password=os.getenv("MONGO_INITDB_ROOT_PASSWORD")
db_database = os.getenv("DB_USER_DATABASE")
db_user_collection = os.getenv("DB_USER_COLLECTION")



# Connect to MongoDB

client = MongoClient('mongodb://localhost:27017/',
                  username=mongo_username,
                 password=mongo_password)

db = client[db_database]
users_collection = db[db_user_collection]

app = Flask(__name__)


@app.route('/')
def index():
    if 'username' in session:
        #return 'You are logged in as ' + session['username']
        return render_template('dashboard.html',username=session['username'])

    return render_template('index.html')

@app.route('/login', methods=['POST'])
def login():
    users = users_collection
    login_user = users.find_one({'name' : request.form['username']})

    if login_user:
        if bcrypt.hashpw(request.form['pass'].encode('utf-8'), login_user['password'].encode('utf-8')) == login_user['password'].encode('utf-8'):
            session['username'] = request.form['username']
            return redirect(url_for('index'))

    return 'Invalid username/password combination'

@app.route('/register', methods=['POST', 'GET'])
def register():
    if request.method == 'POST':
        users = users_collection
        existing_user = users.find_one({'name' : request.form['username']})

        if existing_user is None:
            hashpass = bcrypt.hashpw(request.form['pass'].encode('utf-8'), bcrypt.gensalt())
            users.insert_one({'name' : request.form['username'], 'password' : hashpass})
            session['username'] = request.form['username']
            return redirect(url_for('index'))
        
        return 'That username already exists!'

    return render_template('register.html')

if __name__ == '__main__':
    app.secret_key = 'mysecret'
    app.run(debug=True)