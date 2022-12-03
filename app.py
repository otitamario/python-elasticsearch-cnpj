from flask import Flask, render_template, url_for, request, session, redirect
import bcrypt
from dotenv import load_dotenv
import os
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from datetime import datetime

# Load env variables
load_dotenv()
#Database CSV config
dataset_csv = os.getenv("DATASET_CSV")
# Mongodb Config
mongo_uri=os.getenv("MONGO_URI")
mongo_username=os.getenv("MONGO_INITDB_ROOT_USERNAME")
mongo_password=os.getenv("MONGO_INITDB_ROOT_PASSWORD")
db_database = os.getenv("DB_USER_DATABASE")
db_user_collection = os.getenv("DB_USER_COLLECTION")
db_search_collection = os.getenv("DB_SEARCH_COLLECTION")
# Elasticsearch Config
es_host = os.getenv("ELASTICSEARCH_URI")
es = Elasticsearch([es_host])
es_index = os.getenv("ELASTICSEARCH_INDEX")


app = Flask(__name__)


@app.route('/')
def index():
    if 'username' in session and session["username"] != None:
        #return 'You are logged in as ' + session['username']
        return render_template('search.html',username=session['username'])

    return render_template('index.html')

@app.route("/logout", methods=['POST'])
def logout():
    session["username"] = None
    return redirect("/")

@app.route('/login', methods=['POST'])
def login():
    users = users_collection
    login_user = users.find_one({'name' : request.form['username']})

    if login_user:
        if bcrypt.hashpw(request.form['pass'].encode('utf-8'), login_user['password']) == login_user['password']:
            session['username'] = request.form['username']
            return redirect(url_for('index'))

    return 'Invalid username/password combination'

@app.route('/register', methods=['POST', 'GET'])
def register():
    if request.method == 'POST':
        # Connect to MongoDB
        client = MongoClient(mongo_uri,username=mongo_username,password=mongo_password)
        db = client[db_database]
        users_collection = db[db_user_collection]
        users = users_collection
        existing_user = users.find_one({'name' : request.form['username']})

        if existing_user is None:
            hashpass = bcrypt.hashpw(request.form['pass'].encode('utf-8'), bcrypt.gensalt())
            users.insert_one({'name' : request.form['username'], 'password' : hashpass})
            client.close()
            session['username'] = request.form['username']
            return redirect(url_for('index'))
        
        return 'That username already exists!'

    return render_template('register.html')


#endpoint for search
@app.route('/search', methods=['GET', 'POST'])
def search():
    if 'username' in session:
        if request.method == "POST":
            cnpj = request.form['cnpj']
            razaosocial = request.form['razaosocial']
            endereco = request.form['endereco']
            telefone = request.form['telefone']
            must = []
            registry = {"registration_date":None,"request":{}} 
            if len(cnpj)>0:
                must.append({"match": {"CNPJ BÁSICO": cnpj}})
                registry["request"]["cnpj"] = cnpj
            if len(razaosocial)>0:
                must.append({"match": {"NOME FANTASIA": razaosocial}})
                registry["request"]["razaosocial"] = razaosocial
            if len(endereco)>0:
                registry["request"]["endereco"] = endereco
                must.append({"query_string": {
                            "query": endereco,
                            "fields": [
                                "LOGRADOURO",
                                "BAIRRO",
                                "MUNICÍPIO"
                            ]
                                            }
                            })
            if len(telefone)>0:
                registry["request"]["telefone"] = telefone
                must.append({"match": {"TELEFONE 1": telefone}})
                
                

            query_body = {
                "bool": {
                    "must": must
                    }
            }
            res = es.search(index=es_index, query=query_body)
            data = res["hits"]["hits"]
            registry["data"]=data
            registry["registration_date"]=datetime.today().replace(microsecond=0)
            # Connect to MongoDB
            client = MongoClient(mongo_uri,username=mongo_username,password=mongo_password)
            db = client[db_database]
            searchs_collection = db[db_search_collection]
            searchs = searchs_collection
            searchs.insert_one(registry)
            client.close()
            
            return render_template('search.html', data=data)
        
        return render_template('search.html',data={})

    return render_template('index.html')

if __name__ == '__main__':
    app.secret_key = 'mysecret'
    app.run(debug=True)