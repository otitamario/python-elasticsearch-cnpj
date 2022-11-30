from dotenv import load_dotenv
import os
import pandas as pd
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json
from bson import json_util
import requests


# Load env variables
load_dotenv()
#Database CSV config
dataset_csv = os.getenv("DATASET_CSV")
# Mongodb Config
mongo_username=os.getenv("MONGO_INITDB_ROOT_USERNAME")
mongo_password=os.getenv("MONGO_INITDB_ROOT_PASSWORD")
db_database = os.getenv("DB_DATABASE")
db_collection = os.getenv("DB_COLLECTION")
# Elasticsearch Config
es_host = os.getenv("ELASTICSEARCH_URI")
es = Elasticsearch([es_host])
es_index = os.getenv("ELASTICSEARCH_INDEX")




'''for chunk in pd.read_csv('Estabelecimentos2.csv', sep = ';', encoding = "ISO-8859-1", chunksize=100):
    print(chunk.head())'''
columns = ['CNPJ BÁSICO','CNPJ ORDEM','CNPJ DV','IDENTIFICADOR MATRIZ/FILIAL','NOME FANTASIA',
'SITUAÇÃO CADASTRAL','DATA SITUAÇÃO CADASTRAL','MOTIVO SITUAÇÃO CADASTRAL',
'NOME DA CIDADE NO EXTERIOR','PAIS','DATA DE INÍCIO ATIVIDADE',
'CNAE FISCAL PRINCIPAL','CNAE FISCAL SECUNDÁRIA','TIPO DE LOGRADOURO',
'LOGRADOURO','NÚMERO','COMPLEMENTO','BAIRRO','CEP','UF','MUNICÍPIO',
'DDD 1','TELEFONE 1','DDD 2','TELEFONE 2','DDD DO FAX','FAX',
'CORREIO ELETRÔNICO','SITUAÇÃO ESPECIAL','DATA DA SITUAÇÃO ESPECIAL'
]

data = pd.read_csv(dataset_csv, sep = ';', encoding = "ISO-8859-1", nrows= 20, header=None, na_filter=False, names=columns)

# Connect to MongoDB

client = MongoClient('mongodb://localhost:27017/',
                  username=mongo_username,
                 password=mongo_password)

#client =  MongoClient("mongodb+srv://<<YOUR USERNAME>>:<<PASSWORD>>@clustertest-icsum.mongodb.net/test?retryWrites=true&w=majority")
db = client[db_database]
collection = db[db_collection]

'''
data.reset_index(inplace=True)
data_dict = data.to_dict("records")
# Insert collection
collection.insert_many(data_dict)
'''

def parse_json(data):
    return json.loads(json_util.dumps(data))

def migrate():
  res = collection.find()
  # number of docs to migrate
  num_docs = 20
  actions = []
  for i in range(num_docs):
      doc = res[i]
      mongo_id = doc['_id']
      print('mongo_id: ',mongo_id)
      doc.pop('_id', None)
      actions.append({
          "_index": es_index,
          "_id": str(mongo_id),
          "_source": json.dumps(doc)
      })
  res = helpers.bulk(es, actions)
  
  #print(actions)
  #res =es.bulk(es,body=actions)
  #headers = {'Content-type': 'application/x-ndjson'}  
  #index_url = f'{"http://localhost:9200/"}{es_index}'
  #bulk_post_response, exception = requests.post(f'{index_url}/_bulk', data=actions, headers=headers)
  #res = es.bulk(index = es_index, body = actions, headers = headers ,refresh = True)

migrate()
