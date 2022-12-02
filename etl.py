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
dataset_est_csv = os.getenv("DATASET_EST_CSV")
dataset_emp_csv = os.getenv("DATASET_EMP_CSV")

# Mongodb Config
mongo_username=os.getenv("MONGO_INITDB_ROOT_USERNAME")
mongo_password=os.getenv("MONGO_INITDB_ROOT_PASSWORD")
db_database = os.getenv("DB_DATABASE")
db_emp_collection = os.getenv("DB_EMP_COLLECTION")
db_est_collection = os.getenv("DB_EST_COLLECTION")

# Elasticsearch Config
es_host = os.getenv("ELASTICSEARCH_URI")
es = Elasticsearch([es_host])
es_index = os.getenv("ELASTICSEARCH_INDEX")





# Connect to MongoDB

client = MongoClient('mongodb://localhost:27017/',
                  username=mongo_username,
                 password=mongo_password)

#client =  MongoClient("mongodb+srv://<<YOUR USERNAME>>:<<PASSWORD>>@clustertest-icsum.mongodb.net/test?retryWrites=true&w=majority")
db = client[db_database]
collection_est = db[db_est_collection]





def extract():
    '''for chunk in pd.read_csv('Estabelecimentos2.csv', sep = ';', encoding = "ISO-8859-1", chunksize=100):
    print(chunk.head())'''
    columns_emp = ['CNPJ BÁSICO','RAZÃO SOCIAL','NATUREZA JURÍDICA','QUALIFICAÇÃO DO RESPONSÁVEL',
    'CAPITAL SOCIAL DA EMPRESA','PORTE DA EMPRESA','ENTE FEDERATIVO RESPONSÁVEL'
    ]
    columns_est = ['CNPJ BÁSICO','CNPJ ORDEM','CNPJ DV','IDENTIFICADOR MATRIZ/FILIAL','NOME FANTASIA',
    'SITUAÇÃO CADASTRAL','DATA SITUAÇÃO CADASTRAL','MOTIVO SITUAÇÃO CADASTRAL',
    'NOME DA CIDADE NO EXTERIOR','PAIS','DATA DE INÍCIO ATIVIDADE',
    'CNAE FISCAL PRINCIPAL','CNAE FISCAL SECUNDÁRIA','TIPO DE LOGRADOURO',
    'LOGRADOURO','NÚMERO','COMPLEMENTO','BAIRRO','CEP','UF','MUNICÍPIO',
    'DDD 1','TELEFONE 1','DDD 2','TELEFONE 2','DDD DO FAX','FAX',
    'CORREIO ELETRÔNICO','SITUAÇÃO ESPECIAL','DATA DA SITUAÇÃO ESPECIAL'
    ]
    count = 0
    chunksize=10000
    for data in pd.read_csv(dataset_est_csv, sep = ';', encoding = "ISO-8859-1", na_filter=False, names=columns_est,iterator=True,chunksize=chunksize):
        count += 1
        print(f"saving up {count*10000}")
        data.reset_index(inplace=True)
        data_dict = data.to_dict("records")
        # Insert collection
        collection_est.insert_many(data_dict)
        
    
    #data = pd.read_csv(dataset_est_csv, sep = ';', encoding = "ISO-8859-1", nrows= 20, header=None, na_filter=False, names=columns_est)
    #data.reset_index(inplace=True)
    #data_dict = data.to_dict("records")
    # Insert collection
    #collection.insert_many(data_dict)
    #df_cd = pd.merge(df_SN7577i_c, df_SN7577i_d, how='inner', on = 'Id')


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
  

#migrate()
extract()