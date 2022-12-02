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
dataset_mun_csv = os.getenv("DATASET_MUN_CSV")

# Mongodb Config
mongo_uri=os.getenv("MONGO_URI")
mongo_username=os.getenv("MONGO_INITDB_ROOT_USERNAME")
mongo_password=os.getenv("MONGO_INITDB_ROOT_PASSWORD")
db_database = os.getenv("DB_DATABASE")
db_est_collection = os.getenv("DB_EST_COLLECTION")

# Elasticsearch Config
es_host = os.getenv("ELASTICSEARCH_URI")
es = Elasticsearch([es_host])
es_index = os.getenv("ELASTICSEARCH_INDEX")

# Connect to MongoDB
mongodb_client = MongoClient(mongo_uri,username=mongo_username,password=mongo_password)
db = mongodb_client[db_database]
collection_est = db[db_est_collection]


def extract(total=100000):
    columns_est = ['CNPJ BÁSICO','CNPJ ORDEM','CNPJ DV','IDENTIFICADOR MATRIZ/FILIAL','NOME FANTASIA',
    'SITUAÇÃO CADASTRAL','DATA SITUAÇÃO CADASTRAL','MOTIVO SITUAÇÃO CADASTRAL',
    'NOME DA CIDADE NO EXTERIOR','PAIS','DATA DE INÍCIO ATIVIDADE',
    'CNAE FISCAL PRINCIPAL','CNAE FISCAL SECUNDÁRIA','TIPO DE LOGRADOURO',
    'LOGRADOURO','NÚMERO','COMPLEMENTO','BAIRRO','CEP','UF','CÓDIGO_MUN',
    'DDD 1','TELEFONE 1','DDD 2','TELEFONE 2','DDD DO FAX','FAX',
    'CORREIO ELETRÔNICO','SITUAÇÃO ESPECIAL','DATA DA SITUAÇÃO ESPECIAL'
    ]
    columns_mun = ['CÓDIGO_MUN','MUNICÍPIO'] 
    df_est = pd.DataFrame()
    step=10000
    added=0
    i=0
    for chunk in pd.read_csv(dataset_est_csv, sep = ';', encoding = "ISO-8859-1",header=None,na_filter=False,names=columns_est,chunksize=step):
        i += 1
        print(f"saving data cnpj up {i*step}")
        nulos = chunk['NOME FANTASIA'].isna().sum()
        print(f'nulos: {nulos}')
        chunk.dropna(subset=['NOME FANTASIA'],inplace=True)
        new = step - nulos
        if added + new > total:
            new = total - added
            df_est = pd.concat([df_est,chunk.head(new)])
        else:
            df_est = pd.concat([df_est,chunk])
        added += new
        if(added==total):
            break
        
    df_est.reset_index(drop=True, inplace=True)

    df_mun = pd.DataFrame()
    step=1000
    i=0
    for chunk in pd.read_csv(dataset_mun_csv, sep = ';', encoding = "ISO-8859-1",header=None,na_filter=False,names=columns_mun,chunksize=step):
        i += 1
        print(f"saving municipios up {i*step}")
        df_mun = pd.concat([df_mun,chunk])
            
    df_mun.reset_index(drop=True, inplace=True)

    data = df_est.merge(df_mun, on='CÓDIGO_MUN', how='left')
    columns_drop = ['DDD 2','TELEFONE 2','DDD DO FAX','FAX',
                    'SITUAÇÃO ESPECIAL','DATA DA SITUAÇÃO ESPECIAL',
                    'NOME DA CIDADE NO EXTERIOR','PAIS'
    ]
    data.drop(columns_drop, axis=1, inplace=True)
    '''data['CEP'] = data['CEP'].astype('Int64')
    data['DDD 1'] = data['DDD 1'].astype('Int64')
    data['TELEFONE 1'] = data['TELEFONE 1'].astype('Int64')'''
    data.reset_index(inplace=True)
    data_dict = data.to_dict("records")
    # Insert collection on mongodb
    collection_est.insert_many(data_dict)
    
    

'''def migrate(num_docs = 100000):
  res = collection_est.find()
  
  actions = []
  for i in range(num_docs):
      doc = res[i]
      mongo_id = doc['_id']
      print(f"mongo_id: {mongo_id}, doc #: {i+1}",)
      doc.pop('_id', None)
      actions.append({
          "_index": es_index,
          "_id": str(mongo_id),
          "_source": json.dumps(doc)
      })
  response = helpers.bulk(es, actions)
  
'''
'''
def migrate(num_docs=100000):
  res = collection_est.find()
  for i in range(num_docs):
      doc = res[i]
      mongo_id = doc['_id']
      print(f"ES sending doc #: {i+1}",)
      doc.pop('_id', None)
      actions =[{
          "_index": es_index,
          "_id": str(mongo_id),
          "_source": json.dumps(doc)
      }]
      response = helpers.bulk(es, actions)
'''  
def _doc_to_json(doc):
        doc_str = json.dumps(doc, default=str)
        doc_json = json.loads(doc_str)
        return doc_json

def migrate(limit=100000,chunk_size=10000):
    no_docs = 0
    offset = 0
    print("Starting ES upload")
    while True:
        mongo_cursor = collection_est.find()
        mongo_cursor.skip(offset)
        mongo_cursor.limit(chunk_size)
        docs = list(mongo_cursor)
        # break loop if no more documents found
        if not len(docs):
            break
        # convert document to json to avoid SerializationError
        docs = [_doc_to_json(doc) for doc in docs]
        actions = []
        for doc in docs:
            mongo_id = doc['_id']
            #print(f"mongo_id: {mongo_id}",)
            doc.pop('_id', None)
            actions.append({
                "_index": es_index,
                "_id": str(mongo_id),
                "_source": json.dumps(doc)
            })
        response = helpers.bulk(es, actions)
        # check for number of documents limit, stop if exceed
        no_docs += len(docs)
        print(f"Es total docs: {no_docs}",)
        if no_docs >=limit:
            print("Finishing ES upload.")
            break
            # update offset to fetch next chunk/page
        offset += chunk_size



extract()
migrate()
