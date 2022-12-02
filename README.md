# python-elasticsearch-cnpj

<h4>CSV Databases</h4>
We need 2 csv databases download at <a href="https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica-cnpj">https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica-cnpj</a>:<br>
<ul>
<li>Download the database of municipios, need to get the names of each município</li>
<li>Choose one Estabelecimento database. I have choosed the Estabelecimentos3 database</li>

</ul>

You will need to inform these database names on the enviroment file.


<h4>Virtual enviroment and install dependencies</h4>
Creat a virtual enviroment:<br>
python3 -m venv .venv <br>
Install dependencies:<br>
pip install -r requirements.txt


<h4>Enviroment file</h4>
Create an envimorent .env file as the .env.example file

<h4>Download elasticsearch and kibana images</h4>
docker pull docker.elastic.co/elasticsearch/elasticsearch:7.5.2

docker pull docker.elastic.co/kibana/kibana:7.5.2

<h4>Run elasticsearch container</h4>
docker run -d --name tera_elasticsearch --net mongo-els --restart=always -v esdata:/usr/share/elasticsearch/data -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.5.2

<h4>Run kibana container</h4>
docker run -d --name tera_kibana --net mongo-els --restart=always -p 5601:5601 -e "ELASTICSEARCH_HOSTS=http://tera_elasticsearch:9200" docker.elastic.co/kibana/kibana:7.5.2 

<h4>Run docker compose to init mongodb and mongo-express containers</h4>
docker compose up -d

<h4>Extract and Load data to mongodb and elasticsearch<h4>

python etl.py






