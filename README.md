# python-elasticsearch-cnpj

docker pull docker.elastic.co/elasticsearch/elasticsearch:7.5.2

docker pull docker.elastic.co/kibana/kibana:7.5.2

docker run -d --name tera_elasticsearch --net mongo-els --restart=always -v esdata:/usr/share/elasticsearch/data -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.5.2


docker run -d --name tera_kibana --net mongo-els --restart=always -p 5601:5601 -e "ELASTICSEARCH_HOSTS=http://tera_elasticsearch:9200" docker.elastic.co/kibana/kibana:7.5.2 








