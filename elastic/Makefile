ca:
	docker compose -f elastic.yml  cp es1:/usr/share/elasticsearch/config/certs/http_ca.crt .

test:
	curl --cacert http_ca.crt -u 'elastic:VqEUnEgvHW*TvA0Rxq3T' https://localhost:9200/_cat
