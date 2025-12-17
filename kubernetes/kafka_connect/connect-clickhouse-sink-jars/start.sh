#!/bin/bash

echo JAVA_HOME
echo $JAVA_HOME
which java

echo "Waiting for Kafka Connect to start"
# Launch Kafka Connect

# confluent-hub install --no-prompt /data_iceberg/iceberg-kafka-connect-runtime-1.9.0-SNAPSHOT.zip

/etc/confluent/docker/run &

#
# Wait for Kafka Connect listener
echo "Waiting for Kafka Connect to start listening on localhost ⏳"
while : ; do
  curl_status=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8083/connectors)
  echo -e $(date) " Kafka Connect listener HTTP state: " $curl_status " (waiting for 200)"
  if [ $curl_status -eq 200 ] ; then
    break
  fi
  sleep 5 
done

echo -e "\n--\n+> Creating WS source"
curl -s -X PUT -H  "Content-Type:application/json" http://localhost:8083/connectors/sink-clickhouse-09/config \
    -d '{
    "topics": "ma",
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    "ssl": "true",
    "jdbcConnectionProperties": "?sslmode=STRICT&ssl=true&sslrootcert=/data/connect-clickhouse-sink-jars/yandex_CA.pem",
    "security.protocol": "SSL",
    "hostname": "rc1b-aaaaaaaabbbbbbbbbbbbdddddddddd.mdb.yandexcloud.net",
    "port": "8443",
    "database": "bigdata25",
    "username": "bigdata25",
    "password": "aaaaaaaabbbbbbbbbeeeeeeeeeee",
    "ssl.truststore.location": "/data/connect-clickhouse-sink-jars/kafka.client.truststore.jks",
    "exactlyOnce": "true",
    "schemas.enable": "false",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "errors.log.enable": "true",
    "tasks.max": 1
}'
sleep infinity
