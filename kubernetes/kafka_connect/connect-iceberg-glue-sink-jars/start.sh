#!/bin/bash

echo JAVA_HOME
echo $JAVA_HOME
which java

echo "Waiting for Kafka Connect to start"
# Launch Kafka Connect

confluent-hub install --no-prompt /data_iceberg/iceberg-kafka-connect-runtime-1.9.0-SNAPSHOT.zip

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

#    "ws_init_messages": ["{\"type\":\"subscribe\",\"symbol\":\"BINANCE:BTCUSDT\"}"],
#    "iceberg.catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
#    "iceberg.catalog.warehouse": "s3a://bigdata25-rates-glue/rates-bronze/",
#    "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
#    "iceberg.catalog.catalog-impl": "software.amazon.s3tables.iceberg.S3TablesCatalog",
#    "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
#    "iceberg.catalog.warehouse": "arn:aws:s3tables:ap-northeast-2:363147381615:bucket/bigdata25-rates-bronze",
#    "iceberg.catalog.s3.path-style-access": "true",
echo -e "\n--\n+> Creating WS source"
curl -s -X PUT -H  "Content-Type:application/json" http://localhost:8083/connectors/sink-iceberg-glue-09/config \
    -d '{
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
    "topics": "ws_raw",
    "iceberg.catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "iceberg.catalog.warehouse": "s3a://bigdata25-rates-glue/bigdata25.db/rates-bronze3",
    "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "iceberg.catalog.client.region": "ap-northeast-2",
    "iceberg.catalog.s3.region": "ap-northeast-2",
    "iceberg.catalog.s3.access-key-id": "AAAAAABBBBBBFFFFFFFFF",
    "iceberg.catalog.s3.secret-access-key": "AAAAAAAAAAAAAABBBBBBBBBBBBBBGGGGGGGGGGGGGGGG",
    "s3.region": "ap-northeast-2",
    "s3.access-key-id": "AAAAAABBBBBBFFFFFFFFF",
    "s3.secret-access-key": "AAAAAAAAAAAAAABBBBBBBBBBBBBBGGGGGGGGGGGGGGGG",
    "iceberg.tables": "bigdata25.rates_bronze3",
    "iceberg.tables.auto-create-enabled": "true",
    "iceberg.tables.evolve-schema-enabled": "true",
    "iceberg.tables.upsert-mode-enabled": "false",
    "iceberg.tables.dynamic-enabled": "false",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "iceberg.control.commit.interval-ms": "1000",
    "errors.log.enable": "true",
    "tasks.max": 1
}'
sleep infinity
