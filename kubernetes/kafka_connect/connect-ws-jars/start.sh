#!/bin/bash

echo "Waiting for Kafka Connect to start"
# Launch Kafka Connect
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
echo -e "\n--\n+> Creating WS source"
curl -s -X PUT -H  "Content-Type:application/json" http://localhost:8083/connectors/source-ws-01/config \
    -d '{
    "connector.class": "com.hwn.bd25.kafkaconnectwssource.WsSourceConnector",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "topic": "ws_raw",
    "ws_uri": "wss://ws.finnhub.io?token=AAAAAAAABBBBBBBBBBCCCCCCC",
    "max.interval":750,
    "quickstart": "ratings",
    "tasks.max": 1
}'
sleep infinity
