#!/bin/bash

echo "Starting jobmanager"

#pwd
#ls -la

#pwd
#ls -la ./bin

./bin/jobmanager.sh start-foreground &

#
# Wait for Flink
echo "Waiting for Flink to start listening on localhost ⏳"
while : ; do
  curl_status=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8084)
  echo -e $(date) " Flink listener HTTP state: " $curl_status " (waiting for 200)"
  if [ $curl_status -eq 200 ] ; then
    break
  fi
  sleep 5 
done

#sleep 120 

# Run Flink job

./bin/flink run /data/flink-kafka-ma-1.0.0.jar


