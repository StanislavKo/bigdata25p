#!/bin/bash

echo "Waiting for Airflow to start"
# Launch Kafka Connect
airflow db migrate
echo '{"admin": "admin"}' > /opt/airflow/simple_auth_manager_passwords.json.generated
airflow api-server &

sleep 60

while true; do
    airflow connections add 'spark_http_conn_id' --conn-type 'http' --conn-host 'spark-master' --conn-port 6066 --conn-schema 'http' || break
    sleep 3
done
echo "spark_http_conn_id is registered!"

while true; do
    airflow connections add 'aws_my' --conn-type 'aws' --conn-login 'AAAAAABBBBBBFFFFFFFFF' --conn-password 'AAAAAAAAAAAAAABBBBBBBBBBBBBBGGGGGGGGGGGGGGGG' --conn-extra '{"region_name": "ap-northeast-2"}' || break
    sleep 3
done
echo "aws_my is registered!"



tail -f /dev/null
