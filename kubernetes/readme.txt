microk8s kubectl get pods
microk8s kubectl apply -f .
microk8s kubectl logs <pod> --previous
microk8s kubectl rollout restart deployment <deployment>
microk8s kubectl delete deployment <deployment>
microk8s kubectl delete deployment --all --namespace=default
microk8s kubectl describe pod <pod>
microk8s kubectl describe deployment <deployment>
microk8s kubectl port-forward pod/kafka-monitoring-ui-7685b4fb56-9cb48 :8082
microk8s kubectl exec -it <pod> -- sh


minikube delete
minikube start
minikube start --memory 5120 --cpus=4
kubectl port-forward svc/kafka-ui 8082:8082
minikube mount /home/stanislav/k8s/25_11/kafka_connect:/home/stanislav/k8s/25_11/kafka_connect

kind create cluster --config=config.yaml
kind delete cluster

sudo microk8s start
sudo microk8s stop
microk8s kubectl port-forward svc/kafka-ui 8082:8082
microk8s kubectl port-forward svc/flink-ma-jobmanager-1 8084:8084
microk8s kubectl port-forward svc/zookeeper-ui 9000:9000
microk8s kubectl port-forward svc/superset-oscilator 8088:8088
microk8s kubectl port-forward svc/airflow-apiserver 8086:8086
microk8s kubectl port-forward svc/airflow-apiserver 8793:8793
microk8s kubectl port-forward svc/spark-master 8087:8087


microk8s kubectl get pods -A -o json | jq '.items[].spec.containers[]?.resources.requests.cpu' | sed -r 's/([0-9]*)m/\.\1/' | sed -e 's/"//g' -e 's/,//g' | paste -sd+ - | bc
microk8s kubectl get pods -A -o json | jq '.items[].spec.containers[]?.resources.requests.memory' | sed -r 's/"([0-9]*)M"/\1/g' | sed -r 's/"([0-9]*)Mi"/\1/g' | sed -r 's/"([0-9]*)Gi"/\1000/g' | sed -r 's/"([0-9]*)G"/\1000/g' | sed 's/null/0/g' |  paste -sd+ - | bc | sed 's/$/Mi/'


zookeeper-1:2181
"clickhousedb://bigdata25:aaaaaaaabbbbbbbbbeeeeeeeeeee@rc1b-aaaaaaaaabbbbbbbbbbbdddddddddddd.mdb.yandexcloud.net:8443/bigdata25?secure=True"

airflow dags list-import-errors

_schemas topic


./gradlew -x test -x integrationTest clean build
gradlew -x test clean build -Dspark_binary_version=3.5 -Dscala_binary_version=2.12 :clickhouse-spark-runtime-3.5_2.12
confluent-hub install --no-prompt tabular/iceberg-kafka-connect:0.6.19

curl -X DELETE http://localhost:8083/connectors/sink-iceberg-09
curl -vs http://localhost:8083/connectors  2>&1

docker image pull stanislavko2/bigdata25_dbt_clickhouse:1.9.7















