microk8s kubectl rollout restart deployment airflow-apiserver
microk8s kubectl rollout restart deployment airflow-dag-processor
microk8s kubectl rollout restart deployment airflow-scheduler
microk8s kubectl rollout restart deployment airflow-triggerer
microk8s kubectl rollout restart deployment airflow-worker

