FROM apache/airflow:slim-3.1.3-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends

COPY ./yandex_CA.pem /usr/app/yandex_CA.pem

RUN cat /usr/app/yandex_CA.pem >> $(python -m certifi)

RUN apt-get install wget -y --no-install-recommends
RUN mkdir --parents /usr/local/share/ca-certificates/Yandex/
RUN wget "https://storage.yandexcloud.net/cloud-certs/RootCA.pem" --output-document /usr/local/share/ca-certificates/Yandex/RootCA.crt
RUN wget "https://storage.yandexcloud.net/cloud-certs/IntermediateCA.pem" --output-document /usr/local/share/ca-certificates/Yandex/IntermediateCA.crt
RUN chmod 655 /usr/local/share/ca-certificates/Yandex/RootCA.crt /usr/local/share/ca-certificates/Yandex/IntermediateCA.crt
RUN update-ca-certificates

WORKDIR /home/airflow
RUN ls /home/airflow/.local/bin
#    .local/bin/python3 -m pip install -Iv --no-cache asyncpg && \
RUN cd /home/airflow && \
    . .local/bin/activate && \
    .local/bin/python3 -m pip install -Iv --no-cache clickhouse-connect && \
    .local/bin/python3 -m pip uninstall -y sqlalchemy==1.4.54 && \
    .local/bin/python3 -m pip install -Iv --no-cache sqlalchemy==2.0.45 && \
    .local/bin/python3 -m pip install -Iv --no-cache psycopg2-binary==2.9.11 && \
    .local/bin/python3 -m pip install -Iv --no-cache asyncpg && \
    .local/bin/python3 -m pip install -Iv --no-cache apache-airflow-providers-celery==3.12.4 && \
    echo "###########" && \
    which pip3
# RUN .local/bin/python3 -m pip install -Iv --no-cache apache-airflow-providers-apache-spark==5.4.0
RUN .local/bin/python3 -m pip install -Iv --no-cache apache-airflow-providers-http==5.6.0
RUN .local/bin/python3 -m pip install -Iv --no-cache redis==4.6.0
RUN .local/bin/python3 -m pip install -Iv --no-cache apache-airflow-providers-amazon==9.18.1

USER airflow

WORKDIR /opt/airflow

RUN pip3 show psycopg2-binary
RUN pip3 show sqlalchemy
RUN pip3 show apache-airflow-providers-celery
RUN pip3 show redis

