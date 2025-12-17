FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends

WORKDIR /usr/app

COPY ./yandex_CA.pem /usr/app/yandex_CA.pem

RUN pip install --upgrade pip
RUN pip install truststore
RUN pip install dbt-clickhouse==1.9.7 --use-feature=truststore
RUN pip install pytz --use-feature=truststore
# RUN pip install /usr/app/yandex_CA.pem --use-feature=truststore
# RUN export SSL_CERT_FILE=/usr/app/yandex_CA.pem
RUN pip config set global.cert /usr/app/yandex_CA.pem
RUN pip config list

RUN cat /usr/app/yandex_CA.pem >> $(python -m certifi)
# RUN cat $(python -m certifi)
# RUN tail -n 10 $(python -m certifi)

RUN apt-get install wget -y --no-install-recommends
RUN mkdir --parents /usr/local/share/ca-certificates/Yandex/
RUN wget "https://storage.yandexcloud.net/cloud-certs/RootCA.pem" --output-document /usr/local/share/ca-certificates/Yandex/RootCA.crt
RUN wget "https://storage.yandexcloud.net/cloud-certs/IntermediateCA.pem" --output-document /usr/local/share/ca-certificates/Yandex/IntermediateCA.crt
RUN chmod 655 /usr/local/share/ca-certificates/Yandex/RootCA.crt /usr/local/share/ca-certificates/Yandex/IntermediateCA.crt
RUN update-ca-certificates

# CMD dbt deps && dbt build --profiles-dir profiles && sleep infinity

# CMD ["dbt", "run"]

COPY ./run.sh /usr/app/run.sh
RUN chmod +x /usr/app/run.sh

ENTRYPOINT ["sh","/usr/app/run.sh"]


