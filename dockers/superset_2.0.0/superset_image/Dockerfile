FROM apache/superset:5.0.0-py311

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

# RUN pip install --upgrade pip
# RUN pip install --upgrade sqlalchemy sqlalchemy-utils
#    .venv/bin/python3 -m pip install -Iv --no-cache sqlalchemy-clickhouse && \
WORKDIR /app
RUN ls /app/.venv/bin
RUN cd /app && \
    . .venv/bin/activate && \
    .venv/bin/python3 -m ensurepip --upgrade && \
    .venv/bin/python3 -m pip install -Iv --no-cache clickhouse-connect && \
    .venv/bin/python3 -m pip install -Iv --no-cache psycopg2-binary && \
    echo "###########" && \
    which pip3

USER superset

RUN echo 1
RUN ls /app/.venv/lib/python3.11/site-packages
RUN which pip3
RUN pip3 show sqlalchemy



