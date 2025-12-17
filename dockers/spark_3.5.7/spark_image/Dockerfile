FROM python:3.11-bullseye
# default shell is sh
RUN apt-get update
RUN apt-get install -y --no-install-recommends openjdk-11-jdk
RUN apt-get clean && rm -rf /var/lib/apt/lists/*
ENV SPARK_HOME="/opt/spark"
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
ENV PATH="${JAVA_HOME}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
RUN mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}
# Fetch latest spark 3.5.x version and download the appropriate binary file
RUN curl -O https://dlcdn.apache.org/spark/spark-3.5.7/spark-3.5.7-bin-hadoop3-scala2.13.tgz

 # Unpack the file and cleanup the binary file
RUN tar xvzf spark-3.5.7-bin-hadoop3-scala2.13.tgz --directory ${SPARK_HOME} --strip-components 1 \
    && rm -rf spark-3.5.7-bin-hadoop3-scala2.13.tgz
# Port master will be exposed
ENV SPARK_MASTER_PORT="7077"
# Name of master container and also counts as hostname
ENV SPARK_MASTER_HOST="spark-master"
COPY ./spark-defaults.conf "${SPARK_HOME}/conf"
COPY ./start-worker-my.sh "${SPARK_HOME}/sbin"
RUN chmod 755 "${SPARK_HOME}/sbin/start-worker-my.sh"
RUN ls -la "${SPARK_HOME}/sbin"

COPY ./yandex_CA.pem /usr/app/yandex_CA.pem

RUN apt-get install wget -y --no-install-recommends
RUN mkdir --parents /usr/local/share/ca-certificates/Yandex/
RUN wget "https://storage.yandexcloud.net/cloud-certs/RootCA.pem" --output-document /usr/local/share/ca-certificates/Yandex/RootCA.crt
RUN wget "https://storage.yandexcloud.net/cloud-certs/IntermediateCA.pem" --output-document /usr/local/share/ca-certificates/Yandex/IntermediateCA.crt
RUN chmod 655 /usr/local/share/ca-certificates/Yandex/RootCA.crt /usr/local/share/ca-certificates/Yandex/IntermediateCA.crt
RUN update-ca-certificates

ENTRYPOINT ["/bin/bash","-c"]