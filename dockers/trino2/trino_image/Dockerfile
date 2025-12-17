FROM trinodb/trino:479-amd64

COPY ./yandex_CA.pem /etc/pki/ca-trust/source/anchors/certfile.crt

USER root

RUN mkdir --parents /usr/local/share/ca-certificates/Yandex/
RUN curl -o /usr/local/share/ca-certificates/Yandex/RootCA.crt https://storage.yandexcloud.net/cloud-certs/RootCA.pem
RUN curl -o /usr/local/share/ca-certificates/Yandex/IntermediateCA.crt https://storage.yandexcloud.net/cloud-certs/IntermediateCA.pem
RUN chmod 655 /usr/local/share/ca-certificates/Yandex/RootCA.crt /usr/local/share/ca-certificates/Yandex/IntermediateCA.crt

RUN curl -o /etc/pki/ca-trust/source/anchors/RootCA.crt https://storage.yandexcloud.net/cloud-certs/RootCA.pem
RUN curl -o /etc/pki/ca-trust/source/anchors/IntermediateCA.crt https://storage.yandexcloud.net/cloud-certs/IntermediateCA.pem
RUN chmod 655 /etc/pki/ca-trust/source/anchors/RootCA.crt /etc/pki/ca-trust/source/anchors/IntermediateCA.crt

RUN /bin/update-ca-trust

# ONLY this is working
COPY ./yandex_CA.pem /etc/pki/ca-trust/source/anchors/myroot.pem
RUN keytool -import -alias myroot -keystore ${JAVA_HOME}/lib/security/cacerts -storepass qwerty -noprompt -trustcacerts -file /etc/pki/ca-trust/source/anchors/myroot.pem
RUN keytool -import -alias myroot -keystore cacerts -storepass qwerty -noprompt -trustcacerts -file /etc/pki/ca-trust/source/anchors/myroot.pem

# set this environment variable so Trino knows which certs to use!
ENV AWS_CA_BUNDLE=/etc/pki/ca-trust/source/anchors/certfile.crt

USER trino
