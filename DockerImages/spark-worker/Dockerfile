FROM bde2020/spark-base:3.1.1-hadoop3.2

LABEL maintainer="Obilio Sánchez <obiliosd@gmail.com>"

# Needed to install numpy
RUN apk --no-cache add --virtual build-deps musl-dev linux-headers g++ gcc python3-dev
# Install numpy
RUN pip3 install numpy==1.18.4

COPY worker.sh /

ENV SPARK_WORKER_WEBUI_PORT 8081
ENV SPARK_WORKER_LOG /spark/logs
ENV SPARK_MASTER "spark://spark-master:7077"

EXPOSE 8081

CMD ["/bin/bash", "/worker.sh"]