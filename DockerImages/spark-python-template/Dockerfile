FROM bde2020/spark-submit:3.1.1-hadoop3.2

LABEL maintainer="Obilio Sánchez <obiliosd@gmail.com>"

# Needed to install numpy
RUN apk --no-cache add --virtual build-deps musl-dev linux-headers g++ gcc python3-dev
# Install numpy
RUN pip3 install numpy==1.18.4

COPY template.sh /

# Copy the requirements.txt first, for separate dependency resolving and downloading
ONBUILD COPY requirements.txt /app/
ONBUILD RUN cd /app \
      && pip3 install -r requirements.txt

# Copy the source code
ONBUILD COPY ./src /app/src

CMD ["/bin/bash", "/template.sh"]