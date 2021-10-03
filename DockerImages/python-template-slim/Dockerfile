FROM python:3.9.7-slim

LABEL maintainer="Obilio Sánchez <obiliosd@gmail.com>"

RUN pip3 install numpy pandas

WORKDIR /src/app

ONBUILD COPY requirements.txt .
ONBUILD RUN pip3 install -r requirements.txt

ONBUILD COPY ./src /src

CMD [ "python3", "app.py" ]