FROM openjdk:8-alpine

RUN apk add \
    bash \
    gcompat \
    gcc \
    libffi-dev \
    musl-dev \
    openssl-dev \
    python3-dev

RUN python3 -m pip install \
    "pyspark==2.4.5" \
    "moto[server]"

ENV PYSPARK_PYTHON=python3

WORKDIR /work

COPY setup.sh .

RUN ./setup.sh install-packages \
    org.apache.hadoop:hadoop-aws:2.7.7

COPY . .
