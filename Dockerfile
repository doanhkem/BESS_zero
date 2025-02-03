# syntax=docker/dockerfile:1
FROM python:3.11.3-alpine
ENV PYTHONUNBUFFERED=1
RUN mkdir /etc/modbus/
WORKDIR /etc/modbus/
RUN apk add build-base && \
  apk add git && \
  git clone https://github.com/doanhkem/BESS_zero.git /etc/modbus/ && \
  pip3 install -r requirements.txt && \
  apk del build-base linux-headers pcre-dev openssl-dev && \
  rm -rf /var/cache/apk/*
CMD ["python", "bess_control.py"]