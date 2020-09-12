FROM python:3.6.8-jessie

LABEL Name=megasearch_engine_test Version=0.0.1

WORKDIR /app
COPY . /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libc-dev build-essential && \
    python3 -m pip install -r requirements.txt && \
    apt-get remove --purge -y && \
    rm -rf /var/lib/apt/lists/*

ENTRYPOINT [ "/app/docker-entrypoint.sh" ]
CMD [ "start" ]