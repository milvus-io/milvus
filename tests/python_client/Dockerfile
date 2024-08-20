FROM python:3.8-buster

RUN apt-get update && apt-get install -y jq

# Define the ARG variable
ARG PIP_TRUSTED_HOST=""
ARG PIP_INDEX_URL=""
ARG PIP_INDEX=""
ARG PIP_FIND_LINKS=""

# Set the ENV variable
ENV PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST}
ENV PIP_INDEX_URL=${PIP_INDEX_URL}
ENV PIP_INDEX=${PIP_INDEX}
ENV PIP_FIND_LINKS=${PIP_FIND_LINKS}


WORKDIR /milvus

COPY tests/python_client/requirements.txt tests/python_client/requirements.txt

RUN cd tests/python_client && python3 -m pip install --upgrade setuptools \
    && python3 -m pip install --upgrade pip \
    && python3 -m pip install --no-cache-dir  -r requirements.txt --timeout 30 --retries 6

COPY tests/python_client tests/python_client
COPY tests/restful_client tests/restful_client
COPY tests/restful_client_v2 tests/restful_client_v2
COPY tests/scripts tests/scripts
