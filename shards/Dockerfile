FROM python:3.6
RUN apt-get update && apt-get install -y --no-install-recommends \
    less \
    telnet && \
    apt-get remove --purge -y && \
    rm -rf /var/lib/apt/lists/*
RUN mkdir /source
WORKDIR /source
COPY ./requirements.txt ./
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "mishards/main.py"]
