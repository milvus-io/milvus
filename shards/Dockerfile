FROM python:3.6
RUN apt update && apt install -y \
    less \
    telnet
RUN mkdir /source
WORKDIR /source
ADD ./requirements.txt ./
RUN pip install -r requirements.txt
COPY . .
CMD python mishards/main.py
