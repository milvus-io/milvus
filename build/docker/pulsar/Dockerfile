FROM adoptopenjdk:11-jdk-hotspot
# Prepare environment
ENV PULSAR_HOME=/pulsar
ENV PATH=$PULSAR_HOME/bin:$PATH
RUN groupadd --system --gid=9999 pulsar && useradd --system --home-dir $PULSAR_HOME --uid=9999 --gid=pulsar pulsar
WORKDIR $PULSAR_HOME

ARG PULSAR_VERSION
ENV PULSAR_VERSION 2.8.2
# Install Pulsar
RUN set -ex; \
  apt-get update && apt-get install -y wget; \
  PULSAR_VERSION=$PULSAR_VERSION; \
  wget -O pulsar.tgz "https://archive.apache.org/dist/pulsar/pulsar-${PULSAR_VERSION}/apache-pulsar-${PULSAR_VERSION}-bin.tar.gz"; \
  tar -xf pulsar.tgz --strip-components=1; \
  rm pulsar.tgz; \
  \
  chown -R pulsar:pulsar .;

COPY apply-config-from-env.py bin/

RUN apt-get update && apt-get install python3-pip python3 -y

EXPOSE 6650 8080

RUN python3 bin/apply-config-from-env.py conf/standalone.conf

CMD [ "bin/pulsar","standalone", "--no-functions-worker", "--no-stream-storage" ]
