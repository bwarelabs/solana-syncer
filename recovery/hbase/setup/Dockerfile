FROM ubuntu:22.04

# Install OS packages
RUN apt-get update \
  && apt-get -y upgrade \
  && apt-get install -y wget sudo locales xmlstarlet

# OpenJDK setup
RUN apt-get install -y openjdk-17-jdk openjdk-17-jre
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN locale-gen en_US.UTF-8
ENV LANG=en_US.UTF-8 LANGUAGE=en_US:en LC_ALL=en_US.UTF-8

RUN useradd -G sudo -U -m -s /bin/bash ubuntu \
  && echo "ubuntu ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

WORKDIR /home/ubuntu

USER ubuntu

# HBase setup
ARG HBASE_VERSION=2.5.10
RUN wget https://dlcdn.apache.org/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz \
  && tar -xzvf hbase-${HBASE_VERSION}-bin.tar.gz \
  && rm hbase-${HBASE_VERSION}-bin.tar.gz
ENV PATH=/home/ubuntu/hbase-${HBASE_VERSION}/bin:$PATH

# Configure Zookeeper to listen on all interfaces
RUN xmlstarlet ed -L \
  -s "/configuration" -t elem -n "property" -v "" \
  -s "/configuration/property[last()]" -t elem -n "name" -v "hbase.zookeeper.property.clientPortAddress" \
  -s "/configuration/property[last()]" -t elem -n "value" -v "0.0.0.0" \
  /home/ubuntu/hbase-${HBASE_VERSION}/conf/hbase-site.xml
RUN xmlstarlet ed -L \
  -s "/configuration" -t elem -n "property" -v "" \
  -s "/configuration/property[last()]" -t elem -n "name" -v "hbase.server.keyvalue.maxsize" \
  -s "/configuration/property[last()]" -t elem -n "value" -v "268435456" \
  /home/ubuntu/hbase-${HBASE_VERSION}/conf/hbase-site.xml
RUN xmlstarlet ed -L \
  -s "/configuration" -t elem -n "property" -v "" \
  -s "/configuration/property[last()]" -t elem -n "name" -v "hbase.client.keyvalue.maxsize" \
  -s "/configuration/property[last()]" -t elem -n "value" -v "268435456" \
  /home/ubuntu/hbase-${HBASE_VERSION}/conf/hbase-site.xml
RUN xmlstarlet ed -L \
  -s "/configuration" -t elem -n "property" -v "" \
  -s "/configuration/property[last()]" -t elem -n "name" -v "hbase.hregion.max.filesize" \
  -s "/configuration/property[last()]" -t elem -n "value" -v "1610612736" \
  /home/ubuntu/hbase-${HBASE_VERSION}/conf/hbase-site.xml
RUN xmlstarlet ed -L \
  -s "/configuration" -t elem -n "property" -v "" \
  -s "/configuration/property[last()]" -t elem -n "name" -v "hbase.hregion.memstore.block.multiplier" \
  -s "/configuration/property[last()]" -t elem -n "value" -v "4" \
  /home/ubuntu/hbase-${HBASE_VERSION}/conf/hbase-site.xml
RUN xmlstarlet ed -L \
  -s "/configuration" -t elem -n "property" -v "" \
  -s "/configuration/property[last()]" -t elem -n "name" -v "hbase.hregion.memstore.flush.size" \
  -s "/configuration/property[last()]" -t elem -n "value" -v "134217728" \
  /home/ubuntu/hbase-${HBASE_VERSION}/conf/hbase-site.xml
RUN xmlstarlet ed -L \
  -s "/configuration" -t elem -n "property" -v "" \
  -s "/configuration/property[last()]" -t elem -n "name" -v "hbase.zookeeper.property.maxClientCnxns" \
  -s "/configuration/property[last()]" -t elem -n "value" -v "300" \
  /home/ubuntu/hbase-${HBASE_VERSION}/conf/hbase-site.xml
RUN xmlstarlet ed -L \
  -s "/configuration" -t elem -n "property" -v "" \
  -s "/configuration/property[last()]" -t elem -n "name" -v "zookeeper.session.timeout" \
  -s "/configuration/property[last()]" -t elem -n "value" -v "1200" \
  /home/ubuntu/hbase-${HBASE_VERSION}/conf/hbase-site.xml
RUN xmlstarlet ed -L \
  -s "/configuration" -t elem -n "property" -v "" \
  -s "/configuration/property[last()]" -t elem -n "name" -v "hbase.client.operation.timeout" \
  -s "/configuration/property[last()]" -t elem -n "value" -v "1200" \
  /home/ubuntu/hbase-${HBASE_VERSION}/conf/hbase-site.xml
RUN xmlstarlet ed -L \
  -s "/configuration" -t elem -n "property" -v "" \
  -s "/configuration/property[last()]" -t elem -n "name" -v "hbase.hstore.blockingStoreFiles" \
  -s "/configuration/property[last()]" -t elem -n "value" -v "200" \
  /home/ubuntu/hbase-${HBASE_VERSION}/conf/hbase-site.xml
# RUN xmlstarlet ed -L \
#   -s "/configuration" -t elem -n "property" -v "" \
#   -s "/configuration/property[last()]" -t elem -n "name" -v "hbase.rootdir" \
#   -s "/configuration/property[last()]" -t elem -n "value" -v "file:///hbase-data" \
#   /home/ubuntu/hbase-${HBASE_VERSION}/conf/hbase-site.xml

COPY init.sh /home/ubuntu/init.sh
CMD ["/bin/bash", "init.sh"]