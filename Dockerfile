FROM ubuntu:20.04

# Install prerequisites
RUN apt update -qq && \
    apt install -qq -y python3-pip \
        git \
        wget \
        curl \
        apt-transport-https \
        build-essential

# Install Elastic+Kibana
RUN wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | apt-key add - && \
    echo "deb https://artifacts.elastic.co/packages/7.x/apt stable main" | tee /etc/apt/sources.list.d/elastic-7.x.list && \
    apt update -qq && \
    apt install elasticsearch \
        kibana

# Start Elastic server
RUN update-rc.d elasticsearch defaults 95 10

# Start Kibana server
RUN /etc/init.d/elasticsearch start && \
    update-rc.d kibana defaults 95 10 && \
    sed -n 'H;${x;s/^\n//;s/#server.host.*$/server.host: "0.0.0.0"\n&/;p;}' /etc/kibana/kibana.yml > /etc/kibana/kibana.yml.new && \
    mv /etc/kibana/kibana.yml.new /etc/kibana/kibana.yml && \
    while ! curl -s 127.0.0.1:9200; do sleep 1; done && \
    service kibana start

# Get code from GH
RUN git clone https://github.com/Sonik-zirael/lizaalert.git && \
    cd lizaalert && \
    git checkout test_for_docker

# Set up environment
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install -r /lizaalert/requirements.txt

# Start loading data
RUN /etc/init.d/elasticsearch start && \
    while ! curl -s 127.0.0.1:9200; do sleep 1; done && \
    cd /lizaalert/data_loading/ && \
    python3 load_data.py

# Pass dashboard config to Kibana
COPY export.json /lizaalert/
RUN /etc/init.d/elasticsearch start && \
    while ! curl -s 127.0.0.1:9200; do sleep 1; done && \
    service kibana start && \
    while ! curl -s 127.0.0.1:5601 || [ $(curl -s 127.0.0.1:5601) = "Kibana server is not ready yet" ] ; do sleep 1; done && \
    cd /lizaalert && \
    curl -X POST -H "Content-Type: application/json" -H "kbn-xsrf: true" -d @export.json http://localhost:5601/api/kibana/dashboards/import

# Create start script
RUN echo "/etc/init.d/elasticsearch start;\nservice kibana start;\nwhile true; do sleep 1; done;" > start_script.sh

EXPOSE 5601

ENTRYPOINT ["bash", "start_script.sh"]
