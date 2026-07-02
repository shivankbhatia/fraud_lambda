FROM flink:1.18.1-scala_2.12-java11

# Install Python 3.10 and pip
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install PyFlink matching the Flink version, plus xgboost for model scoring later
RUN pip3 install --no-cache-dir \
    apache-flink==1.18.1 \
    xgboost==2.0.3