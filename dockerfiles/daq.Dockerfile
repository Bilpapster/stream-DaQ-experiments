FROM python:3.12-slim

RUN apt-get -y update && apt-get -y install git

WORKDIR /git_clone_directory

RUN git clone https://github.com/Bilpapster/stream-DaQ.git && mv stream-DaQ/ /app/ && cd /app/ && git checkout 3da85c1cb468651f09cc49779152a6ed7eb2d7cf

WORKDIR /app

COPY requirements/requirements_daq.txt ./requirements.txt

# Install system dependencies and Python packages
RUN apt-get update && apt-get install -y \
    gcc \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app/pathway

COPY scalability_experiment* .

CMD ["/bin/sh", "-c", "\
    echo The value of STREAM environment variable is: $STREAM; \
    if [ \"$STREAM\" = \"reddit\" ]; then \
        echo 'Starting Stream DaQ for Reddit stream processing...'; \
        pathway spawn --processes ${SPARK_NUM_CORES} python ./scalability_experiment_reddit.py; \
    else \
        echo 'Starting Stream DaQ for Products stream processing...'; \
        pathway spawn --processes ${SPARK_NUM_CORES} python ./scalability_experiment.py; \
    fi \
"]