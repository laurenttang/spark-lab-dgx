# Spark Lab image: one image for master / workers / jupyter / portal
# Base: official Jupyter pyspark stack (multi-arch, native aarch64)
#   Spark 4.1.2 (Scala 2.13) + OpenJDK 21 + Python 3.13
FROM quay.io/jupyter/pyspark-notebook:spark-4.1.2

USER root
RUN apt-get update && apt-get install -y --no-install-recommends netcat-openbsd curl \
    && rm -rf /var/lib/apt/lists/*
USER ${NB_UID}

# Python libs used by the course demos + the web portal
RUN pip install --no-cache-dir \
      "fastapi==0.115.*" "uvicorn[standard]==0.32.*" \
      "geoip2==4.8.*" "afinn==0.1" "kafka-python==2.0.*" \
      "shapely==2.0.*" "mercantile==1.2.*"

# Bake the Structured Streaming Kafka connector into $SPARK_HOME/jars
# (the course used spark-streaming-kafka-0-8, removed since Spark 3.0)
RUN PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.9-src.zip python -c "from pyspark.sql import SparkSession as S; \
S.builder.master('local[1]').config('spark.jars.ivy','/tmp/ivy').config('spark.jars.packages', \
'org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2').getOrCreate().stop()"
USER root
RUN cp /tmp/ivy/jars/*.jar ${SPARK_HOME}/jars/ && rm -rf /tmp/ivy ${HOME}/.cache \
    && ls ${SPARK_HOME}/jars | grep -E 'kafka|commons-pool2'
USER ${NB_UID}

# pyspark on PYTHONPATH for every entrypoint (not only start.sh), plus shared job libs
ENV PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.9-src.zip:/opt/lab/jobs \
    PYSPARK_PYTHONPATH_SET=1 PYSPARK_PYTHON=/opt/conda/bin/python
WORKDIR /opt/lab
