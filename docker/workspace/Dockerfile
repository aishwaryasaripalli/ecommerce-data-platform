# Same Jupyter + PySpark image you already ran, plus the Kafka client library
# baked in so you never have to `pip install` inside a running container again.
FROM jupyter/pyspark-notebook:latest

# Phase 3: connector jars Spark needs to read Kafka, write Delta, and talk to MinIO (S3).
# Versions must match the image's Spark 3.5.0 / Scala 2.12 / Hadoop 3.3.4. Baking them in
# means jobs start instantly and work offline (no `--packages` download on every run).
USER root
ARG MAVEN=https://repo1.maven.org/maven2
RUN cd "${SPARK_HOME}/jars" && \
    for jar in \
      org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
      org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
      org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
      io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
      io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
      org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar ; \
    do curl -fsSL -O "${MAVEN}/${jar}" || exit 1; done

USER ${NB_UID}
# delta-spark gives Python the DeltaTable API; --no-deps keeps the image's own pyspark
RUN pip install --no-cache-dir kafka-python boto3 && \
    pip install --no-cache-dir --no-deps delta-spark==3.2.0

# Let plain `python script.py` import pyspark (notebooks already could),
# plus our own src/ so jobs can share code with `from common.spark_session import ...`
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:/home/jovyan/src"
