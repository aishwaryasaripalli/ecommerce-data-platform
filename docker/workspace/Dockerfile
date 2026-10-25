# Same Jupyter + PySpark image you already ran, plus the Kafka client library
# baked in so you never have to `pip install` inside a running container again.
FROM jupyter/pyspark-notebook:latest

USER ${NB_UID}
RUN pip install --no-cache-dir kafka-python
