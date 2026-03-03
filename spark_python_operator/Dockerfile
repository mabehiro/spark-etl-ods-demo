ARG ELYRA_TAG=3.2.3
ARG ODH_TAG=s3.3.1-h3.3.4_v0.1.1
FROM docker.io/elyra/kernel-spark-py:${ELYRA_TAG} AS elyra_kernel

FROM quay.io/opendatahub-contrib/pyspark:${ODH_TAG}

ARG MYSQL_CONNECTOR_VERSION=8.0.33

USER root

# Reuse Elyra kernel launcher scripts while keeping Spark/Hadoop stack from ODH image.
COPY --from=elyra_kernel /usr/local/bin/kernel-launchers /usr/local/bin/kernel-launchers

RUN set -eux; \
    python -m pip install --no-cache-dir \
      ipykernel==6.29.5 \
      jupyter_client==8.6.3 \
      pycryptodomex==3.20.0; \
    curl -fsSL -o "/opt/spark/jars/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.jar" \
      "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_CONNECTOR_VERSION}/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.jar"; \
    ln -sf "/opt/spark/jars/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.jar" /opt/spark/jars/mysql-connector-j.jar; \
    chmod -R a+rX /usr/local/bin/kernel-launchers /opt/spark/jars

ENV SPARK_HOME=/opt/spark
