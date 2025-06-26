#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

FROM azul/zulu-openjdk:17-jre-headless

# Set up the user and group
RUN  set -xeu && \
     groupadd iceberg --gid 1000 && \
     useradd iceberg --uid 1000 --gid 1000 --create-home && \
     apt-get update && \
     apt-get install -y --no-install-recommends curl && \
     rm -rf /var/lib/apt/lists/*

# Working directory for the application
WORKDIR /usr/lib/iceberg-test
ENV SPARK_VERSION=4.0.0

# Copy the JAR file directly to the target location
COPY --chown=iceberg:iceberg spark/v4.0/spark-runtime/build/libs/iceberg-spark-runtime-*.jar /usr/lib/iceberg-test/iceberg-spark-runtime.jar

# Download Spark
#RUN apt-get update && apt-get install -y curl unzip python3 python3-pip sqlite3
#
#RUN curl -sL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz | tar xz -C /usr/lib/iceberg-spark/
#ENV SPARK_HOME=/usr/lib/iceberg-spark/spark-${SPARK_VERSION}-bin-hadoop3
#ENV PATH=$PATH:$SPARK_HOME/bin

# Install Hive
RUN #apt-get install -y hive libpq-dev

# Install Postgres

# Copy Iceberg Spark runtime JARs (assumes local build volume mount)
#COPY iceberg-spark-runtime/ /opt/iceberg-jars/

# Copy catalog starter scripts
#COPY catalog-scripts/ /opt/catalog-scripts/
RUN #chmod +x /opt/catalog-scripts/*.sh

# Set Spark defaults (e.g., prewire REST catalog or hive)
#COPY spark-defaults.conf $SPARK_HOME/conf/

# Entry point lets you exec arbitrary commands like:
# docker run iceberg-env bash /opt/catalog-scripts/start-rest.sh