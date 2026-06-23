# syntax=docker/dockerfile:1.7

# ============================================================
# Apache Spark 3.5.6 - Scala 2.12 - Ubuntu 24.04
#
# Goal:
#   Minimal image, as close as possible to the official apache/spark image,
#   but using a newer OS base and Python runtime.
#
# Runtime:
#   - Ubuntu 24.04
#   - OpenJDK 17
#   - Python 3.12
#   - Spark 3.5.6 prebuilt distribution for Hadoop 3 / Scala 2.12
#
# This image intentionally does NOT include:
#   - hadoop-aws
#   - AWS SDK bundle
#   - Delta Lake
#   - Iceberg
#   - JDBC drivers
#
# Those dependencies should continue to be injected exactly as in the
# current PoC, using initContainers, mounted volumes, or spark.jars.
# ============================================================

FROM ubuntu:24.04

ARG spark_uid=185
ARG SPARK_VERSION=3.5.6
ARG SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop3

ENV DEBIAN_FRONTEND=noninteractive

ENV SPARK_HOME=/opt/spark
ENV SPARK_CONF_DIR=/opt/spark/conf
ENV JAVA_HOME=/opt/java/openjdk
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${JAVA_HOME}/bin:${PATH}"

# Same Spark tarball and GPG key pattern used by the official Spark Dockerfiles.
ENV SPARK_TGZ_URL=https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz
ENV SPARK_TGZ_ASC_URL=https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz.asc
ENV GPG_KEY=0FE4571297AB84440673665669600C8338F65970

# ------------------------------------------------------------
# Base OS packages.
#
# This is intentionally very close to the official image package set,
# plus OpenJDK 17 and Python 3 from Ubuntu 24.04.
# ------------------------------------------------------------

RUN set -ex; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        gnupg2 \
        wget \
        tini \
        libc6 \
        libpam-modules \
        krb5-user \
        libnss3 \
        procps \
        net-tools \
        gosu \
        libnss-wrapper \
        openjdk-17-jdk-headless \
        python3 \
        python3-pip \
        python3-venv; \
    rm -rf /var/lib/apt/lists/*

# Create a stable JAVA_HOME path independent of the Debian/Ubuntu arch suffix.
RUN set -ex; \
    mkdir -p /opt/java; \
    ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" "${JAVA_HOME}"; \
    java -version; \
    python3 --version; \
    python3 --version | grep -E '^Python 3\.12\.'

# ------------------------------------------------------------
# Create Spark user and base directories.
# UID 185 matches the official Apache Spark image convention.
# ------------------------------------------------------------

RUN set -ex; \
    groupadd --system --gid=${spark_uid} spark; \
    useradd --system --uid=${spark_uid} --gid=spark spark; \
    mkdir -p /opt/spark; \
    mkdir -p /opt/spark/python; \
    mkdir -p /opt/spark/examples; \
    mkdir -p /opt/spark/work-dir; \
    chmod g+w /opt/spark/work-dir; \
    touch /opt/spark/RELEASE; \
    chown -R spark:spark /opt/spark; \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su

# ------------------------------------------------------------
# Download and verify Apache Spark.
#
# This follows the same idea as the official image:
#   - download Spark binary tarball
#   - download .asc signature
#   - import release GPG key
#   - verify the tarball
#   - copy Spark runtime folders into /opt/spark
# ------------------------------------------------------------

RUN set -ex; \
    SPARK_TMP="$(mktemp -d)"; \
    cd "${SPARK_TMP}"; \
    wget -nv -O spark.tgz "${SPARK_TGZ_URL}"; \
    wget -nv -O spark.tgz.asc "${SPARK_TGZ_ASC_URL}"; \
    GNUPGHOME="$(mktemp -d)"; \
    export GNUPGHOME; \
    gpg --batch --keyserver hkps://keys.openpgp.org --recv-key "${GPG_KEY}" || \
    gpg --batch --keyserver hkps://keyserver.ubuntu.com --recv-keys "${GPG_KEY}"; \
    gpg --batch --verify spark.tgz.asc spark.tgz; \
    gpgconf --kill all; \
    rm -rf "${GNUPGHOME}" spark.tgz.asc; \
    tar -xf spark.tgz --strip-components=1; \
    chown -R spark:spark .; \
    mv jars /opt/spark/; \
    mv RELEASE /opt/spark/; \
    mv bin /opt/spark/; \
    mv sbin /opt/spark/; \
    mv examples /opt/spark/; \
    ln -s "$(basename /opt/spark/examples/jars/spark-examples_*.jar)" /opt/spark/examples/jars/spark-examples.jar; \
    mv kubernetes/dockerfiles/spark/decom.sh /opt/; \
    mv kubernetes/tests /opt/spark/; \
    mv data /opt/spark/; \
    mv python/pyspark /opt/spark/python/pyspark/; \
    mv python/lib /opt/spark/python/lib/; \
    mv R /opt/spark/; \
    chmod a+x /opt/decom.sh; \
    cd /; \
    rm -rf "${SPARK_TMP}"

COPY entrypoint.sh /opt/entrypoint.sh

RUN set -ex; \
    chmod a+x /opt/entrypoint.sh; \
    chown spark:spark /opt/entrypoint.sh; \
    chown -R spark:spark /opt/spark

WORKDIR /opt/spark/work-dir

USER spark

ENTRYPOINT ["/opt/entrypoint.sh"]

CMD ["/bin/bash"]
