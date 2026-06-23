#!/usr/bin/env bash

set -eo pipefail

setup_fake_passwd_entry_if_needed() {
    local current_uid
    current_uid="$(id -u)"

    if ! getent passwd "${current_uid}" > /dev/null; then
        local wrapper

        for wrapper in {/usr,}/lib{/*,}/libnss_wrapper.so; do
            if [ -s "${wrapper}" ]; then
                local current_gid
                current_gid="$(id -g)"

                export LD_PRELOAD="${wrapper}"
                export NSS_WRAPPER_PASSWD
                export NSS_WRAPPER_GROUP

                NSS_WRAPPER_PASSWD="$(mktemp)"
                NSS_WRAPPER_GROUP="$(mktemp)"

                printf 'spark:x:%s:%s:spark:%s:/bin/false\n' \
                    "${current_uid}" \
                    "${current_gid}" \
                    "${SPARK_HOME}" > "${NSS_WRAPPER_PASSWD}"

                printf 'spark:x:%s:\n' "${current_gid}" > "${NSS_WRAPPER_GROUP}"

                break
            fi
        done
    fi
}

if [ -z "${JAVA_HOME:-}" ]; then
    JAVA_HOME="$(java -XshowSettings:properties -version 2>&1 > /dev/null | awk '/java.home/ {print $3}')"
    export JAVA_HOME
fi

SPARK_CLASSPATH="${SPARK_CLASSPATH:-}:${SPARK_HOME}/jars/*"

if [ -n "${SPARK_EXTRA_CLASSPATH:-}" ]; then
    SPARK_CLASSPATH="${SPARK_CLASSPATH}:${SPARK_EXTRA_CLASSPATH}"
fi

if [ -n "${HADOOP_HOME:-}" ] && [ -z "${SPARK_DIST_CLASSPATH:-}" ]; then
    SPARK_DIST_CLASSPATH="$("${HADOOP_HOME}/bin/hadoop" classpath)"
    export SPARK_DIST_CLASSPATH
fi

if [ -n "${HADOOP_CONF_DIR:-}" ]; then
    SPARK_CLASSPATH="${HADOOP_CONF_DIR}:${SPARK_CLASSPATH}"
fi

if [ -n "${SPARK_CONF_DIR:-}" ]; then
    SPARK_CLASSPATH="${SPARK_CONF_DIR}:${SPARK_CLASSPATH}"
elif [ -n "${SPARK_HOME:-}" ]; then
    SPARK_CLASSPATH="${SPARK_HOME}/conf:${SPARK_CLASSPATH}"
fi

SPARK_CLASSPATH="${SPARK_CLASSPATH}:${PWD}"

export SPARK_CLASSPATH
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-/usr/bin/python3}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-/usr/bin/python3}"

switch_to_spark_if_root() {
    if [ "$(id -u)" -eq 0 ]; then
        echo gosu spark
    fi
}

case "$1" in
    driver)
        shift 1

        setup_fake_passwd_entry_if_needed

        CMD=(
            "${SPARK_HOME}/bin/spark-submit"
            --conf "spark.driver.bindAddress=${SPARK_DRIVER_BIND_ADDRESS:-0.0.0.0}"
        )

        if [ -n "${SPARK_DRIVER_BIND_ADDRESS:-}" ]; then
            CMD+=(
                --conf "spark.executorEnv.SPARK_DRIVER_POD_IP=${SPARK_DRIVER_BIND_ADDRESS}"
            )
        fi

        CMD+=(
            --deploy-mode client
            "$@"
        )

        exec $(switch_to_spark_if_root) /usr/bin/tini -s -- "${CMD[@]}"
        ;;

    executor)
        shift 1

        setup_fake_passwd_entry_if_needed

        SPARK_EXECUTOR_JAVA_OPTS_ARRAY=()

        for opt in "${!SPARK_JAVA_OPT_@}"; do
            SPARK_EXECUTOR_JAVA_OPTS_ARRAY+=("${!opt}")
        done

        CMD=(
            "${JAVA_HOME}/bin/java"
            "${SPARK_EXECUTOR_JAVA_OPTS_ARRAY[@]}"
            -Xms"${SPARK_EXECUTOR_MEMORY}"
            -Xmx"${SPARK_EXECUTOR_MEMORY}"
            -cp "${SPARK_CLASSPATH}:${SPARK_DIST_CLASSPATH:-}"
            org.apache.spark.scheduler.cluster.k8s.KubernetesExecutorBackend
            --driver-url "${SPARK_DRIVER_URL}"
            --executor-id "${SPARK_EXECUTOR_ID}"
            --cores "${SPARK_EXECUTOR_CORES}"
            --app-id "${SPARK_APPLICATION_ID}"
            --hostname "${SPARK_EXECUTOR_POD_IP}"
            --resourceProfileId "${SPARK_RESOURCE_PROFILE_ID:-0}"
            --podName "${SPARK_EXECUTOR_POD_NAME}"
        )

        exec $(switch_to_spark_if_root) /usr/bin/tini -s -- "${CMD[@]}"
        ;;

    *)
        exec "$@"
        ;;
esac
