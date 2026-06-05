#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# prepare_lakehouse_jars_local.sh
#
# Downloads all JARs required by the Spark Lakehouse PoC into a local directory.
# It does not use S3, AWS CLI, Docker, Spark, Ivy or Python.
#
# Expected layout:
#   ${LOCAL_JARS_DIR}/spark/    -> generic Spark runtime JARs used by Spark Master/Workers/Thrift
#   ${LOCAL_JARS_DIR}/connect/  -> JARs used only by Spark Connect
#   ${LOCAL_JARS_DIR}/hive/     -> JARs used by Hive Metastore process
#   ${LOCAL_JARS_DIR}/ivy/      -> frozen Hive 3.1.3 metastore client dependency set
#   ${LOCAL_JARS_DIR}/manifest/ -> generated manifests and checksums
#
# The docker-compose files mount ${LOCAL_JARS_DIR} read-only into:
#   /opt/lakehouse/jars
#
# Required files:
#   - .env, or set ENV_FILE=/path/to/env
#   - hive-ivy-urls.txt, or set HIVE_IVY_URL_MANIFEST=/path/to/file
# -----------------------------------------------------------------------------

ENV_FILE="${ENV_FILE:-.env}"

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

require_command() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "ERROR: required command not found: ${cmd}" >&2
    exit 1
  fi
}

load_env() {
  if [[ -f "${ENV_FILE}" ]]; then
    log "Using env file: ${ENV_FILE}"
    set -a
    # shellcheck source=/dev/null
    source "${ENV_FILE}"
    set +a
  else
    log "Env file not found: ${ENV_FILE}. Using script defaults where possible."
  fi
}

maven_url() {
  local group_id="$1"
  local artifact_id="$2"
  local version="$3"
  local classifier="${4:-}"
  local group_path="${group_id//./\/}"
  local filename

  if [[ -n "${classifier}" ]]; then
    filename="${artifact_id}-${version}-${classifier}.jar"
  else
    filename="${artifact_id}-${version}.jar"
  fi

  printf '%s/%s/%s/%s/%s' \
    "${MAVEN_BASE_URL%/}" \
    "${group_path}" \
    "${artifact_id}" \
    "${version}" \
    "${filename}"
}

filename_from_url() {
  local url="$1"
  printf '%s\n' "${url##*/}"
}

download_url() {
  local url="$1"
  local target="$2"

  mkdir -p "$(dirname "${target}")"

  if [[ -s "${target}" ]]; then
    log "Already exists: ${target}"
    return 0
  fi

  log "Downloading ${url}"
  rm -f "${target}.tmp"
  curl -fL --retry 5 --retry-delay 2 --connect-timeout 20 --max-time 300 \
    -o "${target}.tmp" \
    "${url}"

  if [[ ! -s "${target}.tmp" ]]; then
    echo "ERROR: downloaded empty file: ${url}" >&2
    rm -f "${target}.tmp"
    exit 1
  fi

  mv "${target}.tmp" "${target}"
}

download_coord() {
  local group_id="$1"
  local artifact_id="$2"
  local version="$3"
  local dest_dir="$4"
  local classifier="${5:-}"
  local url filename

  url="$(maven_url "${group_id}" "${artifact_id}" "${version}" "${classifier}")"
  filename="$(filename_from_url "${url}")"
  download_url "${url}" "${dest_dir}/${filename}"
}

download_spark_jars() {
  log "Downloading Spark runtime JARs into ${SPARK_JARS_DIR}"

  download_coord "org.apache.hadoop" "hadoop-aws" "${HADOOP_AWS_SPARK_VERSION}" "${SPARK_JARS_DIR}"
  download_coord "com.amazonaws" "aws-java-sdk-bundle" "${AWS_SDK_BUNDLE_SPARK_VERSION}" "${SPARK_JARS_DIR}"
  download_coord "io.delta" "delta-spark_2.12" "${DELTA_VERSION}" "${SPARK_JARS_DIR}"
  download_coord "io.delta" "delta-storage" "${DELTA_VERSION}" "${SPARK_JARS_DIR}"
  download_coord "org.apache.iceberg" "iceberg-spark-runtime-3.5_2.12" "${ICEBERG_VERSION}" "${SPARK_JARS_DIR}"

}

download_connect_jars() {
  log "Downloading Spark Connect JARs into ${CONNECT_JARS_DIR}"

  # Spark Connect is not bundled in the apache/spark:3.5.6 image.
  # Do not use --packages at runtime: keep these JARs in the local repository.
  # The filenames intentionally match the Ivy-style names that Spark creates when
  # resolving org.apache.spark:spark-connect_2.12:3.5.6.
  download_url     "$(maven_url "org.apache.spark" "spark-connect_2.12" "${SPARK_CONNECT_VERSION}")"     "${CONNECT_JARS_DIR}/org.apache.spark_spark-connect_2.12-${SPARK_CONNECT_VERSION}.jar"
  download_url     "$(maven_url "org.spark-project.spark" "unused" "${SPARK_CONNECT_UNUSED_VERSION}")"     "${CONNECT_JARS_DIR}/org.spark-project.spark_unused-${SPARK_CONNECT_UNUSED_VERSION}.jar"
}

download_hive_jars() {
  log "Downloading Hive Metastore auxiliary JARs into ${HIVE_JARS_DIR}"

  download_coord "org.postgresql" "postgresql" "${POSTGRES_JDBC_VERSION}" "${HIVE_JARS_DIR}"
  download_coord "org.apache.hadoop" "hadoop-aws" "${HADOOP_AWS_HIVE_VERSION}" "${HIVE_JARS_DIR}"
  download_coord "com.amazonaws" "aws-java-sdk-bundle" "${AWS_SDK_BUNDLE_HIVE_VERSION}" "${HIVE_JARS_DIR}"
}

download_ivy_jars() {
  local manifest_file="${HIVE_IVY_URL_MANIFEST}"

  if [[ ! -f "${manifest_file}" ]]; then
    echo "ERROR: Hive/Ivy URL manifest not found: ${manifest_file}" >&2
    echo "Place hive-ivy-urls.txt next to this script or set HIVE_IVY_URL_MANIFEST=/path/to/file" >&2
    exit 1
  fi

  log "Downloading frozen Hive/Ivy transitive JAR set from ${manifest_file} into ${IVY_JARS_DIR}"

  local line url filename count
  count=0
  while IFS= read -r line || [[ -n "${line}" ]]; do
    url="${line%%#*}"
    url="$(printf '%s' "${url}" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"

    [[ -z "${url}" ]] && continue
    [[ "${url}" == *.jar ]] || {
      echo "ERROR: invalid non-JAR URL in ${manifest_file}: ${url}" >&2
      exit 1
    }

    filename="$(filename_from_url "${url}")"
    download_url "${url}" "${IVY_JARS_DIR}/${filename}"
    count=$((count + 1))
  done < "${manifest_file}"

  log "Processed ${count} Hive/Ivy URLs"
}

validate_repository() {
  log "Validating local JAR repository"

  ls "${SPARK_JARS_DIR}"/hadoop-aws-*.jar >/dev/null 2>&1 || { echo "ERROR: missing Spark hadoop-aws JAR" >&2; exit 1; }
  ls "${SPARK_JARS_DIR}"/aws-java-sdk-bundle-*.jar >/dev/null 2>&1 || { echo "ERROR: missing Spark AWS SDK bundle JAR" >&2; exit 1; }
  ls "${SPARK_JARS_DIR}"/delta-spark_2.12-*.jar >/dev/null 2>&1 || { echo "ERROR: missing Delta Spark JAR" >&2; exit 1; }
  ls "${SPARK_JARS_DIR}"/iceberg-spark-runtime-3.5_2.12-*.jar >/dev/null 2>&1 || { echo "ERROR: missing Iceberg Spark runtime JAR" >&2; exit 1; }
  ls "${CONNECT_JARS_DIR}"/org.apache.spark_spark-connect_2.12-*.jar >/dev/null 2>&1 || { echo "ERROR: missing Spark Connect JAR" >&2; exit 1; }
  ls "${CONNECT_JARS_DIR}"/org.spark-project.spark_unused-*.jar >/dev/null 2>&1 || { echo "ERROR: missing Spark Connect unused marker JAR" >&2; exit 1; }

  ls "${HIVE_JARS_DIR}"/postgresql-*.jar >/dev/null 2>&1 || { echo "ERROR: missing PostgreSQL JDBC JAR" >&2; exit 1; }
  ls "${HIVE_JARS_DIR}"/hadoop-aws-*.jar >/dev/null 2>&1 || { echo "ERROR: missing Hive hadoop-aws JAR" >&2; exit 1; }
  ls "${HIVE_JARS_DIR}"/aws-java-sdk-bundle-*.jar >/dev/null 2>&1 || { echo "ERROR: missing Hive AWS SDK bundle JAR" >&2; exit 1; }

  ls "${IVY_JARS_DIR}"/hive-metastore-*.jar >/dev/null 2>&1 || { echo "ERROR: missing Hive metastore client JAR in ivy directory" >&2; exit 1; }
  ls "${IVY_JARS_DIR}"/hive-exec-*.jar >/dev/null 2>&1 || { echo "ERROR: missing Hive exec JAR in ivy directory" >&2; exit 1; }
}

create_manifests() {
  log "Creating manifests in ${MANIFEST_DIR}"
  mkdir -p "${MANIFEST_DIR}"

  find "${LOCAL_JARS_DIR}" -type f -name '*.jar' -printf '%P\n' | sort > "${MANIFEST_DIR}/jars-relative.txt"
  find "${SPARK_JARS_DIR}" -type f -name '*.jar' -printf '%f\n' | sort > "${MANIFEST_DIR}/spark-jars.txt"
  find "${HIVE_JARS_DIR}" -type f -name '*.jar' -printf '%f\n' | sort > "${MANIFEST_DIR}/hive-jars.txt"
  find "${CONNECT_JARS_DIR}" -type f -name '*.jar' -printf '%f\n' | sort > "${MANIFEST_DIR}/connect-jars.txt"
  find "${IVY_JARS_DIR}" -type f -name '*.jar' -printf '%f\n' | sort > "${MANIFEST_DIR}/ivy-jars.txt"

  if command -v sha256sum >/dev/null 2>&1; then
    (
      cd "${LOCAL_JARS_DIR}"
      find spark connect hive ivy -type f -name '*.jar' -print0 | sort -z | xargs -0 sha256sum
    ) > "${MANIFEST_DIR}/sha256sum.txt"
  fi

  {
    echo "Generated at: $(date -Is)"
    echo "Maven base URL: ${MAVEN_BASE_URL}"
    echo "Local JAR directory: ${LOCAL_JARS_DIR}"
    echo "Hive/Ivy URL manifest: ${HIVE_IVY_URL_MANIFEST}"
    echo "Hive version: ${HIVE_VERSION}"
    echo "Delta version: ${DELTA_VERSION}"
    echo "Iceberg version: ${ICEBERG_VERSION}"
    echo "Spark Connect version: ${SPARK_CONNECT_VERSION}"
    echo "Spark Connect unused marker version: ${SPARK_CONNECT_UNUSED_VERSION}"
    echo "Spark hadoop-aws version: ${HADOOP_AWS_SPARK_VERSION}"
    echo "Spark aws-java-sdk-bundle version: ${AWS_SDK_BUNDLE_SPARK_VERSION}"
    echo "Hive hadoop-aws version: ${HADOOP_AWS_HIVE_VERSION}"
    echo "Hive aws-java-sdk-bundle version: ${AWS_SDK_BUNDLE_HIVE_VERSION}"
    echo "PostgreSQL JDBC version: ${POSTGRES_JDBC_VERSION}"
    echo
    echo "Counts:"
    printf 'spark=%s\n' "$(wc -l < "${MANIFEST_DIR}/spark-jars.txt")"
    printf 'hive=%s\n' "$(wc -l < "${MANIFEST_DIR}/hive-jars.txt")"
    printf 'connect=%s\n' "$(wc -l < "${MANIFEST_DIR}/connect-jars.txt")"
    printf 'ivy=%s\n' "$(wc -l < "${MANIFEST_DIR}/ivy-jars.txt")"
    printf 'total=%s\n' "$(wc -l < "${MANIFEST_DIR}/jars-relative.txt")"
  } > "${MANIFEST_DIR}/summary.txt"

  cat "${MANIFEST_DIR}/summary.txt"
}

main() {
  require_command curl
  require_command sed
  require_command find
  require_command sort

  load_env

  MAVEN_BASE_URL="${MAVEN_BASE_URL:-https://repo1.maven.org/maven2}"
  LOCAL_JARS_DIR="${LOCAL_JARS_DIR:-./lakehouse-jars}"
  HIVE_IVY_URL_MANIFEST="${HIVE_IVY_URL_MANIFEST:-hive-ivy-urls.txt}"

  HIVE_VERSION="${HIVE_VERSION:-3.1.3}"
  DELTA_VERSION="${DELTA_VERSION:-3.3.2}"
  ICEBERG_VERSION="${ICEBERG_VERSION:-1.9.2}"
  SPARK_CONNECT_VERSION="${SPARK_CONNECT_VERSION:-3.5.6}"
  SPARK_CONNECT_UNUSED_VERSION="${SPARK_CONNECT_UNUSED_VERSION:-1.0.0}"

  HADOOP_AWS_SPARK_VERSION="${HADOOP_AWS_SPARK_VERSION:-3.3.4}"
  AWS_SDK_BUNDLE_SPARK_VERSION="${AWS_SDK_BUNDLE_SPARK_VERSION:-1.12.262}"
  HADOOP_AWS_HIVE_VERSION="${HADOOP_AWS_HIVE_VERSION:-3.1.0}"
  AWS_SDK_BUNDLE_HIVE_VERSION="${AWS_SDK_BUNDLE_HIVE_VERSION:-1.11.271}"
  POSTGRES_JDBC_VERSION="${POSTGRES_JDBC_VERSION:-42.7.11}"

  SPARK_JARS_DIR="${LOCAL_JARS_DIR}/spark"
  CONNECT_JARS_DIR="${LOCAL_JARS_DIR}/connect"
  HIVE_JARS_DIR="${LOCAL_JARS_DIR}/hive"
  IVY_JARS_DIR="${LOCAL_JARS_DIR}/ivy"
  MANIFEST_DIR="${LOCAL_JARS_DIR}/manifest"

  if [[ "${CLEAN_JARS:-false}" == "true" ]]; then
    log "CLEAN_JARS=true: removing ${LOCAL_JARS_DIR}"
    rm -rf "${LOCAL_JARS_DIR}"
  fi

  mkdir -p "${SPARK_JARS_DIR}" "${CONNECT_JARS_DIR}" "${HIVE_JARS_DIR}" "${IVY_JARS_DIR}" "${MANIFEST_DIR}"

  log "Preparing local JAR repository at ${LOCAL_JARS_DIR}"
  download_spark_jars
  download_connect_jars
  download_hive_jars
  download_ivy_jars
  validate_repository
  create_manifests
  log "Done. Local JAR repository is ready at ${LOCAL_JARS_DIR}"
}

main "$@"
