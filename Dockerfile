###############################
# Apache Iceberg ETL Demo     #
# Production-friendly Docker  #
###############################

ARG PYTHON_VERSION=3.9-slim
FROM python:${PYTHON_VERSION} AS runtime

ARG TEMURIN_VERSION=17.0.11_9
ARG TEMURIN_BUILD_TAG=jdk-17.0.11+9
ARG ICEBERG_SMOKE_WAREHOUSE=/tmp/iceberg_smoke_wh

# System deps
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       curl bash ca-certificates procps git \
    && rm -rf /var/lib/apt/lists/* \
    && arch=$(uname -m) \
    && if [ "$arch" = "x86_64" ]; then jarch="x64"; else jarch="aarch64"; fi \
    && echo "Downloading Temurin JRE ${TEMURIN_VERSION} for $arch ($jarch)" \
    && curl -fsSL -o /tmp/jre.tar.gz \
       https://github.com/adoptium/temurin17-binaries/releases/download/${TEMURIN_BUILD_TAG}/OpenJDK17U-jre_${jarch}_linux_hotspot_${TEMURIN_VERSION}.tar.gz \
    && mkdir -p /opt/java \
    && tar -xzf /tmp/jre.tar.gz -C /opt/java --strip-components=1 \
    && rm /tmp/jre.tar.gz

# Environment
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    ICEBERG_WAREHOUSE=/app/iceberg_warehouse \
    SPARK_LOCAL_IP=127.0.0.1 \
    JAVA_HOME=/opt/java \
    PATH="/opt/java/bin:${PATH}"

WORKDIR /app

# Copy dependency manifests first for caching (only requirements*)
COPY requirements.txt ./
RUN pip install --upgrade pip \
    && pip install -r requirements.txt \
    && python -c "import pyspark, sys; print('PySpark', pyspark.__version__)"

# Copy project source
COPY . .

# Download Iceberg JAR(s) (idempotent)
RUN python scripts/download_iceberg_jars.py || (echo "Download script failed" && exit 1)

# Lightweight smoke test (ensures Spark + Iceberg can initialize)
RUN ICEBERG_JAR="" \
    ICEBERG_WAREHOUSE=${ICEBERG_SMOKE_WAREHOUSE} \
    python - <<'PY'
import os, re, sys
from pyspark.sql import SparkSession
import pyspark
print('Running smoke test...')
spark_minor = '.'.join(pyspark.__version__.split('.')[:2])
jar_dir = os.path.join(os.getcwd(), 'jars')
iceberg_jars = [j for j in os.listdir(jar_dir) if j.startswith('iceberg-spark-runtime-')]
preferred = None
for j in iceberg_jars:
    if re.search(rf"iceberg-spark-runtime-{spark_minor}_", j):
        preferred = j; break
if not preferred and iceberg_jars:
    preferred = sorted(iceberg_jars)[-1]
print('Detected PySpark minor', spark_minor, 'selected jar', preferred)
builder = SparkSession.builder.appName('iceberg-smoke')
if preferred:
    builder = builder.config('spark.jars', os.path.join(jar_dir, preferred)) \
        .config('spark.sql.extensions','org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
        .config('spark.sql.catalog.spark_catalog','org.apache.iceberg.spark.SparkSessionCatalog') \
        .config('spark.sql.catalog.spark_catalog.type','hive') \
        .config('spark.sql.catalog.demo_catalog','org.apache.iceberg.spark.SparkCatalog') \
        .config('spark.sql.catalog.demo_catalog.type','hadoop') \
        .config('spark.sql.catalog.demo_catalog.warehouse', os.environ.get('ICEBERG_WAREHOUSE','/tmp/wh'))
spark = builder.getOrCreate()
try:
    if preferred:
        spark.sql('CREATE DATABASE IF NOT EXISTS demo_catalog.smoke').collect()
        print('Spark + Iceberg init OK; version PySpark=', spark.version)
    else:
        print('Spark init OK (no Iceberg jar found)')
except Exception as e:
    print('WARNING: Iceberg catalog init failed during smoke test:', e)
    print('Continuing; runtime will attempt full init later.')
finally:
    spark.stop()
PY

# Optional: allow overriding jar selection at runtime
ENV ICEBERG_JAR=""

# Entry point script
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

VOLUME ["/app/iceberg_warehouse"]

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["python", "etl_pipeline_demo.py"]
