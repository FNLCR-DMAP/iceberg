#!/usr/bin/env python3
"""Download the correct Apache Iceberg Spark runtime JAR(s) for this repo.

Logic:
- Detect PySpark version -> derive Spark minor (e.g. 3.4)
- Infer Scala binary version from bundled spark-core jar (2.12 vs 2.13)
- Map Spark minor to a recommended Iceberg runtime version
- Download if missing into ./jars

Safe to run multiple times (idempotent).
"""
from __future__ import annotations
import os
import sys
import glob
import urllib.request
import hashlib

try:
    import pyspark  # type: ignore
except ImportError:  # pragma: no cover
    print(
        "PySpark not installed. Install dependencies first:\n"
        "  pip install -r requirements.txt"
    )
    sys.exit(1)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
JARS_DIR = os.path.join(BASE_DIR, 'jars')
os.makedirs(JARS_DIR, exist_ok=True)

PYSPARK_VERSION = pyspark.__version__
SPARK_MINOR = '.'.join(PYSPARK_VERSION.split('.')[:2])  # 3.4, 3.5, etc.


def detect_scala_binary() -> str:
    jars_dir = os.path.join(os.path.dirname(pyspark.__file__), 'jars')
    for core in glob.glob(os.path.join(jars_dir, 'spark-core_*')):
        if '_2.13' in core:
            return '2.13'
        if '_2.12' in core:
            return '2.12'
    # Fallback
    return '2.12'


def recommended_iceberg_version(spark_minor: str) -> str:
    # Simple mapping; adjust as needed
    if spark_minor.startswith('3.5'):
        return '1.10.0'
    return '1.4.2'


def build_candidate_name(
    spark_minor: str, scala_bin: str, iceberg_version: str
) -> str:
    return (
        f"iceberg-spark-runtime-{spark_minor}_{scala_bin}-"
        f"{iceberg_version}.jar"
    )


def download(url: str, dest: str) -> None:
    print(f"Downloading {url} -> {dest}")
    with urllib.request.urlopen(url) as r, open(dest, 'wb') as f:
        f.write(r.read())


def sha256sum(path: str) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def main():
    scala_bin = detect_scala_binary()
    iceberg_version = recommended_iceberg_version(SPARK_MINOR)

    jar_name = build_candidate_name(SPARK_MINOR, scala_bin, iceberg_version)
    dest_path = os.path.join(JARS_DIR, jar_name)

    # Maven Central URL pattern
    base_url = (
        "https://repo1.maven.org/maven2/org/apache/iceberg/"
        f"iceberg-spark-runtime-{SPARK_MINOR}_{scala_bin}/"
        f"{iceberg_version}/"
        f"iceberg-spark-runtime-{SPARK_MINOR}_{scala_bin}-"
        f"{iceberg_version}.jar"
    )

    if os.path.exists(dest_path):
        print(f"✅ JAR already present: {dest_path}")
    else:
        try:
            download(base_url, dest_path)
            print(f"✅ Downloaded {jar_name}")
        except Exception as e:
            print("❌ Failed to download primary JAR:", e)
            print(
                "Check Spark/Iceberg version compatibility or adjust mapping."
            )
            sys.exit(2)

    print(
        "Spark minor:", SPARK_MINOR,
        "Scala:", scala_bin,
        "Iceberg:", iceberg_version,
    )
    print("JAR ready at:", dest_path)


if __name__ == '__main__':  # pragma: no cover
    main()
