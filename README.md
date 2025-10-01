# Apache Iceberg ETL Demo (Lean Version)

Single, working demonstration of Apache Iceberg features (time travel, branching, schema evolution, ACID) via the script `etl_pipeline_demo.py`.

## Quick Start
```bash
python -m venv .venv && . .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
python scripts/download_iceberg_jars.py
python etl_pipeline_demo.py
```

## What You Get
- Raw → Intermediate → Final metric tables
- Time travel snapshot comparison
- Branching (dev + staging) using Iceberg refs
- Schema evolution (adds `discount_amount`)
- Snapshot / files metadata inspection

## Requirements
Version-pinned baseline (reproducible):

| Component | Version | Pin / Artifact | Notes |
|-----------|---------|----------------|-------|
| Python | 3.9.x | `environment-minimal.yml` | Tested with 3.9.23 |
| Java (OpenJDK) | 11.x | `java -version` | JDK 17 also works |
| PySpark | 3.4.1 | `pyspark==3.4.1` | Ships Scala 2.12 jars |
| Scala runtime | 2.12.17 | `scala-library-2.12.17.jar` | Must match `_2.12` in Iceberg JAR name |
| Iceberg runtime | 1.4.2 | `iceberg-spark-runtime-3.4_2.12-1.4.2.jar` | Auto-downloaded |
| PyIceberg | 0.5.1 | `pyiceberg==0.5.1` | Python client utilities |
| Pandas | 2.0.3 | `pandas==2.0.3` | Convenience only |
| Jupyter (meta) | 1.0.0 | `jupyter==1.0.0` | Optional (not used by script) |

The `scripts/download_iceberg_jars.py` script picks the correct runtime JAR for the pinned PySpark/Scala combination.

## Adjusting Versions
| Target | New Versions | Actions |
|--------|--------------|---------|
| Spark + Iceberg upgrade | PySpark 3.5.1 + Iceberg 1.10.0 | `pip install pyspark==3.5.1`; ensure Scala 2.13; download `iceberg-spark-runtime-3.5_2.13-1.10.0.jar` |
| PyIceberg only | 0.5.2+ | Bump `pyiceberg` pin in `requirements.txt` / env file |
| Java | 17 | Use JDK 17; no code changes expected |

After changing PySpark version always re-run the download script and remove outdated JARs to avoid mixed Scala artifacts.

## More Detail
See `SETUP.md` for: environment creation, troubleshooting, CI hints, and optional Hadoop-only vs Hive-backed catalog notes.

## Cleanup
```bash
rm -rf iceberg_warehouse *.pyc .venv
```

Enjoy exploring Iceberg! If something breaks, open an issue with your Spark + PySpark + Scala + Iceberg versions.