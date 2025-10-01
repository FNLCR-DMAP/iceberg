# Iceberg ETL Demo - Reproducible Setup Guide

This repository provides a single, working Python script (`etl_pipeline_demo.py`) demonstrating Apache Iceberg features (time travel, branching, schema evolution, ACID) on top of Spark.

## 1. Requirements

- macOS / Linux (Windows WSL works)
- Java 11+ (OpenJDK recommended)
- Python 3.9–3.11
- ~2 GB free disk space

## 2. Create and Activate Environment

Using `venv` (default, Python 3.9 recommended to match `environment-minimal.yml`):
```bash
python3.9 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
```

Or Conda (matches provided env file):
```bash
conda env create -f environment-minimal.yml
conda activate iceberg-demo
```

## 3. Install Python Dependencies

```bash
pip install -r requirements.txt
```

## 4. Verify Java & Spark Compatibility

Pinned stack (reproducible baseline):

| Component | Version | Pin / Artifact | Notes |
|-----------|---------|----------------|-------|
| Python | 3.9.x | `environment-minimal.yml` | Tested with 3.9.23 |
| Java (OpenJDK) | 11.x | `java -version` | JDK 17 works but 11 preferred |
| PySpark | 3.4.1 | `pyspark==3.4.1` | Scala 2.12 ABI |
| Scala runtime | 2.12.17 | `scala-library-2.12.17.jar` | Must match Iceberg JAR suffix `_2.12` |
| Iceberg runtime | 1.4.2 | `iceberg-spark-runtime-3.4_2.12-1.4.2.jar` | Downloaded via script |
| PyIceberg | 0.5.1 | `pyiceberg==0.5.1` | Python client utilities |
| Pandas | 2.0.3 | `pandas==2.0.3` | Optional convenience |
| Jupyter (meta) | 1.0.0 | `jupyter==1.0.0` | Only if using notebooks |

Why these: PySpark 3.4.x couples to Scala 2.12. Using a Scala 2.13 Iceberg runtime JAR with this stack produces `NoClassDefFoundError: scala/collection/SeqOps`.

Check Java:
```bash
java -version
```
Should report something like: `openjdk version "11.x"`.

## 5. Download Correct Iceberg Runtime JAR

Run the helper script below (creates `jars/` if missing):
```bash
python scripts/download_iceberg_jars.py
```
This will fetch (if absent):
- `iceberg-spark-runtime-3.4_2.12-1.4.2.jar`
It prints detected Spark minor & Scala binary versions. After upgrading PySpark, rerun the script to fetch a matching JAR.

## 6. Run the Demo

```bash
python etl_pipeline_demo.py
```
Expected output includes:
- Creation of raw/intermediate/final Iceberg tables
- Time travel snapshot comparison
- Branch creation (dev, staging)
- Schema evolution adding `discount_amount`
- Metadata listing (files, snapshots)

## 7. Common Issues & Fixes

| Symptom | Cause | Fix |
|---------|-------|-----|
| ClassNotFound: org.apache.iceberg.* | Missing or wrong JAR | Re-run download script; ensure Spark/Iceberg versions align |
| NoClassDefFoundError scala/collection/SeqOps | Scala ABI mismatch (2.12 vs 2.13) | Use JAR with `_2.12` for PySpark 3.4.x |
| MetaStore / Hive errors | Hive catalog not needed | Current script uses Hive; if issue persists, switch to Hadoop-only catalog (see below) |

### Switch to Hadoop-Only (Optional Simplification)
If Hive-related errors occur, edit the Spark builder in `etl_pipeline_demo.py`:
```python
.config("spark.sql.catalog.spark_catalog.type", "in-memory")
```
And remove any need for `.enableHiveSupport()` (not present in current script). The script already uses a Hadoop catalog `demo_catalog`.

## 8. Upgrade Path (Optional)
| Target | New Versions | Steps |
|--------|--------------|-------|
| Spark + Iceberg | PySpark 3.5.1 + Iceberg 1.10.0 (Scala 2.13) | 1. `pip install pyspark==3.5.1` 2. Run download script (fetches `_3.5_2.13-1.10.0`) 3. Update JAR path if hard-coded 4. Run demo |
| PyIceberg | 0.5.2+ | Bump `pyiceberg` in env + test schema evolution & snapshot queries |
| Java | 17 | Install JDK 17; no code changes expected |

After upgrading Spark: remove old `_2.12` JARs to avoid accidental mixed-classpath issues.

## 9. Clean Up
```bash
rm -rf iceberg_warehouse warehouse .venv
```

## 10. CI Smoke Test (Example)
Add to a GitHub Actions workflow:
```yaml
- uses: actions/setup-python@v5
  with:
    python-version: '3.10'
- run: pip install -r requirements.txt
- run: python scripts/download_iceberg_jars.py
- run: python etl_pipeline_demo.py
```

## 11. Repro Script (All-in-One)
```bash
python -m venv .venv && . .venv/bin/activate && \
python scripts/download_iceberg_jars.py && \
pip install -r requirements.txt && \
python etl_pipeline_demo.py
```

---
Need auto-detect JAR logic integrated into the main script? Open an issue or extend with a small helper that inspects PySpark’s scala jars to choose the correct runtime.
